from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.error import URLError
from urllib.parse import urlsplit, parse_qs
from pathlib import Path
import json,csv,io,time,datetime,os,sqlite3,threading,math
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parent
SOURCE='https://stops.lt/vilnius/gps_full.txt'
FLEET={
4200:'NLV624',4201:'NLV626',4202:'NLV630',4203:'NLV633',4204:'NLV634',4205:'NLV637',4206:'NLV642',
4207:'NMC025',4208:'NMB971',4209:'NMC031',4210:'NMC041',4211:'NMC037',4212:'NMB982',4213:'NMC035',4214:'NMB989',
4215:'NRA517',4216:'NRA518',4217:'NRA519',4218:'NRA521',4219:'NRA522',
4674:'AYR764',4675:'NTA485',4676:'NTA490',4677:'NTU703',4678:'NTA483',4679:'NTA487',4680:'NTA492',4681:'NTA489',4682:'AIM433',4683:'NTA488',
4706:'NTY501',4707:'NTY502',4708:'NTY507',4709:'NTY506',4710:'NTY499',4711:'NTY504',4712:'NTY503',4713:'NTY498',
4714:'OAG291',4715:'OBY793',4716:'OBY866',4717:'OBY776',4718:'OBY764',4719:'OBY782',4720:'OBY779'}
CACHE={'at':0,'data':None}
DB_PATH=Path(os.environ.get('FLEET_DB_PATH',str(ROOT/'fleet_history.sqlite3')))
LOCK=threading.RLock()
STALE_LIMIT=180
FAULT_LIMIT=48*3600
POLL_SECONDS=int(os.environ.get("GPS_POLL_SECONDS", "5"))
MAX_GAP_SECONDS=120
CACHE_SECONDS=max(1,min(POLL_SECONDS-1,4))
FETCH_LOCK=threading.Lock()
MAX_SPEED_KMH=100

LOCAL_TZ=ZoneInfo('Europe/Vilnius')

def weekday_seconds(start, end):
    """Count elapsed seconds on Lithuania-local Mon-Fri, including full weekdays."""
    if start is None or end <= start:
        return 0
    cursor=datetime.datetime.fromtimestamp(start, LOCAL_TZ)
    finish=datetime.datetime.fromtimestamp(end, LOCAL_TZ)
    seconds=0.0
    while cursor < finish:
        next_day=datetime.datetime.combine(cursor.date()+datetime.timedelta(days=1),datetime.time.min,tzinfo=LOCAL_TZ)
        segment_end=min(next_day,finish)
        if cursor.weekday()<5:
            seconds += (segment_end.timestamp()-cursor.timestamp())
        cursor=segment_end
    return max(0,int(seconds))

def history(vehicles, now):
    # SQLite on Render Free is ephemeral. Configure durable storage for reliable history.
    with LOCK:
        DB_PATH.parent.mkdir(parents=True,exist_ok=True)
        with sqlite3.connect(DB_PATH,timeout=10) as db:
            db.execute('CREATE TABLE IF NOT EXISTS gps_history (garage INTEGER PRIMARY KEY, inactive_since REAL, last_active REAL)')
            for v in vehicles:
                n=v['garage']
                row=db.execute('SELECT inactive_since,last_active FROM gps_history WHERE garage=?',(n,)).fetchone()
                inactive_since,last_active=row if row else (None,None)
                if v['status'] in ('active','depot'):
                    inactive_since=None
                    last_active=now
                elif inactive_since is None:
                    inactive_since=now
                db.execute('INSERT OR REPLACE INTO gps_history (garage,inactive_since,last_active) VALUES (?,?,?)',(n,inactive_since,last_active))
                duration=weekday_seconds(inactive_since,now) if inactive_since is not None else 0
                v['inactiveSeconds']=duration if v['status']=='unknown' else None
                v['inactiveSince']=datetime.datetime.fromtimestamp(inactive_since,datetime.timezone.utc).isoformat() if inactive_since is not None else None
                v['lastActiveAt']=datetime.datetime.fromtimestamp(last_active,datetime.timezone.utc).isoformat() if last_active is not None else None
                v['weekendPaused']=datetime.datetime.fromtimestamp(now,LOCAL_TZ).weekday()>=5 and v['status']=='unknown'
                if v['status']=='unknown' and duration>=FAULT_LIMIT:
                    v['status']='suspected_fault'



def haversine_km(a,b,c,d):
    r=6371.0088
    p1,p2=math.radians(a),math.radians(c)
    dp=p2-p1;dl=math.radians(d-b)
    x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*r*math.asin(min(1,math.sqrt(x)))

def mileage(vehicles,now):
    """Accumulate unique, fresh GPS observations, not repeated page requests."""
    today=datetime.datetime.fromtimestamp(now,LOCAL_TZ).date()
    week_start=today-datetime.timedelta(days=today.weekday())
    month_start=today.replace(day=1)
    with LOCK:
        with sqlite3.connect(DB_PATH,timeout=10) as db:
            db.execute('CREATE TABLE IF NOT EXISTS mileage_points (garage INTEGER PRIMARY KEY, observed_ts REAL, latitude REAL, longitude REAL)')
            db.execute('CREATE TABLE IF NOT EXISTS mileage_daily (garage INTEGER, day TEXT, km REAL NOT NULL DEFAULT 0, PRIMARY KEY (garage,day))')
            db.execute('CREATE TABLE IF NOT EXISTS gps_samples (garage INTEGER NOT NULL, observed_ts REAL NOT NULL, latitude REAL NOT NULL, longitude REAL NOT NULL, speed REAL, received_ts REAL NOT NULL, PRIMARY KEY(garage, observed_ts))')
            db.execute('CREATE TABLE IF NOT EXISTS gps_quality (garage INTEGER PRIMARY KEY, accepted INTEGER DEFAULT 0, rejected_jump INTEGER DEFAULT 0, rejected_gap INTEGER DEFAULT 0, rejected_noise INTEGER DEFAULT 0)')
            for v in vehicles:
                n=v['garage'];lat=v.get('latitude');lon=v.get('longitude');age=v.get('ageSeconds')
                if v['status'] in ('active','depot') and lat is not None and lon is not None and age is not None and 0<=age<=STALE_LIMIT:
                    observed=round(now-age)
                    previous=db.execute('SELECT observed_ts,latitude,longitude FROM mileage_points WHERE garage=?',(n,)).fetchone()
                    if previous is None or observed>previous[0]:
                        db.execute('INSERT OR IGNORE INTO gps_samples (garage,observed_ts,latitude,longitude,speed,received_ts) VALUES (?,?,?,?,?,?)',(n,observed,lat,lon,v.get('speed'),now))
                        db.execute('INSERT OR IGNORE INTO gps_quality(garage) VALUES (?)',(n,))
                        if previous:
                            delta=observed-previous[0]
                            if 0<delta<=MAX_GAP_SECONDS:
                                km=haversine_km(previous[1],previous[2],lat,lon)
                                if km>=0.015 and km/delta*3600<=MAX_SPEED_KMH:
                                    db.execute('UPDATE gps_quality SET accepted=accepted+1 WHERE garage=?',(n,))
                                    day=datetime.datetime.fromtimestamp(observed,LOCAL_TZ).date().isoformat()
                                    db.execute('INSERT INTO mileage_daily (garage,day,km) VALUES (?,?,?) ON CONFLICT(garage,day) DO UPDATE SET km=km+excluded.km',(n,day,km))
                                else:
                                    col='rejected_noise' if km<0.015 else 'rejected_jump'
                                    db.execute(f'UPDATE gps_quality SET {col}={col}+1 WHERE garage=?',(n,))
                            elif delta>MAX_GAP_SECONDS:
                                db.execute('UPDATE gps_quality SET rejected_gap=rejected_gap+1 WHERE garage=?',(n,))
                        db.execute('INSERT OR REPLACE INTO mileage_points (garage,observed_ts,latitude,longitude) VALUES (?,?,?,?)',(n,observed,lat,lon))
            rows=db.execute('SELECT garage,day,km FROM mileage_daily').fetchall()
    totals={n:{'today':0.,'week':0.,'month':0.,'all':0.} for n in FLEET}
    for n,day,km in rows:
        if n not in totals:continue
        d=datetime.date.fromisoformat(day)
        totals[n]['all']+=km
        if d==today:totals[n]['today']+=km
        if week_start<=d<=today:totals[n]['week']+=km
        if month_start<=d<=today:totals[n]['month']+=km
    for v in vehicles:v['mileageKm']={k:round(val,2) for k,val in totals[v['garage']].items()}
    return {k:round(sum(x[k] for x in totals.values()),2) for k in ('today','week','month','all')}


def mileage_archive(month=None, now=None):
    """Read retained daily totals; calendar rollover never deletes mileage."""
    current=datetime.datetime.fromtimestamp(time.time() if now is None else now,LOCAL_TZ).strftime('%Y-%m')
    with LOCK:
        DB_PATH.parent.mkdir(parents=True,exist_ok=True)
        with sqlite3.connect(DB_PATH,timeout=10) as db:
            db.execute('CREATE TABLE IF NOT EXISTS mileage_daily (garage INTEGER, day TEXT, km REAL NOT NULL DEFAULT 0, PRIMARY KEY (garage,day))')
            months=[r[0] for r in db.execute("SELECT DISTINCT substr(day,1,7) FROM mileage_daily WHERE day < ? ORDER BY 1 DESC",(current+'-01',))]
            selected=month or (months[0] if months else None)
            if selected is not None:
                try:
                    parsed=datetime.datetime.strptime(selected,'%Y-%m')
                    if parsed.strftime('%Y-%m')!=selected:raise ValueError()
                except ValueError:raise ValueError('Neteisingas mėnuo. Naudokite YYYY-MM.')
                if selected>=current:raise ValueError('Archyve galima pasirinkti tik pasibaigusį mėnesį.')
            rows=db.execute("SELECT garage,SUM(km) FROM mileage_daily WHERE substr(day,1,7)=? GROUP BY garage",(selected,)).fetchall() if selected else []
    totals=dict(rows)
    vehicles=[{'garage':n,'plate':plate,'km':round(totals.get(n,0),2)} for n,plate in FLEET.items()]
    return {'month':selected,'months':months,'currentMonth':current,'timezone':'Europe/Vilnius','hasData':bool(rows),'vehicles':vehicles,'totalKm':round(sum(totals.get(n,0) for n in FLEET),2)}

ARCHIVE_HTML="""<!doctype html><html lang="lt"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>READY1 · Ridos archyvas</title>
<style>body{font:16px system-ui;background:#101827;color:#eef3ff;max-width:960px;margin:auto;padding:24px}a{color:#76c7ff}select,button{font:inherit;padding:10px;border-radius:8px;margin:8px}table{border-collapse:collapse;width:100%;background:#192539}td,th{text-align:left;padding:12px;border-bottom:1px solid #354359}td:last-child,th:last-child{text-align:right}h1{margin-bottom:8px}p{color:#becbe0}</style>
<a href="/">← Autobusai</a><h1>Ridos archyvas</h1><p>Pasibaigusių mėnesių preliminari GPS rida. Naujo mėnesio skaitiklis prasideda nuo nulio pagal Lietuvos laiką; istorija išsaugoma.</p>
<label>Mėnuo <select id="month" aria-label="Mėnuo"></select></label><a id="download" hidden>Atsisiųsti CSV</a><p id="status" role="status">Kraunama…</p><table><thead><tr><th>Garažo Nr.</th><th>Valst. numeris</th><th>Rida, km</th></tr></thead><tbody id="rows"></tbody></table>
<script>
const select=document.querySelector('#month'), status=document.querySelector('#status');
const number=v=>v.toLocaleString('lt-LT',{minimumFractionDigits:2,maximumFractionDigits:2});
async function load(month){try{const response=await fetch('/api/mileage-archive'+(month?'?month='+encodeURIComponent(month):''),{cache:'no-store'});const data=await response.json();if(!response.ok)throw Error(data.error);select.replaceChildren(...data.months.map(m=>new Option(m,m)));if(data.month&&!data.months.includes(data.month))select.add(new Option(data.month,data.month));if(data.month)select.value=data.month;select.disabled=!data.months.length;document.querySelector('#rows').replaceChildren(...data.vehicles.map(v=>{const row=document.createElement('tr');for(const x of [v.garage,v.plate,number(v.km)]){const cell=document.createElement('td');cell.textContent=x;row.append(cell)}return row}));status.textContent=data.hasData?'Bendra rida: '+number(data.totalKm)+' km':'Šio laikotarpio sukauptų duomenų nėra. Archyvas atsiras pasibaigus mėnesiui, kuriame buvo sukaupta ridos.';const link=document.querySelector('#download');link.hidden=!data.hasData;link.href='/api/mileage-archive.csv?month='+encodeURIComponent(data.month||'');}catch(error){status.textContent='Nepavyko įkelti archyvo: '+error.message}}
select.addEventListener('change',()=>load(select.value));load();
</script></html>"""

def fetch():
    if CACHE['data'] and time.time()-CACHE['at']<CACHE_SECONDS:return CACHE['data']
    with FETCH_LOCK:
        return _fetch_locked()

def _fetch_locked():
    if CACHE['data'] and time.time()-CACHE['at']<CACHE_SECONDS:return CACHE['data']
    req=Request(SOURCE,headers={'User-Agent':'READY1Fleet/1.0 (fleet dashboard)','Accept':'text/plain'})
    with urlopen(req,timeout=14) as res:
        raw=res.read(5000000)
    content=raw.decode('utf-8-sig',errors='replace')
    rows=list(csv.DictReader(io.StringIO(content)))
    if not rows or 'MasinosNumeris' not in rows[0]:raise ValueError('GPS šaltinio formatas pasikeitė')
    matches={}
    for r in rows:
        try:n=int(r.get('MasinosNumeris',''))
        except (ValueError,TypeError):continue
        if n not in FLEET:continue
        def num(v):
            try:return float(v)
            except (TypeError,ValueError):return None
        lon=num(r.get('Ilguma'));lat=num(r.get('Platuma'))
        clock=r.get('MatavimoLaikas','')
        # Feed often uses seconds since midnight; age computed in Lithuania local time.
        age=None
        if clock.isdigit():
            now=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=3 if is_summer() else 2)))
            sec=int(clock); age=(now.hour*3600+now.minute*60+now.second-sec)%86400
        direction=r.get('KryptiesPavadinimas','')
        depot=('parkas' in direction.lower() or 'ap2(' in direction.lower() or 'troleibusų parkas' in direction.lower())
        item={'garage':n,'plate':FLEET[n],'route':r.get('Marsrutas',''),'speed':num(r.get('Greitis')),'direction':direction,
              'latitude':lat/1e6 if lat else None,'longitude':lon/1e6 if lon else None,'ageSeconds':age,'towardDepot':depot,
              'observedAt':clock,'tripId':r.get('ReisoID','')}
        if n not in matches or (age is not None and (matches[n]['ageSeconds'] is None or age<matches[n]['ageSeconds'])):matches[n]=item
    vehicles=[]
    for n,plate in FLEET.items():
        v=matches.get(n,{'garage':n,'plate':plate,'route':'','speed':None,'direction':'','latitude':None,'longitude':None,'ageSeconds':None,'towardDepot':False,'observedAt':'','tripId':''})
        # Older than 3 minutes is stale; no GPS entry = unknown, not parked.
        v['status']='unknown' if n not in matches or (v['ageSeconds'] is not None and v['ageSeconds']>180) else ('depot' if v['towardDepot'] else 'active')
        vehicles.append(v)
    now=time.time()
    history(vehicles,now)
    mileage_totals=mileage(vehicles,now)
    result={'updatedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':SOURCE,'feedRows':len(rows),'vehicles':vehicles,'mileageTotalsKm':mileage_totals,'pollSeconds':POLL_SECONDS,'counts':{'total':len(vehicles),'active':sum(v['status']=='active' for v in vehicles),'depot':sum(v['status']=='depot' for v in vehicles),'unknown':sum(v['status']=='unknown' for v in vehicles),'suspected_fault':sum(v['status']=='suspected_fault' for v in vehicles)}}
    CACHE.update(at=time.time(),data=result)
    return result

def is_summer():
    from zoneinfo import ZoneInfo
    return datetime.datetime.now(ZoneInfo('Europe/Vilnius')).utcoffset().total_seconds()==10800

def diagnostics():
    # Aggregated quality data; historical records are retained for later audits.
    with LOCK, sqlite3.connect(DB_PATH,timeout=10) as db:
        try:
            rows=db.execute('SELECT garage,accepted,rejected_jump,rejected_gap,rejected_noise FROM gps_quality ORDER BY garage').fetchall()
            samples=db.execute('SELECT garage,COUNT(*),MIN(observed_ts),MAX(observed_ts) FROM gps_samples GROUP BY garage').fetchall()
            intervals=db.execute('SELECT garage,observed_ts,LAG(observed_ts) OVER(PARTITION BY garage ORDER BY observed_ts) AS prev FROM gps_samples ORDER BY garage,observed_ts DESC').fetchall()
        except sqlite3.OperationalError:
            return {'pollSeconds':POLL_SECONDS,'vehicles':{},'message':'Laukiama pirmųjų GPS matavimų'}
    by={n:{'acceptedSegments':a,'rejectedJumps':j,'longGaps':g,'smallMovements':z} for n,a,j,g,z in rows}
    for n,count,first,last in samples:
        by.setdefault(n,{}) .update(samples=count,firstSample=datetime.datetime.fromtimestamp(first,datetime.timezone.utc).isoformat(),lastSample=datetime.datetime.fromtimestamp(last,datetime.timezone.utc).isoformat())
    gaps={}
    for n,ts,prev in intervals:
        if prev is not None and 0<ts-prev<600:
            gaps.setdefault(n,[]).append(round(ts-prev))
    for n,values in gaps.items():
        values.sort()
        by.setdefault(n,{})['medianGpsIntervalSeconds']=values[len(values)//2]
    return {'pollSeconds':POLL_SECONDS,'vehicles':by,'warning':'Rida preliminari; be odometro palyginimo 1–3 % paklaida negarantuojama.'}

def poll_loop():
    while True:
        try:fetch()
        except Exception as exc:print('GPS polling:',exc,flush=True)
        time.sleep(POLL_SECONDS)

class Handler(SimpleHTTPRequestHandler):
    def __init__(self,*args,**kwargs):super().__init__(*args,directory=str(ROOT),**kwargs)
    def do_GET(self):
        path=urlsplit(self.path).path
        if path in ('/archive','/archive/','/api/mileage-archive','/api/mileage-archive.csv'):
            status=200;extra={}
            try:
                if path.startswith('/archive'):
                    body=ARCHIVE_HTML.encode();content_type='text/html; charset=utf-8'
                else:
                    month=parse_qs(urlsplit(self.path).query).get('month',[None])[0]
                    payload=mileage_archive(month)
                    if path.endswith('.csv'):
                        out=io.StringIO(newline='');writer=csv.writer(out,delimiter=';')
                        writer.writerow(['Mėnuo','Garažo Nr.','Valst. numeris','Rida km'])
                        for v in payload['vehicles']:writer.writerow([payload['month'] or '',v['garage'],v['plate'],format(v['km'],'.2f').replace('.',',')])
                        body=out.getvalue().encode('utf-8-sig');content_type='text/csv; charset=utf-8'
                        extra['Content-Disposition']='attachment; filename="READY1-rida-'+(payload['month'] or 'archyvas')+'.csv"'
                    else:body=json.dumps(payload,ensure_ascii=False).encode();content_type='application/json; charset=utf-8'
            except ValueError as exc:
                status=400;body=json.dumps({'error':str(exc)},ensure_ascii=False).encode();content_type='application/json; charset=utf-8'
            except Exception:
                status=500;body=json.dumps({'error':'Nepavyko perskaityti ridos archyvo.'}).encode();content_type='application/json; charset=utf-8'
            self.send_response(status);self.send_header('Content-Type',content_type);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)))
            for key,value in extra.items():self.send_header(key,value)
            self.end_headers();self.wfile.write(body);return
        if self.path.split('?')[0]=='/api/gps-diagnostics':
            payload=diagnostics();body=json.dumps(payload,ensure_ascii=False).encode()
            self.send_response(200);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
        if self.path.split('?')[0]=='/api/fleet':
            try:payload=fetch();status=200
            except Exception as e:payload={'error':str(e),'vehicles':[]};status=503
            body=json.dumps(payload,ensure_ascii=False).encode()
            self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
        return super().do_GET()

if __name__=='__main__':
    port=int(os.environ.get('PORT','8080'))
    threading.Thread(target=poll_loop,daemon=True).start()
    print('READY1 Fleet http://localhost:'+str(port),flush=True)
    ThreadingHTTPServer(('0.0.0.0',port),Handler).serve_forever()
