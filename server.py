from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.error import URLError
from pathlib import Path
import json,csv,io,time,datetime,os

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

def fetch():
    if CACHE['data'] and time.time()-CACHE['at']<15:return CACHE['data']
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
    result={'updatedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':SOURCE,'feedRows':len(rows),'vehicles':vehicles,'counts':{'total':len(vehicles),'active':sum(v['status']=='active' for v in vehicles),'depot':sum(v['status']=='depot' for v in vehicles),'unknown':sum(v['status']=='unknown' for v in vehicles)}}
    CACHE.update(at=time.time(),data=result)
    return result

def is_summer():
    from zoneinfo import ZoneInfo
    return datetime.datetime.now(ZoneInfo('Europe/Vilnius')).utcoffset().total_seconds()==10800

class Handler(SimpleHTTPRequestHandler):
    def __init__(self,*args,**kwargs):super().__init__(*args,directory=str(ROOT),**kwargs)
    def do_GET(self):
        if self.path.split('?')[0]=='/api/fleet':
            try:payload=fetch();status=200
            except Exception as e:payload={'error':str(e),'vehicles':[]};status=503
            body=json.dumps(payload,ensure_ascii=False).encode()
            self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
        return super().do_GET()

if __name__=='__main__':
    port=int(os.environ.get('PORT','8080'))
    print('READY1 Fleet http://localhost:'+str(port),flush=True)
    ThreadingHTTPServer(('0.0.0.0',port),Handler).serve_forever()
