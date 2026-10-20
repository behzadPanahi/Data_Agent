from __future__ import annotations
import json, re, uuid
from pathlib import Path
from typing import Any
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT=Path(__file__).parent; DATA=ROOT/'data'; DATA.mkdir(exist_ok=True)
STATE_FILE=DATA/'state.json'; META_FILE=DATA/'metadata.json'
DEFAULT={'settings':{'base_url':'https://api.openai.com/v1','model':'gpt-4o-mini','api_key':'','memory_turns':8,'sample_rows':8},'connections':[],'chats':{}}
def read(path, fallback):
    try: return json.loads(path.read_text(encoding='utf-8'))
    except (OSError,json.JSONDecodeError): return fallback
def write(path,value): path.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
def state(): return read(STATE_FILE,DEFAULT.copy())
def metadata(): return read(META_FILE,{})

def db_connect(kind,cfg):
    if kind=='postgres':
        import psycopg
        return psycopg.connect(host=cfg['host'],port=int(cfg.get('port',5432)),dbname=cfg['database'],user=cfg['username'],password=cfg.get('password',''),connect_timeout=8)
    if kind=='mongo':
        from pymongo import MongoClient
        c=MongoClient(cfg['uri'],serverSelectionTimeoutMS=8000); c.admin.command('ping'); return c
    if kind=='elasticsearch':
        from elasticsearch import Elasticsearch
        c=Elasticsearch(cfg['url'],basic_auth=(cfg.get('username',''),cfg.get('password','')) if cfg.get('username') else None,request_timeout=8); c.info(); return c
    if kind=='clickhouse':
        import clickhouse_connect
        return clickhouse_connect.get_client(host=cfg['host'],port=int(cfg.get('port',8123)),username=cfg.get('username','default'),password=cfg.get('password',''),database=cfg.get('database','default'),connect_timeout=8)
    if kind=='neo4j':
        from neo4j import GraphDatabase
        c=GraphDatabase.driver(cfg['uri'],auth=(cfg.get('username','neo4j'),cfg.get('password','')),connection_timeout=8); c.verify_connectivity(); return c
    raise ValueError('نوع پایگاه داده پشتیبانی نمی‌شود')

def discover(kind,cfg):
    c=db_connect(kind,cfg)
    try:
        if kind=='postgres':
            with c.cursor() as cur:
                cur.execute("SELECT table_schema,table_name,column_name,data_type FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY table_schema,table_name,ordinal_position")
                rows=cur.fetchall()
            out={}
            for schema,table,col,typ in rows: out.setdefault(f'{schema}.{table}',[]).append({'name':col,'type':typ})
            return {'tables':out}
        if kind=='mongo':
            db=c[cfg['database']]; out={}
            for name in db.list_collection_names():
                doc=db[name].find_one(); out[name]=[{'name':k,'type':type(v).__name__} for k,v in (doc or {}).items()]
            return {'collections':out}
        if kind=='elasticsearch':
            out={}
            for name in c.indices.get_alias(index='*').keys():
                props=c.indices.get_mapping(index=name)[name].get('mappings',{}).get('properties',{})
                out[name]=[{'name':k,'type':v.get('type','object')} for k,v in props.items()]
            return {'indices':out}
        if kind=='clickhouse':
            rows=c.query('SELECT table,name,type FROM system.columns WHERE database = {db:String} ORDER BY table,position',parameters={'db':cfg.get('database','default')}).result_rows; out={}
            for table,col,typ in rows: out.setdefault(table,[]).append({'name':col,'type':typ})
            return {'tables':out}
        out={}
        with c.session(database=cfg.get('database','neo4j')) as sess:
            for row in sess.run('CALL db.schema.nodeTypeProperties() YIELD nodeLabels,propertyName,propertyTypes RETURN nodeLabels,propertyName,propertyTypes'):
                for label in row['nodeLabels']: out.setdefault(label,[]).append({'name':row['propertyName'],'type':','.join(row['propertyTypes'])})
        return {'labels':out}
    finally:
        try: c.close()
        except Exception: pass

def run_query(conn,query,limit):
    kind,cfg=conn['type'],conn['config']; q=query.strip()
    if kind in ('postgres','clickhouse') and re.match(r'^(select|with|show|describe|desc|explain|pragma)\b',q,re.I) is None: raise ValueError('فقط کوئری‌های خواندنی مجاز هستند.')
    if re.search(r'\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|attach|detach|optimize)\b',q,re.I): raise ValueError('کوئری شامل دستور تغییردهنده است و اجرا نشد.')
    if kind=='neo4j' and (re.search(r'\b(merge|set|remove|create|delete|detach|foreach)\b',q,re.I) or re.match(r'^(match|optional\s+match|with|unwind|return)\b',q,re.I) is None): raise ValueError('فقط عبارت‌های خواندنی Cypher مجاز هستند.')
    c=db_connect(kind,cfg)
    try:
        if kind=='postgres':
            with c.cursor() as cur:
                cur.execute(q)
                if not cur.description: return []
                names=[d.name for d in cur.description]; return [dict(zip(names,row)) for row in cur.fetchmany(limit)]
        if kind=='mongo':
            spec=json.loads(q); coll=c[cfg['database']][spec['collection']]
            return list(coll.find(spec.get('filter',{}),spec.get('projection')).limit(min(int(spec.get('limit',limit)),limit)))
        if kind=='elasticsearch':
            spec=json.loads(q); res=c.search(index=spec['index'],query=spec.get('query',{'match_all':{}}),size=min(int(spec.get('size',limit)),limit))
            return [h.get('_source',{})|{'_id':h.get('_id')} for h in res['hits']['hits']]
        if kind=='clickhouse':
            res=c.query(q); return [dict(zip(res.column_names,row)) for row in res.result_rows[:limit]]
        with c.session(database=cfg.get('database','neo4j')) as sess: return [dict(row) for row in sess.run(q+f' LIMIT {limit}')]
    finally:
        try: c.close()
        except Exception: pass

class Settings(BaseModel):
    base_url:str='https://api.openai.com/v1'; model:str='gpt-4o-mini'; api_key:str=''; memory_turns:int=Field(8,ge=0,le=50); sample_rows:int=Field(8,ge=1,le=50)
class ConnectionIn(BaseModel): name:str; type:str; config:dict
class ChatIn(BaseModel): chat_id:str|None=None; message:str
app=FastAPI(title='همیار داده'); app.mount('/static',StaticFiles(directory=ROOT/'static'),name='static')
@app.get('/')
def home(): return FileResponse(ROOT/'static'/'index.html')
@app.get('/api/state')
def get_state():
    s=state(); return {'settings':{**s['settings'],'api_key':'','has_api_key':bool(s['settings'].get('api_key'))},'connections':[{'id':c['id'],'name':c['name'],'type':c['type']} for c in s['connections']],'chats':[{'id':k,'title':v.get('title','گفتگوی تازه')} for k,v in s['chats'].items()]}
@app.put('/api/settings')
def put_settings(body:Settings):
    s=state(); incoming=body.model_dump()
    if not incoming['api_key']: incoming['api_key']=s['settings'].get('api_key','')
    s['settings']=incoming; write(STATE_FILE,s); return {'ok':True}
@app.get('/api/connections')
def get_connections(): return [{'id':c['id'],'name':c['name'],'type':c['type'],'metadata':metadata().get(c['id'],{})} for c in state()['connections']]
@app.post('/api/connections/test')
def test_connection(body:ConnectionIn):
    try: info=discover(body.type,body.config); return {'ok':True,'metadata':info,'object_count':sum(len(v) for v in info.values())}
    except Exception as exc: raise HTTPException(400,f'اتصال ناموفق: {exc}')
@app.post('/api/connections')
def add_connection(body:ConnectionIn):
    try: info=discover(body.type,body.config)
    except Exception as exc: raise HTTPException(400,f'اتصال یا دریافت metadata ناموفق بود: {exc}')
    s=state(); cid=str(uuid.uuid4()); s['connections'].append({'id':cid,'name':body.name,'type':body.type,'config':body.config}); write(STATE_FILE,s)
    m=metadata(); m[cid]={'name':body.name,'type':body.type,**info}; write(META_FILE,m); return {'ok':True,'id':cid,'metadata':info}
@app.delete('/api/connections/{cid}')
def delete_connection(cid:str):
    s=state(); s['connections']=[c for c in s['connections'] if c['id']!=cid]; write(STATE_FILE,s); m=metadata(); m.pop(cid,None); write(META_FILE,m); return {'ok':True}
async def llm(messages,tools=None):
    s=state()['settings']
    if not s.get('api_key'): raise HTTPException(400,'ابتدا کلید API را در تنظیمات وارد کنید.')
    payload={'model':s['model'],'messages':messages,'temperature':0.1}
    if tools: payload.update(tools=tools,tool_choice='auto')
    async with httpx.AsyncClient(timeout=90) as client:
        res=await client.post(s.get('base_url','https://api.openai.com/v1').rstrip('/')+'/chat/completions',headers={'Authorization':f"Bearer {s['api_key']}"},json=payload)
        if res.is_error: raise HTTPException(502,f'خطای سرویس مدل: {res.text[:600]}')
        return res.json()['choices'][0]['message']
@app.post('/api/chat')
async def chat(body:ChatIn):
    s=state(); chatid=body.chat_id or str(uuid.uuid4()); cd=s['chats'].get(chatid,{'title':body.message[:36],'messages':[]}); cd['messages'].append({'role':'user','content':body.message})
    keep=s['settings'].get('memory_turns',8)*2; history=cd['messages'][-keep:] if keep else []; conns=s['connections']; meta=metadata()
    words=set(re.findall(r'[\w\u0600-\u06ff]+',body.message.lower())); ranked=[]
    for c in conns:
        for group,objects in meta.get(c['id'],{}).items():
            if isinstance(objects,dict):
                for obj,cols in objects.items():
                    text=f"{c['name']} {c['type']} {group} {obj} "+' '.join(x['name']+' '+x['type'] for x in cols); score=sum(1 for w in words if len(w)>1 and w in text.lower()); ranked.append((score,c,group,obj,cols))
    ranked.sort(key=lambda x:x[0],reverse=True); relevant=ranked[:40]
    schema='\n'.join(f"connection_id={r[1]['id']} name={r[1]['name']} type={r[1]['type']} {r[2]}={r[3]} columns={json.dumps(r[4],ensure_ascii=False)}" for r in relevant) or 'هیچ پایگاه داده‌ای متصل نیست.'
    system=f'''تو یک عامل فارسی تحلیل داده هستی. فقط با metadata زیر تصمیم بگیر و هرگز نام جدول/فیلدی را حدس نزن. ابتدا در یک پیام کوتاه، برنامه اجرای خود را برای کاربر توضیح بده و سپس ابزار را صدا بزن. بعد از نتیجه، پاسخ را فارسی و بر اساس نمونه خروجی بده. فقط کوئری خواندنی بساز. برای postgres و clickhouse SQL، برای mongo JSON شامل collection/filter/limit، برای elasticsearch JSON شامل index/query/size و برای neo4j Cypher خواندنی بساز. اگر خطا آمد کوئری را اصلاح کن؛ حداکثر سه بار پس از اجرای اول تلاش مجدد مجاز است. پس از چهار اجرای ناموفق خطا را بگو. metadata بازیابی‌شده:\n{schema}'''
    messages=[{'role':'system','content':system}]+history; tools=[{'type':'function','function':{'name':'run_database_query','description':'اجرای کوئری خواندنی روی پایگاه داده متصل','parameters':{'type':'object','properties':{'connection_id':{'type':'string'},'query':{'type':'string'}},'required':['connection_id','query'],'additionalProperties':False}}}]; events=[]; answer=''
    for attempt in range(4):
        msg=await llm(messages,tools); messages.append(msg)
        if not msg.get('tool_calls'): answer=msg.get('content') or 'پاسخی از مدل دریافت نشد.'; break
        for call in msg['tool_calls']:
            args=json.loads(call['function']['arguments']); conn=next((c for c in conns if c['id']==args['connection_id']),None); query=args['query']; events.append({'type':'step','text':f"اجرای کوئری در {conn['name'] if conn else 'پایگاه داده'} (تلاش {attempt+1} از 4)"})
            try:
                if conn is None: raise ValueError('اتصال انتخاب‌شده پیدا نشد.')
                rows=run_query(conn,query,int(s['settings'].get('sample_rows',8))); result={'ok':True,'rows':rows,'count':len(rows)}; events.append({'type':'result','text':f'دریافت {len(rows)} ردیف نمونه'})
            except Exception as exc: result={'ok':False,'error':str(exc),'query':query}; events.append({'type':'error','text':f'اجرای کوئری ناموفق بود: {exc}'})
            messages.append({'role':'tool','tool_call_id':call['id'],'content':json.dumps(result,ensure_ascii=False,default=str)})
        if attempt==3:
            if result.get('ok'):
                final=await llm(messages)
                answer=final.get('content') or 'کوئری اجرا شد، اما مدل پاسخی تولید نکرد.'
            else: answer='اجرای کوئری پس از چهار بار تلاش ناموفق بود. آخرین خطا: '+str(result.get('error','خطای نامشخص'))
            break
    cd['messages'].append({'role':'assistant','content':answer,'events':events}); s['chats'][chatid]=cd; write(STATE_FILE,s); return {'chat_id':chatid,'answer':answer,'events':events}
@app.get('/api/chats/{cid}')
def get_chat(cid:str): return state()['chats'].get(cid,{'title':'گفتگوی تازه','messages':[]})
@app.delete('/api/chats/{cid}')
def delete_chat(cid:str):
    s=state(); s['chats'].pop(cid,None); write(STATE_FILE,s); return {'ok':True}
