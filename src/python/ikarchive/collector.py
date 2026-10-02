import base64, json, os, queue, threading, subprocess, time, uuid, urllib.request, urllib.error
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from .store import now, js, digest
from .planner import Planner
from .storage import storage_health
from .ranking_policy import apply_scope, SCOPED_OPS

ROOT=Path(__file__).resolve().parents[3]
HISTORIES={'LatestBattleHistoriesQuery','RegularBattleHistoriesQuery','BankaraBattleHistoriesQuery','XBattleHistoriesQuery','EventBattleHistoriesQuery','PrivateBattleHistoriesQuery','CoopHistoryQuery'}
HISTORY_REFRESH_SECONDS=120

class Bridge:
    def __init__(self):
        self.p=subprocess.Popen([os.environ.get('NODE','node'),str(ROOT/'src/node/bridge.mjs')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,bufsize=1)
        self.lines=queue.Queue()
        def read():
            for line in self.p.stdout:self.lines.put(line)
            self.lines.put(None)
        self.reader=threading.Thread(target=read,daemon=True);self.reader.start()
    def call(self,command,**kwargs):
        self.p.stdin.write(js({'command':command,**kwargs})+'\n');self.p.stdin.flush()
        try:line=self.lines.get(timeout=150)
        except queue.Empty:raise RuntimeError('BRIDGE_TIMEOUT') from None
        if not line:raise RuntimeError('BRIDGE_EXITED')
        result=json.loads(line)
        if result.get('error'):raise RuntimeError(result['error'])
        return result
    def close(self):
        self.p.stdin.close()
        try:self.p.wait(timeout=3)
        except subprocess.TimeoutExpired:self.p.kill();self.p.wait()
        self.reader.join(timeout=3);self.p.stdout.close()

def catalog(store,force=False):
    row=store.db.execute('SELECT * FROM manifests ORDER BY fetched_at DESC LIMIT 1').fetchone()
    if row and not force:return json.loads(row['json_text'])
    b=Bridge()
    try:manifest=b.call('inventory')
    finally:b.close()
    planner=Planner(manifest)
    if not manifest['queries'] or len(manifest['queries'])!=manifest['expected']:raise RuntimeError('INCOMPLETE_CATALOG')
    with store.db:
        text=js(manifest);store.db.execute('INSERT OR IGNORE INTO manifests VALUES(?,?,?)',(digest(text.encode()),manifest['fetched_at'],text))
        for op,reason in planner.unsupported.items():store.issue('UNSUPPORTED_OPERATION',{'operation':op,'reason':reason})
        if row:
            old=json.loads(row['json_text'])['queries']
            changed=[n for n,q in manifest['queries'].items() if n not in old or q['params']['id']!=old[n]['params']['id']]
            for n in changed:store.db.execute("UPDATE jobs SET state='pending',next_attempt=0 WHERE operation=?",(n,))
    return manifest

def priority(job):
    if job['operation'] in HISTORIES:return 0
    if job['operation'] in ('VsHistoryDetailQuery','CoopHistoryDetailQuery'):return 1
    if job['kind']:return 3
    if 'Ranking' in job['operation']:return 5
    return 2

def sync(store,account=None,data_path=None,budget=250,delay=1.5):
    # The caller owns the archive writer lock. A previous running row therefore
    # describes an interrupted process, not a second live collector.
    with store.db:
        store.db.execute("UPDATE runs SET status='interrupted',finished_at=?,error='PROCESS_INTERRUPTED' WHERE status='running'",(now(),))
    run=store.db.execute("INSERT INTO runs(started_at,status,account) VALUES(?,'running',?)",(now(),account)).lastrowid;store.db.commit()
    bridge=None
    try:
        if storage_health(store.path)['state']=='critical':raise RuntimeError('STORAGE_CRITICAL')
        row=store.db.execute('SELECT fetched_at FROM manifests ORDER BY fetched_at DESC LIMIT 1').fetchone()
        from datetime import datetime,timezone
        stale=not row or (datetime.now(timezone.utc)-datetime.fromisoformat(row[0].replace('Z','+00:00'))).total_seconds()>86400
        manifest=catalog(store,stale);planner=Planner(manifest)
        spool=store.output_root/'spool';spool.mkdir(exist_ok=True,mode=0o700)
        store.recover(planner,spool,run)
        pause=store.db.execute("SELECT value FROM control WHERE key='retry_after'").fetchone()
        if pause and float(pause[0])>time.time():raise RuntimeError('SERVER_BACKOFF')
        bridge=Bridge();auth=bridge.call('init',account=account,data_path=data_path,spool=str(spool))
        if auth.get('error'):raise RuntimeError(auth['error'])
        account=auth['account'];country=auth.get('country') or 'JP'
        store.remember_auth(auth)
        store.apply_backfill(account)
        with store.db:
            store.db.execute('UPDATE runs SET account=? WHERE id=?',(account,run))
            for op,v in planner.roots(country):
                store.queue(account,op,v)
                # Roots with cursor pagination are scanned again daily, histories on each pass.
                store.db.execute("UPDATE jobs SET state='pending',next_attempt=0 WHERE account=? AND operation=? AND variables_json=? AND state='done' AND (? OR next_attempt<=?)",(account,op,js(v),int(op in HISTORIES),time.time()))
            # Refresh successful detail/record jobs daily; failure state is never reset to success.
            store.db.execute("UPDATE jobs SET state='pending' WHERE account=? AND state='done' AND next_attempt<=?",(account,time.time()))
            # A repeated-page guard applies to a crawl epoch, not to last day's identical content.
            store.db.execute('DELETE FROM page_fingerprints WHERE account=?',(account,))
        apply_scope(store,account)
        scope_dirty=False
        count=0
        next_history_refresh=time.monotonic()+HISTORY_REFRESH_SECONDS
        while count<budget:
            space=storage_health(store.path)
            if space['state']=='critical':raise RuntimeError('STORAGE_CRITICAL')
            if time.monotonic()>=next_history_refresh:
                from .publish import publish_outputs
                # Keep the GUI current; the incomplete legacy workbook is not generated.
                publish_outputs(store)
                # Long initial crawls must keep observing the short live history window.
                # Only successful jobs are refreshed; retry deadlines remain intact.
                with store.db:
                    for op in HISTORIES:
                        store.db.execute("UPDATE jobs SET state='pending',next_attempt=0 WHERE account=? AND operation=? AND state='done'",(account,op))
                next_history_refresh=time.monotonic()+HISTORY_REFRESH_SECONDS
            jobs=store.db.execute("SELECT * FROM jobs WHERE account=? AND state IN ('pending','retry') AND next_attempt<=?",(account,time.time())).fetchall()
            jobs=[j for j in jobs if j['operation'] in planner.routes]
            # Keep the finite live history window first. Deferred records remain
            # queued and resume automatically when disk space is available.
            if space['state']=='low':jobs=[j for j in jobs if priority(j)<=1]
            if not jobs:break
            job=min(jobs,key=lambda j:(priority(j),j['attempts'],j['operation'],j['variables_json']))
            if scope_dirty and priority(job)>1:
                apply_scope(store,account)
                scope_dirty=False
                continue
            op=job['operation'];variables=json.loads(job['variables_json']);q=planner.queries[op]
            try:
                result=bridge.call('query',operation=op,variables=variables,query_id=q['params']['id'],version=manifest.get('version'))
                f=Path(result['spool_file']);e=json.loads(f.read_text());rid=store.record(e,run);store.project(rid,planner,country);f.unlink()
                if op in SCOPED_OPS or op=='VsHistoryDetailQuery' or 'Ranking' in op:scope_dirty=True
                status=e['status']
                if status in (401,403,429) or status>=500:
                    retry=e.get('headers',{}).get('retry-after','300')
                    try:pause_seconds=float(retry)
                    except ValueError:
                        from email.utils import parsedate_to_datetime
                        try:pause_seconds=max(60,parsedate_to_datetime(retry).timestamp()-time.time())
                        except (ValueError,TypeError):pause_seconds=300
                    with store.db:store.db.execute("INSERT INTO control VALUES('retry_after',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(time.time()+max(60,pause_seconds)),))
                    raise RuntimeError('AUTH_EXPIRED' if status in (401,403) else 'SERVER_BACKOFF')
            except RuntimeError:raise
            except (OSError,ValueError) as exc:
                with store.db:
                    store.issue('REQUEST_FAILED',{'operation':op,'error_type':type(exc).__name__},run=run)
                    store.db.execute("UPDATE jobs SET state='retry',attempts=attempts+1,next_attempt=? WHERE account=? AND operation=? AND variables_json=?",(time.time()+min(3600,30*2**min(job['attempts'],7)),account,op,job['variables_json']))
            count+=1
            if delay:time.sleep(delay)
        if scope_dirty:apply_scope(store,account)
        asset_count=fetch_assets(store,min(50,budget),delay)
        pending=store.db.execute("SELECT count(*) FROM jobs WHERE account=? AND state IN ('pending','retry','awaiting_scope')",(account,)).fetchone()[0]
        missing_assets=store.db.execute("SELECT count(*) FROM assets WHERE state!='done'").fetchone()[0]
        state='partial' if pending or missing_assets or planner.unsupported else 'available_routes_collected'
        with store.db:store.db.execute('UPDATE runs SET finished_at=?,status=? WHERE id=?',(now(),state,run))
        store.finish_backfill(account)
        return {'run_id':run,'requests':count,'assets_attempted':asset_count,'state':state,'pending_jobs':pending,'backfill_armed':store.auth_status()['backfill_armed']}
    except Exception as exc:
        # Only controlled error codes escape; credential-bearing exceptions never enter logs.
        code=str(exc) if isinstance(exc,RuntimeError) and str(exc).isupper() else type(exc).__name__
        with store.db:store.db.execute("UPDATE runs SET finished_at=?,status='blocked',error=? WHERE id=?",(now(),code,run))
        if code in ('AUTH_REQUIRED','AUTH_EXPIRED','SESSION_EXPIRED'):store.remember_auth_failure(code)
        else:store.remember_sync_error(code)
        raise RuntimeError(code) from None
    finally:
        if bridge:bridge.close()

def allowed_asset(url):
    u=urlparse(url)
    if u.scheme!='https' or u.username or u.password or u.port not in (None,443):
        return False
    host=u.hostname or ''
    if any(host==h or host.endswith('.'+h) for h in ('nintendo.net','nintendo.com','nintendo.co.jp')):
        return True
    # 写真アルバムは、このバケットの署名付きURLだけを返す。ほかのGCSバケットは拒む。
    parts=[p for p in u.path.split('/') if p]
    return host=='storage.googleapis.com' and len(parts)>=1 and parts[0]=='ugcstore-toyohr-lp1-persistent'

# 期限の過ぎた署名は、待ち直しても成功しない。通信せず、未取得のまま残す。
ASSET_EXPIRED_RETRY_SECONDS=86400

class AssetUrlExpired(Exception):
    pass

def signed_url_expired(url, now):
    query=parse_qs(urlparse(url).query)
    if 'Expires' in query:
        try:return int(query['Expires'][0])<=now
        except (TypeError, ValueError):return False
    issued=query.get('X-Goog-Date')
    span=query.get('X-Goog-Expires')
    if not issued or not span:return False
    try:
        moment=datetime.strptime(issued[0],'%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
        return moment.timestamp()+int(span[0])<=now
    except (TypeError, ValueError):
        return False

class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        if not allowed_asset(newurl):raise ValueError('ASSET_HOST_UNREVIEWED')
        return super().redirect_request(req,fp,code,msg,headers,newurl)

def fetch_assets(store,budget,delay):
    rows=store.db.execute("SELECT * FROM assets WHERE state!='done' AND next_attempt<=? ORDER BY attempts LIMIT ?",(time.time(),budget)).fetchall()
    opener=urllib.request.build_opener(SafeRedirect())
    started=time.monotonic();attempted=0
    for r in rows:
        if time.monotonic()-started>=HISTORY_REFRESH_SECONDS or storage_health(store.path)['state']!='normal':break
        attempted+=1
        url=r['url']
        try:
            if not allowed_asset(url):raise ValueError('ASSET_HOST_UNREVIEWED')
            if signed_url_expired(url,time.time()):raise AssetUrlExpired()
            with opener.open(url,timeout=45) as response:
                raw=response.read();ct=response.headers.get('content-type','')
            sha=digest(raw)
            with store.db:
                store.db.execute('INSERT OR IGNORE INTO bodies VALUES(?,?,?)',(sha,raw,len(raw)))
                store.db.execute("UPDATE assets SET state='done',body_sha256=?,content_type=?,attempts=attempts+1,last_error=NULL WHERE url=?",(sha,ct,url))
        except Exception as exc:
            retry_after=ASSET_EXPIRED_RETRY_SECONDS if isinstance(exc,AssetUrlExpired) else 3600
            with store.db:store.db.execute("UPDATE assets SET state='retry',attempts=attempts+1,next_attempt=?,last_error=? WHERE url=?",(time.time()+retry_after,type(exc).__name__,url))
        if delay:time.sleep(delay)
    return attempted
