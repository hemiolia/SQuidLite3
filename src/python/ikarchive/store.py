import base64, hashlib, json, sqlite3, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from .planner import walk, identity, decoded_id
from .classify import classify_detail, classify_coop
from .rates import observations, weapon_snapshots
from .catalog_binding import resolve_response_planner

def now():return datetime.now(timezone.utc).isoformat()
def js(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'),sort_keys=True)
def digest(body):return hashlib.sha256(body).hexdigest()

def _immutable_uri(path):
    return f'{Path(path).resolve().as_uri()}?mode=ro&immutable=1'

def _close_preserving_active_exception(connection):
    """Try cleanup without replacing the exception that caused the cleanup."""
    try:
        connection.close()
    except BaseException:
        pass

def database_is_slice(path):
    """slice_meta がある派生ファイルなら真。無いファイルは偽。wal は作らない。"""
    path=Path(path)
    if not path.is_file():
        return False
    connection=sqlite3.connect(_immutable_uri(path),uri=True,timeout=30)
    try:
        connection.execute('PRAGMA query_only=ON')
        is_slice=connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='slice_meta'").fetchone() is not None
    except BaseException:
        _close_preserving_active_exception(connection)
        raise
    connection.close()
    return is_slice

DETAIL_ROOTS={
    'VsHistoryDetailQuery':'vsHistoryDetail',
    'CoopHistoryDetailQuery':'coopHistoryDetail',
}
EXPECTED_NULL_ROOTS={
    'useCurrentFestQuery':'currentFest',
}
RELATED_EMPTY_ROOTS={
    'SaleGearDetailQuery':'saleGear',
    'DownloadSearchReplayQuery':'replay',
}

class Store:
    def __init__(self,path,readonly=False):
        self.db=None
        self.path=Path(path);self.output_root=self.path.parent.parent if self.path.parent.name=='database' else self.path.parent
        self.readonly=readonly
        if readonly:
            if not self.path.is_file():raise FileNotFoundError(f'Database not found: {self.path}')
            probe=sqlite3.connect(_immutable_uri(self.path),uri=True,timeout=30)
            try:
                probe.row_factory=sqlite3.Row
                probe.execute('PRAGMA query_only=ON')
                is_slice=probe.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='slice_meta'").fetchone() is not None
                if is_slice:
                    role=probe.execute("SELECT value FROM slice_meta WHERE key='role'").fetchone()
                    if role is not None and role[0]=='lossless_slice_selector':
                        raise ValueError('LOSSLESS_SELECTOR_REQUIRES_SHARD_READER')
                    raise ValueError('INCOMPLETE_LEGACY_SLICE')
            except BaseException:
                _close_preserving_active_exception(probe)
                raise
            probe.close()
            connection=sqlite3.connect(f'{self.path.resolve().as_uri()}?mode=ro',uri=True,timeout=30)
            self.db=connection
            try:
                connection.row_factory=sqlite3.Row
                connection.execute('PRAGMA query_only=ON')
            except BaseException:
                self.db=None
                _close_preserving_active_exception(connection)
                raise
            return
        if database_is_slice(self.path):
            raise ValueError('SLICE_READONLY')
        self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        try:
            self.db=sqlite3.connect(self.path,timeout=30);self.db.row_factory=sqlite3.Row
            # SQLite's implicit deletes during REPLACE must reach the change feed.
            # Set this on every authoritative writer, before any schema/data work.
            self.db.execute('PRAGMA recursive_triggers=ON')
            self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
            self._apply_schema()
            if 'empty_retries' not in {row['name'] for row in self.db.execute('PRAGMA table_info(jobs)')}:
                self.db.execute('ALTER TABLE jobs ADD COLUMN empty_retries INTEGER NOT NULL DEFAULT 0')
                self.db.commit()
            self._install_existing_change_feed()
            self._reclassify();self.path.chmod(0o600)
        except BaseException:
            connection=self.db
            if connection is not None:
                try:
                    if connection.in_transaction:
                        connection.rollback()
                except BaseException:
                    pass
                try:
                    connection.close()
                except BaseException:
                    pass
                finally:
                    self.db=None
            raise
    def _apply_schema(self):
        # schema.sql は内容が変わったときだけ、一つのトランザクションで適用する。
        # PRAGMA foreign_keys はトランザクション内では無効なので、毎回ここで接続に設定する。
        self.db.execute('PRAGMA foreign_keys=ON')
        text=(Path(__file__).resolve().parents[3]/'sql/schema.sql').read_text()
        wanted=hashlib.sha256(text.encode('utf-8')).hexdigest()
        has_control=self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='control'").fetchone() is not None
        if has_control and self._control('schema_sql_sha256')==wanted:return
        self.db.executescript('BEGIN IMMEDIATE;\n'+text+"\nINSERT INTO control(key,value) VALUES('schema_sql_sha256','"+wanted+"') ON CONFLICT(key) DO UPDATE SET value=excluded.value;\nCOMMIT;")
    def _install_existing_change_feed(self):
        """Repair feed coverage only for databases whose baseline already opted in.

        New databases deliberately start without a feed; their first full
        reconciliation installs it at the pinned baseline boundary. For an
        existing feed, both installers run inside this method's savepoint so
        an identity conflict cannot leave only part of the writer contract.
        """
        from .change_feed import CHANGE_TABLE, install_change_feed
        from .writer_guards import install_writer_guards

        feed = self.db.execute(
            "SELECT type,name FROM sqlite_master WHERE name=? COLLATE NOCASE",
            (CHANGE_TABLE,),
        ).fetchone()
        if feed is None:
            return
        if feed['type'] != 'table' or feed['name'] != CHANGE_TABLE:
            raise ValueError('CHANGE_FEED_SCHEMA_INVALID')
        self.db.execute('SAVEPOINT ia_store_install_change_contract')
        try:
            install_change_feed(self.db)
            install_writer_guards(self.db)
            self.db.execute('RELEASE SAVEPOINT ia_store_install_change_contract')
        except BaseException:
            self.db.execute('ROLLBACK TO SAVEPOINT ia_store_install_change_contract')
            self.db.execute('RELEASE SAVEPOINT ia_store_install_change_contract')
            raise
    def close(self):self.db.close()
    def issue(self,code,context,response=None,run=None):
        self.db.execute('INSERT INTO issues(run_id,response_id,code,context,created_at) VALUES(?,?,?,?,?)',(run,response,code,js(context),now()))
    def issue_once(self,code,context,response=None,run=None):
        serialized=js(context)
        if self.db.execute('SELECT 1 FROM issues WHERE response_id IS ? AND code=? AND context=? LIMIT 1',
                           (response,code,serialized)).fetchone():
            return
        self.db.execute('INSERT INTO issues(run_id,response_id,code,context,created_at) VALUES(?,?,?,?,?)',
                         (run,response,code,serialized,now()))
    def queue(self,account,operation,variables,kind=None,key=None):
        self.db.execute('INSERT OR IGNORE INTO jobs(account,operation,variables_json,kind,match_key) VALUES(?,?,?,?,?)',(account,operation,js(variables),kind,key))
    def record(self,e,run=None):
        raw=base64.b64decode(e['body_base64'],validate=True);sha=digest(raw);text=None;error=None
        try:
            text=raw.decode('utf8');json.loads(text,parse_constant=lambda s:(_ for _ in ()).throw(ValueError(s)))
        except (ValueError,UnicodeError) as exc:text=None;error=type(exc).__name__
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO bodies VALUES(?,?,?)',(sha,raw,len(raw)))
            variables=js(e['variables'])
            existing=self.db.execute('SELECT id FROM responses WHERE account=? AND operation=? AND variables_json=? AND body_sha256=? AND http_status IS ? AND query_id IS ? AND app_version IS ?',
                (e['account'],e['operation'],variables,sha,e.get('status'),e.get('query_id'),e.get('app_version'))).fetchone()
            if existing:rid=existing[0]
            else:
                self.db.execute('''INSERT OR IGNORE INTO responses(event_id,run_id,account,fetched_at,operation,variables_json,query_id,app_version,http_status,headers_json,body_sha256,json_text,parse_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',(e['event_id'],run,e['account'],e['fetched_at'],e['operation'],variables,e.get('query_id'),e.get('app_version'),e.get('status'),js(e.get('headers',{})),sha,text,error))
                rid=self.db.execute('SELECT id FROM responses WHERE event_id=?',(e['event_id'],)).fetchone()[0]
            self.db.execute('INSERT OR IGNORE INTO response_fetches(event_id,response_id,run_id,fetched_at,headers_json) VALUES(?,?,?,?,?)',
                (e['event_id'],rid,run,e['fetched_at'],js(e.get('headers',{}))))
        return rid
    def _response_catalog_validation(self,r,planner,obj,data,variables):
        binding=resolve_response_planner(self.db,r,planner)
        status=binding.status
        current_eligible=status in ('known_current','legacy_unchecked')
        schema_planner=binding.planner if status in ('known_current','known_saved') else None
        checked=bool(r['query_id'] and isinstance(data,dict) and schema_planner is not None
            and r['operation'] in schema_planner.routes)
        omissions=schema_planner.missing_fields(r['operation'],data,variables) if checked else []
        shape_errors=schema_planner.invalid_shapes(r['operation'],data,variables) if checked else []
        outcome=self._response_outcome(r,obj,data,omissions+shape_errors)
        if not current_eligible:
            outcome='retry'
        related_empty=(current_eligible and self._is_related_empty(
            r,obj,data,omissions+shape_errors,
        ))
        return binding,omissions,shape_errors,outcome,related_empty
    def _record_response_catalog_issue(self,binding,r,run=None):
        if binding.status=='known_saved':
            code='RESPONSE_CATALOG_MISMATCH'
        elif binding.status in ('known_current','legacy_unchecked'):
            return
        else:
            code='RESPONSE_CATALOG_UNRESOLVED'
        self.issue_once(code,{
            'binding_status':binding.status,
            'operation':r['operation'],
        },r['id'],run)
    def project(self,rid,planner,country='JP'):
        r=self.db.execute('SELECT * FROM responses WHERE id=?',(rid,)).fetchone()
        if r['projected']:
            self._acknowledge_fetches(r,planner)
            return
        obj=json.loads(r['json_text']) if r['json_text'] else {}
        op=r['operation'];account=r['account'];variables=json.loads(r['variables_json']);data=obj.get('data') if isinstance(obj,dict) else None
        binding,omissions,shape_errors,outcome,related_empty=self._response_catalog_validation(
            r,planner,obj,data,variables,
        )
        okay=outcome=='done'
        with self.db:
            self._record_response_catalog_issue(binding,r)
            for path in omissions:self.issue_once('SELECTED_FIELD_MISSING',{'operation':op,'path':path},rid)
            for path in shape_errors:self.issue_once('SELECTED_FIELD_SHAPE_INVALID',{'operation':op,'path':path},rid)
            if outcome=='retry' and not related_empty:
                self.issue_once('INCOMPLETE_RESPONSE',{'operation':op,'status':r['http_status'],'graphql_errors':bool(obj.get('errors')) if isinstance(obj,dict) else False},rid)
            elif outcome=='unavailable':
                self.issue('DETAIL_UNAVAILABLE',{'operation':op,'reason':'SERVER_RETURNED_NULL'},rid)
            if isinstance(data,dict):
                self._matches(r,data,okay)
                if okay and ('Histories' in op or op=='CoopHistoryQuery'):
                    previous=self.db.execute('SELECT response_id FROM endpoint_heads WHERE account=? AND operation=?',(account,op)).fetchone()
                    if previous:
                        before={x[0] for x in self.db.execute('SELECT match_key FROM sightings WHERE response_id=?',(previous[0],))}
                        after={x[0] for x in self.db.execute('SELECT match_key FROM sightings WHERE response_id=?',(rid,))}
                        if before and after and before.isdisjoint(after):self.issue('POSSIBLE_HISTORY_GAP',{'operation':op,'previous_response':previous[0],'previous_count':len(before),'current_count':len(after)},rid)
                self._assets(r,data)
                if binding.status in ('known_current','legacy_unchecked') and op in planner.routes:
                    for event,a,b,p in planner.visit(op,data,variables):
                        if event=='entity':
                            if isinstance(a,str) and (a=='Image' or a.endswith('Image')):
                                u=b.get('url')
                                if isinstance(u,str) and u.startswith('https://'):
                                    self.db.execute('INSERT OR IGNORE INTO assets(url) VALUES(?)',(u,))
                                    self.db.execute('INSERT OR IGNORE INTO asset_refs VALUES(?,?,?)',(rid,u,js(p+('url',))))
                            eid=b.get('id')
                            if eid:self.db.execute('INSERT INTO entities VALUES(?,?,?,?,?) ON CONFLICT(account,typename,entity_id) DO UPDATE SET response_id=excluded.response_id,json_text=excluded.json_text',(account,a,eid,rid,js(b)))
                            for q,v in planner.related(a,b,country):
                                ident=identity(eid) if eid else None
                                self.queue(account,q,v,*(ident or (None,None)))
                        elif event in ('next','page'):
                            if event=='page':
                                field,child=p
                                page_binding={k:v for k,v in variables.items() if not k.startswith('page') and k!='cursor'}
                                fingerprint=digest(js(child).encode())
                                try:self.db.execute('INSERT INTO page_fingerprints VALUES(?,?,?,?,?)',(account,op,js(page_binding),js(field),fingerprint))
                                except sqlite3.IntegrityError:
                                    self.issue('PAGINATION_REPEATED_PAGE',{'operation':op,'field':field},rid);continue
                            self.queue(account,a,b)
                        elif event=='issue':self.issue(a,b,rid)
                elif op not in planner.routes:self.issue('UNKNOWN_OPERATION',op,rid)
            if okay:self._advance_endpoint_head(r,r['fetched_at'])
            self.db.execute('UPDATE responses SET projected=1 WHERE id=?',(rid,))
            self._acknowledge_fetches(
                r,planner,(binding,omissions,shape_errors,outcome,related_empty),
            )
    def _response_outcome(self,r,obj,data,omissions=()):
        if r['http_status']!=200 or not isinstance(data,dict) or obj.get('errors') or omissions:
            return 'retry'
        operation=r['operation']
        root=DETAIL_ROOTS.get(operation)
        if root and data.get(root) is None:
            return 'unavailable'
        root=EXPECTED_NULL_ROOTS.get(operation)
        if root and root in data and data.get(root) is None:
            return 'done'
        if self._is_related_empty(r,obj,data,omissions):
            return 'retry'
        if not data or not any(v is not None for v in data.values()):
            return 'retry'
        return 'done'
    def _is_related_empty(self,r,obj,data,omissions=()):
        root=RELATED_EMPTY_ROOTS.get(r['operation'])
        return (root is not None and r['http_status']==200 and isinstance(obj,dict)
            and 'errors' not in obj and not omissions and isinstance(data,dict)
            and root in data and data[root] is None)
    def _advance_endpoint_head(self,r,fetched_at):
        current=self.db.execute('SELECT response_id FROM endpoint_heads WHERE account=? AND operation=?',(r['account'],r['operation'])).fetchone()
        replace=current is None
        if current:
            last=self.db.execute('''SELECT COALESCE(MAX(julianday(fetched_at)),
                (SELECT julianday(fetched_at) FROM responses WHERE id=?))
                FROM response_fetches WHERE response_id=?''',(current[0],current[0])).fetchone()[0]
            candidate=self.db.execute('SELECT julianday(?)',(fetched_at,)).fetchone()[0]
            replace=candidate is not None and (last is None or candidate>=last)
        if replace:
            self.db.execute('INSERT INTO endpoint_heads VALUES(?,?,?) ON CONFLICT(account,operation) DO UPDATE SET response_id=excluded.response_id',
                (r['account'],r['operation'],r['id']))
    def _acknowledge_fetches(self,r,planner,validation=None):
        """本文の投影と、再取得の完了処理を分離する。スプール再生は冪等。"""
        obj=json.loads(r['json_text']) if r['json_text'] else {}
        data=obj.get('data') if isinstance(obj,dict) else None
        variables=json.loads(r['variables_json'])
        if validation is None:
            validation=self._response_catalog_validation(r,planner,obj,data,variables)
        binding,omissions,shape_errors,outcome,related_empty=validation
        okay=outcome=='done'
        with self.db:
            self._record_response_catalog_issue(binding,r)
            fetches=self.db.execute('SELECT * FROM response_fetches WHERE response_id=? AND acknowledged=0 ORDER BY julianday(fetched_at),event_id',(r['id'],)).fetchall()
            for fetched in fetches:
                for path in omissions:
                    self.issue_once('SELECTED_FIELD_MISSING',{'operation':r['operation'],'path':path},r['id'],fetched['run_id'])
                for path in shape_errors:
                    self.issue_once('SELECTED_FIELD_SHAPE_INVALID',{'operation':r['operation'],'path':path},r['id'],fetched['run_id'])
                if outcome=='retry' and not related_empty:
                    self.issue_once('INCOMPLETE_RESPONSE',{
                        'operation':r['operation'],'status':r['http_status'],
                        'graphql_errors':bool(obj.get('errors')) if isinstance(obj,dict) else False,
                    },r['id'],fetched['run_id'])
                if okay and r['operation'] in ('WeaponQuery','WeaponCollectionRefetchQuery'):
                    self._write_weapon_snapshots(r,data,fetched['fetched_at'],fetched['event_id'])
                # A delayed spool receipt must not undo a more recent success/failure.
                latest=self.db.execute('''SELECT MAX(julianday(f.fetched_at)) FROM response_fetches f JOIN responses p ON p.id=f.response_id
                    WHERE p.account=? AND p.operation=? AND p.variables_json=? AND f.acknowledged=1''',
                    (r['account'],r['operation'],r['variables_json'])).fetchone()[0]
                at=self.db.execute('SELECT julianday(?)',(fetched['fetched_at'],)).fetchone()[0]
                if latest is None or (at is not None and at>=latest):
                    if okay and r['projected']:
                        # A -> B -> A is a new observation of an old body. It must become
                        # current again without losing B or duplicating the match.
                        self._matches({**dict(r),'fetched_at':fetched['fetched_at']},data,True)
                    if okay:self._advance_endpoint_head(r,fetched['fetched_at'])
                    if r['operation'] in RELATED_EMPTY_ROOTS:
                        job=self.db.execute('SELECT empty_retries FROM jobs WHERE account=? AND operation=? AND variables_json=?',
                            (r['account'],r['operation'],r['variables_json'])).fetchone()
                        if job:
                            empty_retries=job['empty_retries']+1 if related_empty else 0
                            if related_empty and empty_retries==1:
                                self.issue('RELATED_DETAIL_EMPTY',{'operation':r['operation'],'reason':'SERVER_RETURNED_NULL'},r['id'])
                            delay=min(86400,300*2**min(empty_retries-1,9)) if related_empty else (86400 if outcome in ('done','unavailable') else 300)
                            self.db.execute('UPDATE jobs SET state=?,attempts=attempts+1,empty_retries=?,next_attempt=?,last_response_id=? WHERE account=? AND operation=? AND variables_json=?',
                                (outcome,empty_retries,time.time()+delay,r['id'],r['account'],r['operation'],r['variables_json']))
                    else:
                        self.db.execute('UPDATE jobs SET state=?,attempts=attempts+1,next_attempt=?,last_response_id=? WHERE account=? AND operation=? AND variables_json=?',
                            (outcome,time.time()+(86400 if outcome in ('done','unavailable') else 300),r['id'],r['account'],r['operation'],r['variables_json']))
                self.db.execute('UPDATE response_fetches SET acknowledged=1 WHERE event_id=?',(fetched['event_id'],))
    def _write_weapon_snapshots(self,r,data,fetched_at,event_id):
        for item in weapon_snapshots(data):
            self.db.execute('''INSERT INTO rate_points(account,series_id,label,genre,rule_raw,match_key,played_time,value,source,priority)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account,series_id,match_key) DO UPDATE SET label=excluded.label,value=excluded.value
                WHERE rate_points.label IS NOT excluded.label OR rate_points.value IS NOT excluded.value''',
                (r['account'],item['series_id'],item['label'],item['genre'],item['rule_raw'],'fetch:'+event_id,fetched_at,item['value'],'api_snapshot','primary'))
    def _matches(self,r,data,okay):
        for path,v in walk(data):
            if not isinstance(v,dict) or not isinstance(v.get('id'),str):continue
            ident=identity(v['id'])
            if not ident:continue
            kind,key=ident;a=r['account'];t=r['fetched_at']
            self.db.execute('INSERT INTO matches VALUES(?,?,?,?,?,NULL) ON CONFLICT(account,kind,match_key) DO UPDATE SET last_seen=MAX(last_seen,excluded.last_seen),first_seen=MIN(first_seen,excluded.first_seen)',(a,kind,key,t,t))
            self.db.execute('INSERT OR IGNORE INTO match_refs VALUES(?,?,?,?)',(a,kind,v['id'],key))
            self.queue(a,'VsHistoryDetailQuery' if kind=='vs' else 'CoopHistoryDetailQuery',{'vsResultId' if kind=='vs' else 'coopHistoryDetailId':v['id']},kind,key)
            self.db.execute('INSERT OR IGNORE INTO sightings VALUES(?,?,?,?,?,?)',(r['id'],a,kind,key,js(path),js(v)))
            # Only a full detail operation can replace the canonical detail projection.
            full=(r['operation']=='VsHistoryDetailQuery' and kind=='vs' and len(path)==1) or (r['operation']=='CoopHistoryDetailQuery' and kind=='coop' and len(path)==1)
            if full:
                self.db.execute('INSERT OR IGNORE INTO documents VALUES(?,?,?,?,?)',(r['id'],a,kind,key,js(v)))
                if okay:self.db.execute('''UPDATE matches SET detail_response_id=? WHERE account=? AND kind=? AND match_key=? AND
                    (detail_response_id IS NULL OR COALESCE((SELECT MAX(julianday(fetched_at)) FROM response_fetches WHERE response_id=detail_response_id),
                    (SELECT julianday(fetched_at) FROM responses WHERE id=detail_response_id))<=julianday(?))''',(r['id'],a,kind,key,r['fetched_at']))
                if kind in ('vs','coop'):
                    self._write_classification(a,kind,key)
                    self._write_rates(a,kind,key)
    def _assets(self,r,data):
        for path,v in walk(data):
            if not isinstance(v,str) or not v.startswith('https://'):continue
            # Preserve every URL in raw data, download fields explicitly representing media.
            if not path or str(path[-1]).lower() not in ('url','thumbnailurl','imageurl','originalurl'):continue
            u=urlparse(v)
            if any(x in '/'.join(map(str,path)).lower() for x in ('image','photo','thumbnail','album','icon','mask','original')):
                self.db.execute('INSERT OR IGNORE INTO assets(url) VALUES(?)',(v,))
                self.db.execute('INSERT OR IGNORE INTO asset_refs VALUES(?,?,?)',(r['id'],v,js(path)))
    def recover(self,planner,spool,run=None):
        for f in sorted(Path(spool).glob('*.json')):
            e=json.loads(f.read_text());rid=self.record(e,run);self.project(rid,planner);f.unlink()
        for r in self.db.execute('SELECT id FROM responses WHERE projected=0').fetchall():self.project(r[0],planner)
    def _unknown_auth(self):
        # last_failure を null にすると画面は「記録なし」になる。派生には監査が無いので、文字列でない値で未確認にする。
        return {
            'session_expires_at':None,'bullet_expires_at':None,'last_ok_at':None,
            'last_failure':False,'last_failure_at':None,'last_sync_error':False,'last_sync_error_at':None,
            'reauth_required':False,'session_expires_soon':False,'backfill_armed':False,
        }
    def _slice_sync_health(self):
        from .collector import HISTORIES
        from .storage import storage_health
        clocks=[{'operation':operation,'last_success_at':None,'age_seconds':None,'stale':True} for operation in sorted(HISTORIES)]
        return {'state':'absent','stale_after_seconds':600,'histories':clocks,
            'storage_health':storage_health(self.path),'auth':self._unknown_auth(),
            'retry_after':None,'exports_updated_at':None,'export_error':None}
    def _slice_status(self):
        health=self._slice_sync_health()
        return {
            'responses':None,
            'matches':self.db.execute('SELECT count(*) FROM matches').fetchone()[0],
            'documents':self.db.execute('SELECT count(*) FROM documents').fetchone()[0],
            'pending_details':None,'unavailable_details':None,'issues':None,'entities':None,'assets':None,
            'matches_without_detail':self.db.execute('SELECT count(*) FROM matches WHERE detail_response_id IS NULL').fetchone()[0],
            'jobs':None,'assets_by_state':None,'last_run':None,
            'analysis':[dict(r) for r in self.db.execute('SELECT kind,genre,roster_class,analysis_set,count(*) count FROM match_classification GROUP BY 1,2,3,4 ORDER BY 1,2,3')],
            'tags':[dict(r) for r in self.db.execute('SELECT tag,count(*) count FROM match_tags GROUP BY tag ORDER BY tag')],
            'all_server_records_verified':False,'auth':health['auth'],'sync_health':health,
            'storage_health':health['storage_health'],'slice':True,
        }
    def status(self):
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='slice_meta'").fetchone() is not None:
            return self._slice_status()
        counts={t:self.db.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in ('responses','matches','documents','pending_details','unavailable_details','issues','entities','assets')}
        counts['matches_without_detail']=self.db.execute('SELECT count(*) FROM matches WHERE detail_response_id IS NULL').fetchone()[0]
        counts['jobs']=[dict(r) for r in self.db.execute('SELECT state,count(*) count FROM jobs GROUP BY state')]
        counts['assets_by_state']=[dict(r) for r in self.db.execute('SELECT state,count(*) count FROM assets GROUP BY state')]
        counts['last_run']=dict(r) if (r:=self.db.execute('SELECT * FROM runs ORDER BY id DESC LIMIT 1').fetchone()) else None
        counts['analysis']=[dict(r) for r in self.db.execute('SELECT kind,genre,roster_class,analysis_set,count(*) count FROM match_classification GROUP BY 1,2,3,4 ORDER BY 1,2,3')]
        counts['tags']=[dict(r) for r in self.db.execute('SELECT tag,count(*) count FROM match_tags GROUP BY tag ORDER BY tag')]
        counts['all_server_records_verified']=False
        counts['auth']=self.auth_status()
        counts['sync_health']=self.sync_health()
        counts['storage_health']=counts['sync_health']['storage_health']
        return counts
    def sync_health(self):
        from .collector import HISTORIES
        from .storage import storage_health
        receipts=bool(self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='response_fetches'").fetchone())
        clocks=[]
        for operation in sorted(HISTORIES):
            source='response_fetches f JOIN responses r ON r.id=f.response_id' if receipts else 'responses r'
            stamp='f.fetched_at' if receipts else 'r.fetched_at'
            row=self.db.execute(f'''SELECT {stamp} FROM {source} WHERE r.operation=? AND r.http_status=200 AND r.projected=1
                AND NOT EXISTS(SELECT 1 FROM issues i WHERE i.response_id=r.id AND i.code IN (
                    'INCOMPLETE_RESPONSE','SELECTED_FIELD_MISSING','SELECTED_FIELD_SHAPE_INVALID',
                    'RESPONSE_CATALOG_MISMATCH','RESPONSE_CATALOG_UNRESOLVED'))
                ORDER BY julianday({stamp}) DESC LIMIT 1''',(operation,)).fetchone()
            at=row[0] if row else None
            age=None
            if at:
                try:age=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(at.replace('Z','+00:00'))).total_seconds())
                except ValueError:pass
            clocks.append({'operation':operation,'last_success_at':at,'age_seconds':round(age,1) if age is not None else None,'stale':age is None or age>600})
        stale=any(c['stale'] for c in clocks)
        pause=self._control('retry_after')
        if pause:
            try:
                if float(pause)<=time.time():pause=None
            except ValueError:
                pause=None
        return {'state':'delayed' if stale else 'current','stale_after_seconds':600,'histories':clocks,
            'storage_health':storage_health(self.path),'auth':self.auth_status(),
            'retry_after':datetime.fromtimestamp(float(pause),timezone.utc).isoformat() if pause else None,
            'exports_updated_at':self._control('exports_updated_at'),'export_error':self._control('export_error')}
    def _control(self,key,value=None):
        if value is None:return (row[0] if (row:=self.db.execute('SELECT value FROM control WHERE key=?',(key,)).fetchone()) else None)
        self.db.execute('INSERT INTO control VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,value))
    def remember_auth(self,info):
        def iso(ms):
            if not isinstance(ms,(int,float)):return None
            return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat()
        with self.db:
            previous_iat=self._control('auth_session_iat')
            failure=self._control('auth_last_failure')
            iat=info.get('session_iat')
            if failure in ('AUTH_REQUIRED','AUTH_EXPIRED','SESSION_EXPIRED') or (previous_iat and iat is not None and previous_iat!=str(int(iat))):
                self._control('backfill_armed','1');self._control('backfill_reset_done','0')
            if iat is not None:self._control('auth_session_iat',str(int(iat)))
            for key,value in (('auth_session_expires_at',iso(info.get('session_expires_at'))),('auth_bullet_expires_at',iso(info.get('bullet_expires_at'))),('auth_last_ok_at',now())):
                if value:self._control(key,value)
            self.db.execute("DELETE FROM control WHERE key IN ('auth_last_failure','auth_last_failure_at','last_sync_error','last_sync_error_at')")
    def remember_auth_failure(self,code):
        with self.db:
            self._control('auth_last_failure',code)
            self._control('auth_last_failure_at',now())
            self.issue('AUTH_INCIDENT',{'code':code})
    def remember_sync_error(self,code):
        with self.db:
            self._control('last_sync_error',code)
            self._control('last_sync_error_at',now())
    def auth_status(self):
        session=self._control('auth_session_expires_at')
        failure=self._control('auth_last_failure')
        ok=self._control('auth_last_ok_at')
        remaining=None
        if session:
            try:remaining=(datetime.fromisoformat(session)-datetime.now(timezone.utc)).total_seconds()
            except ValueError:remaining=None
        return {
            'session_expires_at':session,
            'bullet_expires_at':self._control('auth_bullet_expires_at'),
            'last_ok_at':ok,
            'last_failure':failure,
            'last_failure_at':self._control('auth_last_failure_at'),
            'last_sync_error':self._control('last_sync_error'),
            'last_sync_error_at':self._control('last_sync_error_at'),
            'reauth_required':failure in ('AUTH_REQUIRED','AUTH_EXPIRED','SESSION_EXPIRED') and (not ok or (self._control('auth_last_failure_at') or '')>=(ok or '')),
            'session_expires_soon':remaining is not None and remaining<14*86400,
            'backfill_armed':self._control('backfill_armed')=='1',
        }
    def apply_backfill(self,account):
        """After re-authentication, walk saved history pages again once. Do not delete rows."""
        if self._control('backfill_armed')!='1' or self._control('backfill_reset_done')=='1':return 0
        with self.db:
            self._control('backfill_reset_done','1')
            cur=self.db.execute('''UPDATE jobs SET state='pending',next_attempt=0 WHERE account=? AND state IN ('done','unavailable') AND (operation LIKE '%Histor%' OR operation='CoopHistoryQuery' OR (operation IN ('VsHistoryDetailQuery','CoopHistoryDetailQuery') AND match_key IN (SELECT match_key FROM matches WHERE account=? AND detail_response_id IS NULL)))''',(account,account))
            return cur.rowcount
    def finish_backfill(self,account):
        if self._control('backfill_armed')!='1':return
        pending=self.db.execute("SELECT count(*) FROM jobs WHERE account=? AND state IN ('pending','retry') AND (operation LIKE '%Histor%' OR operation='CoopHistoryQuery')",(account,)).fetchone()[0]
        if pending==0:
            with self.db:self.db.execute("DELETE FROM control WHERE key IN ('backfill_armed','backfill_reset_done')")
    def _reclassify(self):
        self._repair_job_outcomes()
        # 旧実装の勝敗由来「チョーシ」は誤った派生値。原文・試合記録は保持。
        self.db.execute("DELETE FROM rate_points WHERE source='derived_judgement'")
        for row in self.db.execute("SELECT account,kind,match_key FROM matches WHERE kind IN ('vs','coop') AND detail_response_id IS NOT NULL"):
            self._write_classification(row['account'],row['kind'],row['match_key'])
            self._write_rates(row['account'],row['kind'],row['match_key'])
        for row in self.db.execute("SELECT * FROM responses WHERE operation IN ('WeaponQuery','WeaponCollectionRefetchQuery') AND http_status=200 AND json_text IS NOT NULL"):
            obj=json.loads(row['json_text'])
            if isinstance(obj,dict) and not obj.get('errors'):
                for fetched in self.db.execute('SELECT event_id,fetched_at FROM response_fetches WHERE response_id=?',(row['id'],)):
                    self._write_weapon_snapshots(row,obj.get('data'),fetched['fetched_at'],fetched['event_id'])
        self.db.commit()
    def _repair_job_outcomes(self):
        """旧版の誤った再試行状態を、保存済み原文から非破壊で再判定する。"""
        with self.db:
            pager_jobs = self.db.execute(
                "SELECT account, operation, variables_json, state FROM jobs WHERE operation='VsHistoryDetailPagerRefetchQuery'"
            ).fetchall()
            for j in pager_jobs:
                if j['state'] != 'superseded':
                    self.db.execute(
                        "UPDATE jobs SET state='superseded', next_attempt=? WHERE account=? AND operation=? AND variables_json=?",
                        (time.time() + 86400, j['account'], j['operation'], j['variables_json'])
                    )
            self.db.execute(
                "UPDATE issues SET code='SUPERSEDED_RESPONSE' WHERE code='INCOMPLETE_RESPONSE' AND response_id IN ("
                "SELECT id FROM responses WHERE operation='VsHistoryDetailPagerRefetchQuery')"
            )

            row = self.db.execute("SELECT json_text FROM manifests ORDER BY fetched_at DESC LIMIT 1").fetchone()
            planner = None
            if row and row['json_text']:
                from .planner import Planner
                try:
                    manifest = json.loads(row['json_text'])
                    planner = Planner(manifest)
                except Exception:
                    planner = None

            target_ops = {**EXPECTED_NULL_ROOTS, **DETAIL_ROOTS}
            placeholders = ','.join('?' for _ in target_ops)
            jobs = self.db.execute(
                f"SELECT account, operation, variables_json, state, next_attempt, last_response_id FROM jobs "
                f"WHERE operation IN ({placeholders}) AND last_response_id IS NOT NULL",
                list(target_ops.keys())
            ).fetchall()

            for job in jobs:
                op = job['operation']
                root = target_ops[op]
                rid = job['last_response_id']
                r = self.db.execute("SELECT * FROM responses WHERE id=?", (rid,)).fetchone()
                if not r or not r['json_text']:
                    continue
                try:
                    obj = json.loads(r['json_text'])
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                data = obj.get('data')
                if not isinstance(data, dict) or root not in data or data[root] is not None:
                    continue

                try:
                    variables = json.loads(r['variables_json'])
                except Exception:
                    variables = {}

                binding,omissions,shape_errors,outcome,related_empty=self._response_catalog_validation(
                    r,planner,obj,data,variables,
                )
                self._record_response_catalog_issue(binding,r)
                if planner is None and r['query_id'] and binding.status=='unresolved':
                    # Preserve the historical no-catalog skip, while refusing to
                    # promote this identified response to a successful outcome.
                    continue

                for path in omissions:
                    self.issue_once('SELECTED_FIELD_MISSING',{'operation':op,'path':path},rid)
                for path in shape_errors:
                    self.issue_once('SELECTED_FIELD_SHAPE_INVALID',{'operation':op,'path':path},rid)
                if outcome=='retry' and not related_empty:
                    self.issue_once('INCOMPLETE_RESPONSE',{
                        'operation':op,'status':r['http_status'],
                        'graphql_errors':bool(obj.get('errors')) if isinstance(obj,dict) else False,
                    },rid)

                if job['state'] != outcome:
                    next_attempt = time.time() + (86400 if outcome in ('done', 'unavailable') else 300)
                    self.db.execute(
                        "UPDATE jobs SET state=?, next_attempt=? WHERE account=? AND operation=? AND variables_json=?",
                        (outcome, next_attempt, job['account'], job['operation'], job['variables_json'])
                    )

                if binding.status not in ('known_current','legacy_unchecked'):
                    # Keep earlier issue rows intact; the catalog issue records why
                    # this repair cannot treat the old response as current success.
                    continue
                if outcome == 'unavailable':
                    self.db.execute(
                        "UPDATE issues SET code='DETAIL_UNAVAILABLE' WHERE code='INCOMPLETE_RESPONSE' AND response_id=?",
                        (rid,)
                    )
                elif outcome == 'done':
                    self.db.execute(
                        "UPDATE issues SET code='EXPECTED_ABSENCE' WHERE code='INCOMPLETE_RESPONSE' AND response_id=?",
                        (rid,)
                    )
                else:
                    self.db.execute(
                        "UPDATE issues SET code='INCOMPLETE_RESPONSE' WHERE code IN ('DETAIL_UNAVAILABLE','EXPECTED_ABSENCE') AND response_id=?",
                        (rid,)
                    )
    def _write_classification(self,account,kind,key):
        row=self.db.execute('''SELECT d.json_text,m.detail_response_id FROM matches m
            JOIN documents d ON d.response_id=m.detail_response_id AND d.account=m.account AND d.kind=m.kind AND d.match_key=m.match_key
            WHERE m.account=? AND m.kind=? AND m.match_key=? AND m.detail_response_id IS NOT NULL''',(account,kind,key)).fetchone()
        if not row:
            self.db.execute('DELETE FROM match_classification WHERE account=? AND kind=? AND match_key=?',(account,kind,key));return
        detail=json.loads(row['json_text'])
        info=classify_detail(detail) if kind=='vs' else classify_coop(detail)
        self.db.execute('''INSERT INTO match_classification(account,kind,match_key,genre,mode_raw,bankara_mode,rule_raw,rule_name,roster_class,analysis_set,team_count,my_player_count,opponent_counts,detail_response_id,classified_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(account,kind,match_key) DO UPDATE SET genre=excluded.genre,mode_raw=excluded.mode_raw,bankara_mode=excluded.bankara_mode,rule_raw=excluded.rule_raw,rule_name=excluded.rule_name,roster_class=excluded.roster_class,analysis_set=excluded.analysis_set,team_count=excluded.team_count,my_player_count=excluded.my_player_count,opponent_counts=excluded.opponent_counts,detail_response_id=excluded.detail_response_id,classified_at=excluded.classified_at
            WHERE match_classification.genre IS NOT excluded.genre OR match_classification.mode_raw IS NOT excluded.mode_raw OR match_classification.bankara_mode IS NOT excluded.bankara_mode
              OR match_classification.rule_raw IS NOT excluded.rule_raw OR match_classification.rule_name IS NOT excluded.rule_name OR match_classification.roster_class IS NOT excluded.roster_class
              OR match_classification.analysis_set IS NOT excluded.analysis_set OR match_classification.team_count IS NOT excluded.team_count OR match_classification.my_player_count IS NOT excluded.my_player_count
              OR match_classification.opponent_counts IS NOT excluded.opponent_counts OR match_classification.detail_response_id IS NOT excluded.detail_response_id''',
            (account,kind,key,info['genre'],info['mode_raw'],info['bankara_mode'],info['rule_raw'],info['rule_name'],info['roster_class'],info['analysis_set'],info['team_count'],info['my_player_count'],js(info['opponent_counts']),row['detail_response_id'],now()))
    def _write_rates(self,account,kind,key):
        row=self.db.execute('''SELECT d.json_text,c.genre,c.analysis_set,c.rule_raw,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.judgement') judgement
            FROM match_classification c JOIN documents d ON d.response_id=c.detail_response_id AND d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key
            WHERE c.account=? AND c.kind=? AND c.match_key=?''',(account,kind,key)).fetchone()
        if not row:
            self.db.execute('DELETE FROM rate_points WHERE account=? AND match_key=? AND source=?',(account,key,'api'));return
        detail=json.loads(row['json_text'])
        genre=row['analysis_set'] if row['genre']=='private' else row['genre']
        new={}
        for item in observations(detail,genre,row['rule_raw']):new[item['series_id']]=item  # 同一系列は後勝ち（従来の上書きと同じ）
        for (sid,) in self.db.execute('SELECT series_id FROM rate_points WHERE account=? AND match_key=? AND source=?',(account,key,'api')).fetchall():
            if sid not in new:self.db.execute('DELETE FROM rate_points WHERE account=? AND series_id=? AND match_key=? AND source=?',(account,sid,key,'api'))
        for item in new.values():
            self.db.execute('''INSERT INTO rate_points(account,series_id,label,genre,rule_raw,match_key,played_time,value,source,priority)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account,series_id,match_key) DO UPDATE SET label=excluded.label,value=excluded.value,played_time=excluded.played_time,priority=excluded.priority,source=excluded.source,
                genre=CASE WHEN rate_points.source='api' THEN excluded.genre ELSE rate_points.genre END,
                rule_raw=CASE WHEN rate_points.source='api' THEN excluded.rule_raw ELSE rate_points.rule_raw END
                WHERE rate_points.label IS NOT excluded.label OR rate_points.value IS NOT excluded.value OR rate_points.played_time IS NOT excluded.played_time
                  OR rate_points.priority IS NOT excluded.priority OR rate_points.source IS NOT excluded.source
                  OR (rate_points.source='api' AND (rate_points.genre IS NOT excluded.genre OR rate_points.rule_raw IS NOT excluded.rule_raw))''',
                (account,item['series_id'],item['label'],item['genre'],item['rule_raw'],key,row['played_time'],item['value'],item['source'],item['priority']))
    def verify(self):
        errors=[]
        if self.db.execute('PRAGMA integrity_check').fetchone()[0]!='ok':errors.append('integrity_check')
        if self.db.execute('PRAGMA foreign_key_check').fetchall():errors.append('foreign_key_check')
        for r in self.db.execute('SELECT sha256,body,byte_length FROM bodies'):
            if digest(r['body'])!=r['sha256'] or len(r['body'])!=r['byte_length']:errors.append(r['sha256'])
        return errors
