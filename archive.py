#!/usr/bin/env python3
"""イカリング3の取得・保存・監査CLI（Python標準ライブラリのみ）。"""
import argparse, base64, json, math, os, plistlib, re, shutil, sqlite3, subprocess, sys, uuid
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent/'src/python'))
from ikarchive.store import Store, now, js, database_is_slice
from ikarchive.locking import acquire
from ikarchive.planner import Planner
from ikarchive.collector import catalog, sync, ROOT
from ikarchive.gui import write_gui
from ikarchive.records import RecordReader
from scripts.data_root import data_root

OUTPUT=data_root()
DEFAULT=OUTPUT/'database/archive.sqlite3'
AUTH_INCIDENTS={'AUTH_REQUIRED','AUTH_EXPIRED','SESSION_EXPIRED'}

def nas_storage_marker():
    marker=OUTPUT/'config'/'storage-location.json'
    if not marker.exists() and not marker.is_symlink():return None
    try:
        if marker.is_symlink():raise ValueError('symlink')
        data=json.loads(marker.read_text(encoding='utf-8'))
        if not isinstance(data,dict) or type(data.get('schema_version')) is not int or data['schema_version']!=1 or data.get('backend')!='nas':
            raise ValueError('schema or backend')
        if not isinstance(data.get('ssh_host'),str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,252}',data['ssh_host']):
            raise ValueError('ssh host')
        if not isinstance(data.get('container'),str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',data['container']):
            raise ValueError('container')
        if data.get('database')!='/data/database/archive.sqlite3':raise ValueError('database')
    except (OSError,UnicodeError,json.JSONDecodeError,ValueError,TypeError,KeyError) as exc:
        raise ValueError('NAS_STORAGE_MARKER_INVALID: '+str(marker)) from exc
    return data

def notify_auth_incident(code):
    stamp=OUTPUT/'logs'/'auth-notice-stamp'
    incident=OUTPUT/'logs'/'auth-incident.json'
    OUTPUT.joinpath('logs').mkdir(parents=True,exist_ok=True,mode=0o700)
    incident.write_text(js({'code':code,'at':now()}),encoding='utf8')
    incident.chmod(0o600)
    fresh=True
    if stamp.exists():
        try:fresh=stamp.read_text(encoding='utf8').split('\n',1)[0]!=code or (__import__('time').time()-stamp.stat().st_mtime)>6*3600
        except OSError:fresh=True
    if not fresh:return
    stamp.write_text(code+'\n'+now(),encoding='utf8')
    if sys.platform!='darwin':return
    message='イカリング3の取得が認証で止まった。この間の戦績は保存されていない。ログインが必要。コード: '+code
    subprocess.run(['osascript','-e','display notification '+json.dumps(message)+' with title "イカリング3アーカイブ"'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

def apply_tag(store,args):
    if args.tag_action!='list':
        if not args.match_key:raise ValueError('MATCH_KEY_REQUIRED')
        if not isinstance(args.tag,str) or not args.tag.strip():raise ValueError('EMPTY_TAG')
    account=args.account
    if not account:
        sql='SELECT DISTINCT account FROM matches'+( ' WHERE match_key=?' if args.match_key else '')
        rows=[r[0] for r in store.db.execute(sql,((args.match_key,) if args.match_key else ()))]
        if len(rows)!=1:raise ValueError('ACCOUNT_REQUIRED')
        account=rows[0]
    if args.tag_action=='list':
        sql='SELECT match_key,tag,note,created_at,updated_at FROM match_tags WHERE account=?'
        params=[account]
        if args.match_key:
            sql+=' AND match_key=?';params.append(args.match_key)
        return [dict(r) for r in store.db.execute(sql+' ORDER BY match_key,tag',params)]
    if not store.db.execute('SELECT 1 FROM matches WHERE account=? AND match_key=?',(account,args.match_key)).fetchone():
        raise ValueError('MATCH_NOT_FOUND')
    tag=args.tag.strip()
    if args.tag_action=='add':
        store.db.execute('''INSERT INTO match_tags(account,match_key,tag,note,created_at,updated_at) VALUES(?,?,?,?,?,?)
            ON CONFLICT(account,match_key,tag) DO UPDATE SET note=excluded.note,updated_at=excluded.updated_at''',(account,args.match_key,tag,args.note,now(),now()))
    else:
        store.db.execute('DELETE FROM match_tags WHERE account=? AND match_key=? AND tag=?',(account,args.match_key,tag))
    store.db.commit()
    rows=[tuple(row) for row in store.db.execute(
        'SELECT tag,note,created_at,updated_at FROM match_tags WHERE account=? AND match_key=? ORDER BY tag',
        (account,args.match_key))]
    from ikarchive.slices import sync_tags_to_slices
    sync=sync_tags_to_slices(store.path,account,args.match_key,rows)
    return {'account_selected':True,'match_key':args.match_key,'tag':tag,'action':args.tag_action,'slice_sync':sync['slice_sync']}

def audit(store):
    from ikarchive.event_evidence import event_evidence
    from ikarchive.asset_evidence import asset_acquisition_evidence
    row=store.db.execute('SELECT json_text FROM manifests ORDER BY fetched_at DESC LIMIT 1').fetchone()
    if not row:return {'error':'CATALOG_MISSING','all_server_records_verified':False,'image_acquisition_evidence':asset_acquisition_evidence(store.db)}
    m=json.loads(row[0]);p=Planner(m);ops=[]
    for name in p.queries:
        states=[dict(r) for r in store.db.execute('SELECT state,count(*) count FROM jobs WHERE operation=? GROUP BY state',(name,))]
        responses=store.db.execute('SELECT count(*) FROM responses WHERE operation=?',(name,)).fetchone()[0]
        entities=p.routes.get(name,{}).get('bindings',{})
        ops.append({'operation':name,'classification':'excluded' if name in p.excluded else 'unsupported' if name in p.unsupported else 'related' if entities else 'root','reason':p.excluded.get(name) or p.unsupported.get(name),'states':states,'responses':responses,'needs_entities':entities})
    return {'catalog_at':m.get('fetched_at'),'version':m.get('version'),'extracted':len(p.queries),'read_routes':len(p.routes),'unsupported':p.unsupported,'operations':ops,'storage':store.status(),'event_evidence':event_evidence(store.db),'image_acquisition_evidence':asset_acquisition_evidence(store.db),'all_server_records_verified':False,'limit':'Server-internal records and records no longer exposed by SplatNet cannot be proven complete from the client API.'}

def dispatch(store,args):
    if args.command=='init':result=store.status()
    elif args.command=='refresh-catalog':
        m=catalog(store,True);p=Planner(m);result={'version':m.get('version'),'extracted':len(p.queries),'read_routes':len(p.routes),'unsupported':p.unsupported}
    elif args.command=='sync':
        if args.budget<1 or args.delay<0:raise ValueError('budget>=1, delay>=0')
        from ikarchive.publish import publish_outputs
        from ikarchive.slices import refresh_slices
        # 収集が例外で終わっても、ここまでに確定した試合を派生ファイルへ反映する。
        sync_error=None
        try:result=sync(store,args.account,args.nxapi_data,args.budget,args.delay)
        except Exception as exc:
            sync_error=exc
            result=None
        try:published=publish_outputs(store)
        except Exception as exc:
            published={'error':type(exc).__name__}
        try:slices=refresh_slices(store.path, store.path.parent / 'slices')
        except Exception as exc:
            if sync_error is None:raise
            raise sync_error from exc
        if sync_error is not None:raise sync_error
        result['exports']=published
        result['slices']=slices
    elif args.command=='status':result=store.status()
    elif args.command=='audit':result=audit(store)
    elif args.command=='verify':
        failures=store.verify();result={'ok':not failures,'failures':failures,'bodies_checked':store.db.execute('SELECT count(*) FROM bodies').fetchone()[0]}
        print(json.dumps(result,ensure_ascii=False,indent=2));return 1 if failures else 0
    elif args.command=='backup':
        from ikarchive.storage import ensure_backup_space
        dest=args.destination.resolve()
        if dest.exists():raise ValueError('Destination already exists; refusing overwrite')
        ensure_backup_space(store.db,dest)
        dest.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        import tempfile
        fd,tmpname=tempfile.mkstemp(prefix=dest.name+'.',suffix='.tmp',dir=dest.parent)
        os.close(fd);tmp=Path(tmpname)
        try:
            target=sqlite3.connect(tmp)
            try:store.db.backup(target)
            finally:target.close()
            check=sqlite3.connect(tmp)
            try:integrity=check.execute('PRAGMA integrity_check').fetchone()[0]
            finally:check.close()
            if integrity!='ok':raise RuntimeError('BACKUP_INTEGRITY_FAILED')
            # Atomic creation without overwriting an existing backup.
            os.link(tmp,dest)
        finally:
            tmp.unlink(missing_ok=True)
            Path(str(tmp)+'-journal').unlink(missing_ok=True)
            Path(str(tmp)+'-wal').unlink(missing_ok=True)
            Path(str(tmp)+'-shm').unlink(missing_ok=True)
        result={'backup':str(dest),'integrity':integrity}
    elif args.command=='export-xlsx':raise ValueError('LEGACY_ANALYSIS_XLSX_RETIRED')
    elif args.command=='gui':
        font_path=os.environ.get('IKARING_ARCHIVE_FONT')
        result=write_gui(store,args.destination,font_path=font_path)
        if sys.platform=='darwin':subprocess.run(['open',str(args.destination)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    elif args.command=='export':
        out=args.directory.resolve();out.mkdir(parents=True,exist_ok=True,mode=0o700);count=0
        with (out/'manifest.jsonl').open('x') as index:
            for r in store.db.execute('SELECT r.*,b.body FROM responses r JOIN bodies b ON b.sha256=r.body_sha256 ORDER BY r.id'):
                filename=f'{r["id"]}-{r["body_sha256"]}.json';(out/filename).write_bytes(r['body']);meta=dict(r);meta.pop('body');meta.pop('json_text');meta['file']=filename;index.write(js(meta)+'\n');count+=1
        assets=0
        with (out/'assets.jsonl').open('x') as index:
            for r in store.db.execute("SELECT a.*,b.body FROM assets a JOIN bodies b ON b.sha256=a.body_sha256 WHERE a.state='done'"):
                filename=r['body_sha256']+'.bin';(out/filename).write_bytes(r['body']);meta=dict(r);meta.pop('body');meta['file']=filename;index.write(js(meta)+'\n');assets+=1
        if store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='response_fetches'").fetchone():
            with (out/'fetches.jsonl').open('x') as index:
                for r in store.db.execute('SELECT * FROM response_fetches ORDER BY fetched_at,event_id'):
                    index.write(js(dict(r))+'\n')
        result={'export':str(out),'responses':count,'assets':assets}
    elif args.command=='import':
        p=Planner(catalog(store));count=0;rejected=[]
        for file in sorted(args.directory.rglob('*.json')):
            try:
                raw=file.read_bytes();data=json.loads(raw);op=None
                if isinstance(data,dict) and 'body_base64' in data and 'event_id' in data:
                    if data['account']!=args.account:raise ValueError('account mismatch')
                    e=data
                else:
                    # Explicit nxapi dump wrappers. The source bytes are kept verbatim.
                    result=data.get('result') if isinstance(data,dict) else None
                    if isinstance(result,dict):
                        from ikarchive.planner import identity
                        ident=identity(result.get('id'))
                        if ident:op='VsHistoryDetailQuery' if ident[0]=='vs' else 'CoopHistoryDetailQuery'
                    e={'event_id':'import-'+args.account+'-'+__import__('hashlib').sha256(raw).hexdigest(),'account':args.account,'fetched_at':now(),'operation':'nxapi-import','variables':{'filename':str(file)},'status':200,'body_base64':base64.b64encode(raw).decode()}
                rid=store.record(e)
                if op:
                    # An additional derived envelope is explicitly labeled, original is retained.
                    body={'data':{'vsHistoryDetail' if op=='VsHistoryDetailQuery' else 'coopHistoryDetail':result}}
                    derived={**e,'event_id':e['event_id']+'-projection','operation':op,'body_base64':base64.b64encode(js(body).encode()).decode()}
                    drid=store.record(derived);store.project(drid,p)
                store.project(rid,p);count+=1
            except (ValueError,OSError,TypeError) as exc:rejected.append({'file':str(file),'error':type(exc).__name__})
        result={'imported':count,'rejected':rejected}
    elif args.command=='tag':result=apply_tag(store,args)
    elif args.command=='sql':
        store.db.execute('PRAGMA query_only=ON');result=[dict(r) for r in store.db.execute(args.query)]
    elif args.command=='install-service':
        if sys.platform!='darwin':raise ValueError('LaunchAgent is macOS-only; use cron on other systems')
        if args.interval<60:raise ValueError('interval must be >=60 seconds')
        (OUTPUT/'logs').mkdir(parents=True,exist_ok=True,mode=0o700)
        label='local.ikaring3.archive';dest=Path.home()/'Library/LaunchAgents'/f'{label}.plist'
        if dest.exists():
            backup=dest.with_suffix('.plist.backup-'+uuid.uuid4().hex[:8]);shutil.copy2(dest,backup)
        command=[sys.executable,str(ROOT/'archive.py'),'--db',str(args.db),'sync']
        if args.account:command+=['--account',args.account]
        if args.nxapi_data:command+=['--nxapi-data',args.nxapi_data]
        node=shutil.which('node')
        env={'PATH':str(Path(node).parent)+':/usr/bin:/bin' if node else '/usr/bin:/bin','NODE':node,'IKARING_ARCHIVE_DATA_DIR':str(OUTPUT.resolve())}
        if 'NXAPI_DATA_PATH' in os.environ:env['NXAPI_DATA_PATH']=os.environ['NXAPI_DATA_PATH']
        payload={'Label':label,'ProgramArguments':command,'WorkingDirectory':str(ROOT),'StartInterval':args.interval,'RunAtLoad':True,'ProcessType':'Background','EnvironmentVariables':env,'StandardOutPath':str(OUTPUT/'logs/service.log'),'StandardErrorPath':str(OUTPUT/'logs/service-errors.log')}
        dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(plistlib.dumps(payload))
        subprocess.run(['launchctl','bootout',f'gui/{os.getuid()}/{label}'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        subprocess.run(['launchctl','bootstrap',f'gui/{os.getuid()}',str(dest)],check=True)
        result={'service':str(dest),'interval_seconds':args.interval,'login_required':True}
    if args.command=='sync':
        incident=OUTPUT/'logs'/'auth-incident.json'
        if incident.exists():incident.unlink()
    print(json.dumps(result,ensure_ascii=False,indent=2));return 0

def _write_shard_json(value):
    sys.stdout.write(json.dumps(value,ensure_ascii=True,separators=(',',':'))+'\n')

def _write_shard_text(value):
    # Escape in bounded chunks so large TEXT values do not require a second
    # full-size JSON string allocation. Joining these escaped fragments inside
    # the surrounding quotes preserves one ordinary JSON string value.
    sys.stdout.write('"')
    for offset in range(0,len(value),8192):
        encoded=json.dumps(value[offset:offset+8192],ensure_ascii=True,separators=(',',':'))
        sys.stdout.write(encoded[1:-1])
    sys.stdout.write('"')

def _write_shard_cell(value):
    if value is None:
        kind,encoding='null','none';encoded_value=None
    elif type(value) is int:
        if value < -(1<<63) or value > (1<<63)-1:raise ValueError('SHARD_CELL_INTEGER_OUT_OF_RANGE')
        kind,encoding='integer','decimal';encoded_value=str(value)
    elif type(value) is float:
        kind,encoding='real','float.hex';encoded_value=value.hex()
    elif type(value) is str:
        kind,encoding='text','unicode';encoded_value=value
    elif type(value) is bytes:
        kind,encoding='blob','hex';encoded_value=value
    else:
        raise ValueError('SHARD_CELL_TYPE_UNSUPPORTED')
    sys.stdout.write('{"sqlite_type":'+json.dumps(kind)+',"encoding":'+json.dumps(encoding)+',"value":')
    if kind=='null':sys.stdout.write('null')
    elif kind=='text':_write_shard_text(encoded_value)
    elif kind=='blob':
        sys.stdout.write('"')
        for offset in range(0,len(encoded_value),32768):
            sys.stdout.write(encoded_value[offset:offset+32768].hex())
        sys.stdout.write('"')
    else:
        sys.stdout.write(json.dumps(encoded_value,separators=(',',':')))
    sys.stdout.write('}')

def _write_shard_row(ordinal,values,columns,*,source_rowid=None,include_source_identity=False):
    if len(values)!=len(columns):raise ValueError('SHARD_ROW_WIDTH_INVALID')
    sys.stdout.write('{"ordinal":'+str(ordinal))
    if include_source_identity:
        sys.stdout.write(',"source_rowid":')
        _write_shard_cell(source_rowid)
    sys.stdout.write(',"values":[')
    for index,value in enumerate(values):
        if index:sys.stdout.write(',')
        _write_shard_cell(value)
    sys.stdout.write(']}\n')

def _delta_generation_ids(delta_dirs):
    plans=[]
    for directory in delta_dirs:
        path=Path(directory)/'delta-plan.json'
        try:
            with path.open('r',encoding='utf-8') as stream:
                plan=json.load(stream)
        except (OSError,UnicodeError,json.JSONDecodeError) as exc:
            raise ValueError('SHARD_DELTA_INVALID') from exc
        if not isinstance(plan,dict):raise ValueError('SHARD_DELTA_INVALID')
        plans.append(plan)
    last_reset=max((index for index,plan in enumerate(plans)
                    if plan.get('replaces_delta_chain') is True),default=-1)
    selected=plans[last_reset:]
    generations=[plan.get('generation_id') for plan in selected]
    if not generations or any(not isinstance(item,str) for item in generations):
        raise ValueError('SHARD_DELTA_INVALID')
    return generations

def _run_delta_shard_cli(args):
    from ikarchive.delta_reader import DeltaChainReader, DeltaReaderError
    from ikarchive.shard_reader import LosslessShardReader
    try:
        with LosslessShardReader(args.root,expected_generation=args.generation) as baseline:
            try:
                with DeltaChainReader(baseline,args.delta_dirs) as reader:
                    delta_generations=_delta_generation_ids(args.delta_dirs)
                    if delta_generations[-1]!=reader.generation_id:
                        raise ValueError('SHARD_DELTA_INVALID')
                    baseline_generation=baseline.snapshot_identifier
                    current_generation=reader.generation_id
                    tables=reader.tables()
                    if args.command=='shard-list':
                        result={
                            'reader_role':'lossless_delta_chain_reader',
                            'baseline_only':False,
                            # Keep the historical key tied to the baseline;
                            # current state is named explicitly below.
                            'generation':baseline_generation,
                            'baseline_generation':baseline_generation,
                            'latest_generation':current_generation,
                            'delta_generations':delta_generations,
                            'source_sha256':baseline.source_sha256,
                            'baseline_source_sha256':baseline.source_sha256,
                            'baseline_source_schema_sha256':baseline.source_schema_sha256,
                            'source_schema_sha256':reader.source_schema_sha256,
                            'table_count':len(tables),
                            'tables':tables,
                            'schema_objects':reader.schema_objects(),
                        }
                        _write_shard_json(result)
                        return 0

                    table=next((item for item in tables if item['name']==args.table),None)
                    if table is None:raise ValueError('SHARD_TABLE_NOT_FOUND')
                    columns=table['columns']
                    selector_count=None
                    if args.mode is None:
                        rows=iter(reader.iter_rows(args.table))
                        ordinal_kind='current_table_stream_ordinal'
                        scope={'kind':'full_table','table':args.table}
                    else:
                        selector_count=sum(1 for _ in reader.iter_selected_matches(
                            args.mode,rule_token=args.rule_token))
                        def numbered():
                            for ordinal,values in enumerate(reader.iter_selected_matches(
                                    args.mode,rule_token=args.rule_token)):
                                yield ordinal,None,values
                        rows=iter(numbered())
                        ordinal_kind='selector_stream_ordinal'
                        scope={
                            'kind':'mode_selector' if args.rule_token is None else 'mode_rule_selector',
                            'analysis_set':args.mode,
                            'rule_token':args.rule_token,
                            'selected_match_count':selector_count,
                            'source_value_closure':'full current source remains available through unfiltered table reads',
                            'information_scope':(
                                'selected rows are derived from the current full match_classification and matches tables; '
                                'the verified baseline-plus-delta chain retains every source table and value'
                            ),
                        }

                    sentinel=object()
                    current=next(rows,sentinel)
                    full_count=reader.row_count(args.table)
                    header={
                        'type':'header',
                        'generation':baseline_generation,
                        'baseline_generation':baseline_generation,
                        'latest_generation':current_generation,
                        'delta_generations':delta_generations,
                        'source_sha256':baseline.source_sha256,
                        'baseline_source_sha256':baseline.source_sha256,
                        'source_schema_sha256':reader.source_schema_sha256,
                        'table':args.table,
                        'columns':columns,
                        'column_xinfo':reader.columns(args.table),
                        'foreign_keys':reader.foreign_keys(args.table),
                        'table_full_row_count':full_count,
                        'ordinal_kind':ordinal_kind,
                        'source_rowid_kind':'nullable_original_source_rowid',
                        'scope':scope,
                    }
                    _write_shard_json(header)
                    sys.stdout.flush()
                    returned=0
                    while current is not sentinel:
                        if args.limit is not None and returned>=args.limit:
                            break
                        ordinal,source_rowid,values=current
                        _write_shard_row(ordinal,values,columns,source_rowid=source_rowid,
                                         include_source_identity=args.mode is None)
                        returned+=1
                        current=next(rows,sentinel)
                    truncated=current is not sentinel
                    _write_shard_json({'type':'footer','returned_rows':returned,'truncated':truncated,'limit':args.limit})
                    sys.stdout.flush()
                    return 0
            except DeltaReaderError as exc:
                raise ValueError('SHARD_DELTA_INVALID') from exc
            except (RuntimeError,ValueError,OSError,sqlite3.Error,StopIteration,TypeError,KeyError,UnicodeError) as exc:
                if isinstance(exc,ValueError) and str(exc).startswith('SHARD_'):
                    raise
                raise ValueError('SHARD_DELTA_INVALID') from exc
    except (RuntimeError,ValueError,OSError,sqlite3.Error,TypeError,KeyError,UnicodeError) as exc:
        if isinstance(exc,ValueError) and str(exc).startswith('SHARD_'):
            raise
        raise ValueError('SHARD_PACKAGE_INVALID') from exc

def _run_shard_cli(args):
    if args.command=='shard-read':
        if args.rule_token is not None and args.mode is None:
            raise ValueError('SHARD_RULE_REQUIRES_MODE')
        if args.limit is not None and args.limit<=0:
            raise ValueError('SHARD_LIMIT_MUST_BE_POSITIVE')
        if (args.mode is not None or args.rule_token is not None) and args.table!='matches':
            raise ValueError('SHARD_SELECTOR_REQUIRES_MATCHES')
    if args.delta_dirs:
        return _run_delta_shard_cli(args)
    try:
        from ikarchive.shard_reader import LosslessShardReader
        with LosslessShardReader(args.root,expected_generation=args.generation) as reader:
            # The reader has already checked this exact manifest against disk,
            # all proofs, hashes, and the full SQLite file inventory.
            manifest=json.loads((args.root/'manifest.json').read_text(encoding='utf-8'))
            if args.command=='shard-list':
                result={
                    'reader_role':reader.reader_role,
                    'baseline_only':reader.baseline_only,
                    'generation':manifest['snapshot_identifier'],
                    'source_sha256':manifest['source_sha256'],
                    'source_schema_sha256':manifest['source_schema_sha256'],
                    'table_count':len(reader.tables()),
                    'tables':reader.tables(),
                    'schema_objects':reader.schema_objects(),
                }
                _write_shard_json(result)
                return 0

            table=next((item for item in reader.tables() if item['name']==args.table),None)
            if table is None:raise ValueError('SHARD_TABLE_NOT_FOUND')
            columns=table['columns']
            selector=None
            if args.mode is None:
                rows=iter(reader.iter_rows_with_identity(args.table))
                ordinal_kind='source_table_archive_ordinal'
                scope={'kind':'full_table','table':args.table}
            else:
                relative=(f'by-mode/{args.mode}.sqlite3' if args.rule_token is None else
                          f'by-rule/{args.mode}__{args.rule_token}.sqlite3')
                selector=next((row for row in manifest['by_mode' if args.rule_token is None else 'by_rule']
                               if row['file']==relative),None)
                if selector is None:raise ValueError('SHARD_SELECTOR_NOT_FOUND')
                selected=reader.iter_selected_matches(args.mode,rule_token=args.rule_token)
                def numbered():
                    for ordinal,values in enumerate(selected):yield ordinal,None,values
                rows=iter(numbered())
                ordinal_kind='selector_stream_ordinal'
                scope={
                    'kind':'mode_selector' if args.rule_token is None else 'mode_rule_selector',
                    'analysis_set':args.mode,
                    'rule_token':args.rule_token,
                    'selected_match_count':selector['matches'],
                    'information_scope':'seed matches; all source tables and values remain available through shared shard dependencies',
                }

            # Pull the first row before writing a header so invalid selector
            # metadata or dependency proofs cannot yield a misleading stream.
            sentinel=object()
            current=next(rows,sentinel)
            header={
                'type':'header',
                'generation':manifest['snapshot_identifier'],
                'source_sha256':manifest['source_sha256'],
                'table':args.table,
                'columns':columns,
                'column_xinfo':reader.columns(args.table),
                'table_full_row_count':table['row_count'],
                'ordinal_kind':ordinal_kind,
                'source_rowid_kind':table['rowid_kind'],
                'source_rowid_projection':'original_source' if args.mode is None else 'available_in_full_table_stream',
                'scope':scope,
            }
            _write_shard_json(header)
            sys.stdout.flush()
            returned=0
            while current is not sentinel:
                if args.limit is not None and returned>=args.limit:
                    break
                ordinal,source_rowid,values=current
                _write_shard_row(ordinal,values,columns,source_rowid=source_rowid,
                                 include_source_identity=args.mode is None)
                returned+=1
                current=next(rows,sentinel)
            truncated=current is not sentinel
            _write_shard_json({'type':'footer','returned_rows':returned,'truncated':truncated,'limit':args.limit})
            sys.stdout.flush()
            return 0
    except (RuntimeError,ValueError,OSError,sqlite3.Error,StopIteration,TypeError,KeyError,UnicodeError) as exc:
        if isinstance(exc,ValueError) and str(exc).startswith('SHARD_'):
            raise
        raise ValueError('SHARD_PACKAGE_INVALID') from exc

def _run_published_cli(args):
    if args.command == 'published-read':
        if not args.table:
            raise ValueError('PUBLISHED_TABLE_NOT_FOUND')
        if args.rule_token is not None and args.mode is None:
            raise ValueError('PUBLISHED_RULE_REQUIRES_MODE')
        if args.limit is not None and args.limit <= 0:
            raise ValueError('PUBLISHED_LIMIT_MUST_BE_POSITIVE')
        if (args.mode is not None or args.rule_token is not None) and args.table != 'matches':
            raise ValueError('PUBLISHED_SELECTOR_REQUIRES_MATCHES')
    from scripts.nas_full_data_publish import valid_generation_id
    if args.generation is not None and not valid_generation_id(args.generation):
        raise ValueError('PUBLISHED_GENERATION_INVALID')

    from scripts.published_sqlite_reader import PublishedSQLiteReader, PublishedSQLiteReaderError
    try:
        with PublishedSQLiteReader(
            args.controls,
            args.root,
            args.delta_root,
            expected_generation_id=args.generation,
        ) as reader:
            if args.command == 'published-list':
                tables = reader.tables()
                result = {
                    'role': 'published_lossless_sqlite_reader',
                    'reader_role': 'published_lossless_sqlite_reader',
                    'baseline_generation': reader.baseline_generation_id,
                    'latest_generation': reader.generation_id,
                    'generation': reader.generation_id,
                    'pinned_latest_sha256': reader.pinned_latest_sha256,
                    'captured_at': reader.captured_at,
                    'published_control_binding_verified': reader.published_control_binding_verified,
                    'published_deltas_verified': reader.published_deltas_verified,
                    'table_count': len(tables),
                    'tables': tables,
                    'schema_objects': reader.schema_objects(),
                    'all_remote_artifacts_verified': False,
                    'realtime_synchronized': False,
                    'scope': 'local control + local SQLite package',
                }
            else:
                tables = reader.tables()
                table = next((item for item in tables if item['name'] == args.table), None)
                if table is None:
                    raise ValueError('PUBLISHED_TABLE_NOT_FOUND')
                columns = table['columns']

                if args.mode is None:
                    rows = reader.iter_rows(args.table)
                    ordinal_kind = 'current_table_stream_ordinal'
                    scope = {
                        'kind': 'full_table',
                        'table': args.table,
                        'source_rowid': 'original_source_rowid',
                        'scope': 'local control + local SQLite package',
                    }
                else:
                    selected = reader.iter_selected_matches(args.mode, rule_token=args.rule_token)
                    def numbered():
                        try:
                            for ordinal, values in enumerate(selected):
                                yield ordinal, None, values
                        finally:
                            close = getattr(selected, 'close', None)
                            if close is not None:
                                close()
                    rows = numbered()
                    ordinal_kind = 'selector_stream_ordinal'
                    scope = {
                        'kind': 'mode_selector' if args.rule_token is None else 'mode_rule_selector',
                        'analysis_set': args.mode,
                        'rule_token': args.rule_token,
                        'source_rowid': 'unavailable for derived selector rows; not forged',
                        'source_value_closure': 'full current source remains available through unfiltered table reads',
                        'information_scope': (
                            'selected rows are derived from the current full match_classification and matches tables; '
                            'the verified baseline-plus-delta chain retains every source table and value'
                        ),
                        'scope': 'local control + local SQLite package',
                    }

                sentinel = object()
                current = next(rows, sentinel)
                full_count = reader.row_count(args.table)
                header = {
                    'type': 'header',
                    'role': 'published_lossless_sqlite_reader',
                    'reader_role': 'published_lossless_sqlite_reader',
                    'baseline_generation': reader.baseline_generation_id,
                    'latest_generation': reader.generation_id,
                    'generation': reader.generation_id,
                    'pinned_latest_sha256': reader.pinned_latest_sha256,
                    'captured_at': reader.captured_at,
                    'published_control_binding_verified': reader.published_control_binding_verified,
                    'published_deltas_verified': reader.published_deltas_verified,
                    'all_remote_artifacts_verified': False,
                    'realtime_synchronized': False,
                    'table': args.table,
                    'columns': columns,
                    'column_xinfo': reader.columns(args.table),
                    'foreign_keys': reader.foreign_keys(args.table),
                    'table_full_row_count': full_count,
                    'ordinal_kind': ordinal_kind,
                    'source_rowid_kind': 'nullable_original_source_rowid',
                    'source_rowid_projection': 'original_source' if args.mode is None else 'unavailable_for_derived_rows',
                    'scope': scope,
                }
                _write_shard_json(header)
                sys.stdout.flush()

                returned = 0
                try:
                    while current is not sentinel:
                        if args.limit is not None and returned >= args.limit:
                            break
                        ordinal, source_rowid, values = current
                        _write_shard_row(ordinal, values, columns, source_rowid=source_rowid,
                                         include_source_identity=args.mode is None)
                        returned += 1
                        current = next(rows, sentinel)
                finally:
                    close = getattr(rows, 'close', None)
                    if close is not None:
                        close()

                truncated = current is not sentinel
                footer = {
                    'type': 'footer',
                    'returned_rows': returned,
                    'truncated': truncated,
                    'limit': args.limit,
                    'pinned_latest_sha256': reader.pinned_latest_sha256,
                    'generation': reader.generation_id,
                    'latest_generation': reader.generation_id,
                }
        if args.command == 'published-list':
            _write_shard_json(result)
            sys.stdout.flush()
            return 0
        # Run the reader's final context guards before declaring the stream
        # successful. A failure here must not leave a success footer behind.
        _write_shard_json(footer)
        sys.stdout.flush()
        return 0
    except PublishedSQLiteReaderError as exc:
        raise ValueError(exc.category) from None
    except ValueError as exc:
        if str(exc).startswith('PUBLISHED_'):
            raise
        raise ValueError('PUBLISHED_PACKAGE_INVALID') from None
    except (RuntimeError, OSError, sqlite3.Error, StopIteration, TypeError, KeyError, UnicodeError) as exc:
        raise ValueError('PUBLISHED_PACKAGE_INVALID') from None

def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',type=Path,default=DEFAULT)
    sub=parser.add_subparsers(dest='command',required=True)
    for name in ('init','status','audit','verify','refresh-catalog'):sub.add_parser(name)
    p=sub.add_parser('sync');p.add_argument('--account');p.add_argument('--nxapi-data');p.add_argument('--budget',type=int,default=250);p.add_argument('--delay',type=float,default=1.5)
    p=sub.add_parser('backup');p.add_argument('destination',type=Path)
    p=sub.add_parser('export');p.add_argument('directory',type=Path)
    p=sub.add_parser('export-xlsx');p.add_argument('destination',nargs='?',type=Path,default=OUTPUT/'exports'/'分析.xlsx')
    p=sub.add_parser('gui');p.add_argument('destination',nargs='?',type=Path,default=OUTPUT/'exports'/'gui'/'index.html')
    p=sub.add_parser('import');p.add_argument('directory',type=Path);p.add_argument('--account',required=True)
    p=sub.add_parser('sql');p.add_argument('query')
    p=sub.add_parser('install-service');p.add_argument('--interval',type=int,default=120);p.add_argument('--account');p.add_argument('--nxapi-data')
    p=sub.add_parser('tag');p.add_argument('tag_action',choices=('add','remove','list'));p.add_argument('--account');p.add_argument('--match-key');p.add_argument('--tag');p.add_argument('--note')
    p=sub.add_parser('slice-export');p.add_argument('destination',type=Path)
    sub.add_parser('slice-list')
    sub.add_parser('published-xlsx')
    p=sub.add_parser('shard-list');p.add_argument('--root',type=Path,required=True);p.add_argument('--generation');p.add_argument('--delta-dir',dest='delta_dirs',type=Path,action='append',default=[])
    p=sub.add_parser('shard-read');p.add_argument('--root',type=Path,required=True);p.add_argument('--table',required=True);p.add_argument('--mode');p.add_argument('--rule',dest='rule_token');p.add_argument('--limit',type=int);p.add_argument('--generation');p.add_argument('--delta-dir',dest='delta_dirs',type=Path,action='append',default=[])
    p=sub.add_parser('published-list');p.add_argument('--controls',type=Path,required=True);p.add_argument('--root',type=Path,required=True);p.add_argument('--delta-root',type=Path,required=True);p.add_argument('--generation')
    p=sub.add_parser('published-read');p.add_argument('--controls',type=Path,required=True);p.add_argument('--root',type=Path,required=True);p.add_argument('--delta-root',type=Path,required=True);p.add_argument('--generation');p.add_argument('--table',required=True);p.add_argument('--mode');p.add_argument('--rule',dest='rule_token');p.add_argument('--limit',type=int)
    p_rec=sub.add_parser('records')
    rec_sub=p_rec.add_subparsers(dest='subaction',required=True)
    p_list=rec_sub.add_parser('list')
    p_list.add_argument('--account',required=True)
    p_list.add_argument('--kind',choices=('vs','coop'),default=None)
    p_list.add_argument('--analysis-set',dest='analysis_set')
    p_list.add_argument('--rule',dest='rule_raw')
    p_list.add_argument('--played-from',dest='played_from')
    p_list.add_argument('--played-to',dest='played_to')
    p_list.add_argument('--weapon')
    p_list.add_argument('--tag')
    p_list.add_argument('--q',dest='query')
    p_list.add_argument('--limit',type=int,default=50)
    p_list.add_argument('--offset',type=int,default=0)
    p_facets=rec_sub.add_parser('facets')
    p_facets.add_argument('--account',required=True)
    p_summary=rec_sub.add_parser('tag-summary')
    p_summary.add_argument('--account',required=True)
    p_results=rec_sub.add_parser('rule-results')
    p_results.add_argument('--account',required=True)
    p_rates=rec_sub.add_parser('rate-summary')
    p_rates.add_argument('--account',required=True)
    p_get=rec_sub.add_parser('get')
    p_get.add_argument('--account',required=True)
    p_get.add_argument('--kind',choices=('vs','coop'),required=True)
    p_get.add_argument('--match-key',required=True)
    p_target=rec_sub.add_parser('tag-target')
    p_target.add_argument('--account',required=True)
    p_target.add_argument('--match-key',required=True)
    sub.add_parser('login')
    p=sub.add_parser('watch');p.add_argument('--interval',type=int,default=120)
    args=parser.parse_args()
    if args.command in {'shard-list','shard-read'}:
        return _run_shard_cli(args)
    if args.command in {'published-list','published-read'}:
        return _run_published_cli(args)
    args.db=args.db.resolve()
    if args.command=='export-xlsx':
        from ikarchive.xlsx_export import LEGACY_ANALYSIS_XLSX_RETIRED
        print(LEGACY_ANALYSIS_XLSX_RETIRED+'; use scripts/export_full_xlsx.py for full-data export', file=sys.stderr)
        return 4
    if args.command=='login':
        # Interactive nxapi login runs in the user's terminal, never passes tokens as argv.
        binary=ROOT/'src/node/login.mjs'
        return subprocess.call([os.environ.get('NODE','node'),str(binary)])
    if args.db==DEFAULT.resolve() and nas_storage_marker() is not None:
        raise ValueError('NAS_STORAGE_ACTIVE: use scripts/nas_archive.py; local default database access is disabled')
    if args.command=='slice-export':
        from ikarchive.slices import export_slices
        print(json.dumps(export_slices(args.db,args.destination),ensure_ascii=False,indent=2))
        return 0
    if args.command=='slice-list':
        # manifest だけを読む。正本の SQLite は開かない。
        from ikarchive.slices import list_datasets
        print(json.dumps(list_datasets(args.db),ensure_ascii=False))
        return 0
    if args.command=='published-xlsx':
        # 旧分析表は不完全なため、readerはファイルやDBへ触れず退役categoryを返す。
        from ikarchive.xlsx_export import read_published_xlsx, PublishedXlsxRetired
        try:
            payload=read_published_xlsx(args.db)
        except PublishedXlsxRetired as exc:
            print(exc.category, file=sys.stderr)
            return 4
        sys.stdout.buffer.write(payload)
        return 0
    if args.command=='records':
        if not args.account:
            raise ValueError('account must be a non-empty string')
        with RecordReader(args.db) as reader:
            if args.subaction=='list':
                result=reader.list_matches(
                    account=args.account,
                    kind=args.kind,
                    analysis_set=args.analysis_set,
                    rule_raw=args.rule_raw,
                    played_from=args.played_from,
                    played_to=args.played_to,
                    weapon=args.weapon,
                    tag=args.tag,
                    query=args.query,
                    limit=args.limit,
                    offset=args.offset,
                )
                print(json.dumps(result,ensure_ascii=False,indent=2))
                return 0
            if args.subaction=='facets':
                print(json.dumps(reader.list_facets(account=args.account),ensure_ascii=False,indent=2))
                return 0
            if args.subaction=='tag-summary':
                print(json.dumps(reader.tag_summary(account=args.account),ensure_ascii=False,indent=2))
                return 0
            if args.subaction=='rule-results':
                print(json.dumps(reader.rule_results(account=args.account),ensure_ascii=False,indent=2))
                return 0
            if args.subaction=='rate-summary':
                print(json.dumps(reader.rate_summary(account=args.account),ensure_ascii=False,indent=2))
                return 0
            if args.subaction=='get':
                if not args.match_key:
                    raise ValueError('match_key must be a non-empty string')
                item=reader.get_match(
                    account=args.account,
                    kind=args.kind,
                    match_key=args.match_key,
                )
                if item is None:
                    print('null')
                    return 0
                if item.get('source') is not None and 'body_bytes' in item['source']:
                    src=dict(item['source'])
                    body_bytes=src.pop('body_bytes')
                    src['body_base64']=base64.b64encode(body_bytes).decode('ascii')
                    # original_records の source_body token はこのcarrierを参照する。
                    # 巨大BLOBをtyped row側へ重複base64化せず、旧CLI carrier名も維持する。
                    # 既知source scalar列へ保存されたSQLite BLOBは、Python APIではbytesのまま、
                    # JSON境界でだけ型付きbase64へ変換して値を落とさない。
                    for key,value in tuple(src.items()):
                        if type(value) is bytes:
                            src[key]={'type':'BLOB','value':base64.b64encode(value).decode('ascii')}
                        elif type(value) is float and not math.isfinite(value):
                            src[key]={'type':'REAL','value':value.hex()}
                    item=dict(item)
                    item['source']=src
                print(json.dumps(item,ensure_ascii=False,indent=2))
                return 0
            if args.subaction=='tag-target':
                if not args.match_key:
                    raise ValueError('match_key must be a non-empty string')
                item=reader.tag_target(account=args.account,match_key=args.match_key)
                print('null' if item is None else json.dumps(item,ensure_ascii=False,indent=2))
                return 0
    if args.command=='watch':
        import time
        if args.interval<60:raise ValueError('interval must be >=60')
        while True:
            subprocess.run([sys.executable,str(ROOT/'archive.py'),'--db',str(args.db),'sync'])
            time.sleep(args.interval)
    readonly_commands={'status','audit','verify','sql','backup','export','gui'}
    is_readonly=(args.command in readonly_commands) or (args.command=='tag' and getattr(args,'tag_action',None)=='list')
    if is_readonly:
        if not args.db.is_file():
            raise FileNotFoundError(f'Database not found: {args.db}')
        store=Store(args.db,readonly=True)
        store.db.execute('BEGIN')
        try:
            return dispatch(store,args)
        finally:
            try:store.db.rollback()
            except sqlite3.Error:pass
            store.close()

    if database_is_slice(args.db):
        raise ValueError('SLICE_READONLY')
    args.db.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    with open(str(args.db)+'.lock','a') as lock:
        try:acquire(lock)
        except BlockingIOError:print(js({'state':'another_sync_running'}));return 0
        store=Store(args.db)
        try:
            return dispatch(store,args)
        finally:store.close()

if __name__=='__main__':
    try:sys.exit(main())
    except KeyboardInterrupt:sys.exit(130)
    except (RuntimeError,ValueError,sqlite3.Error,OSError) as exc:
        code=str(exc)
        if code in AUTH_INCIDENTS:notify_auth_incident(code)
        print(json.dumps({'error':code},ensure_ascii=False),file=sys.stderr);sys.exit(1)
