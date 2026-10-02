import base64, contextlib, io, json, sqlite3, tempfile, unittest, uuid
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/python'))
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from unittest.mock import patch
import archive
from ikarchive.store import Store, js, now
from ikarchive.planner import Planner, identity
from ikarchive.collector import allowed_asset, signed_url_expired

MANIFEST=json.loads((ROOT/'config/query-catalog.snapshot.json').read_text())

def encoded(s):return base64.b64encode(s.encode()).decode()

def response(op,body,account='account-a',variables=None,status=200):
    raw=body if isinstance(body,bytes) else js(body).encode()
    return {'event_id':str(uuid.uuid4()),'account':account,'fetched_at':now(),'operation':op,'variables':variables or {},'status':status,'body_base64':base64.b64encode(raw).decode()}

class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'a.sqlite');self.p=Planner(MANIFEST)
    def tearDown(self):self.store.close();self.tmp.cleanup()
    def ingest(self,op,body,**kwargs):
        e=response(op,body,**kwargs);i=self.store.record(e);self.store.project(i,self.p);return i
    def test_legacy_export_cli_retires_before_store_open(self):
        db=Path(self.tmp.name)/'readonly-source.sqlite3'
        db.write_bytes(b'database bytes stay unchanged')
        db.chmod(0o640)
        before=(db.read_bytes(),db.stat().st_mode&0o777,db.stat().st_mtime_ns)
        destination=Path(self.tmp.name)/'old-analysis.xlsx'
        stdout,stderr=io.StringIO(),io.StringIO()
        argv=['archive.py','--db',str(db),'export-xlsx',str(destination)]
        with patch.object(sys,'argv',argv), patch.object(archive,'Store',side_effect=AssertionError('Store opened')), \
             contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):
            code=archive.main()
        self.assertEqual(code,4)
        self.assertEqual(stdout.getvalue(),'')
        self.assertIn('LEGACY_ANALYSIS_XLSX_RETIRED',stderr.getvalue())
        self.assertIn('scripts/export_full_xlsx.py',stderr.getvalue())
        self.assertFalse(destination.exists())
        after=(db.read_bytes(),db.stat().st_mode&0o777,db.stat().st_mtime_ns)
        self.assertEqual(after,before)
        self.assertFalse(Path(str(db)+'-wal').exists())
        self.assertFalse(Path(str(db)+'-shm').exists())
        args=type('Args',(),{'command':'export-xlsx'})()
        with self.assertRaisesRegex(ValueError,'LEGACY_ANALYSIS_XLSX_RETIRED'):
            archive.dispatch(self.store,args)
    def test_current_catalog_exhaustively_classified(self):
        self.assertEqual(len(self.p.queries),113);self.assertEqual(len(self.p.routes),103);self.assertFalse(self.p.unsupported)
        self.assertEqual(set(self.p.queries),set(self.p.routes)|set(self.p.excluded))
        self.assertIn('VsHistoryDetailPagerRefetchQuery',self.p.excluded)
        for n in self.p.routes:self.assertEqual(self.p.queries[n]['params']['operationKind'],'query')
        for n in ['WeaponQuery','SideOrderRecordQuery','EventBattleHistoriesQuery','WeaponHistory_PaginationQuery']:
            self.assertIn(n,self.p.routes)
    def test_original_bytes_unknown_fields_null_and_large_integer(self):
        raw=b'{ "data": {"future": {"huge": 9999999999999999999999999, "null":null, "empty":[], "false":false, "unknown":{"x":"\\u3042"}}}}\n'
        e=response('FutureQuery',raw);rid=self.store.record(e);self.store.project(rid,self.p)
        body=self.store.db.execute('SELECT body FROM bodies').fetchone()[0];self.assertEqual(body,raw)
        fields={r['fullkey']:r['type'] for r in self.store.db.execute('SELECT * FROM all_fields')}
        self.assertIn('$.data.future.unknown.x',fields);self.assertEqual(fields['$.data.future.empty'],'array')
        self.assertEqual(fields['$.data.future.null'],'null');self.assertEqual(fields['$.data.future.false'],'false')
        self.assertEqual(self.store.verify(),[])
    def test_idempotency_and_changed_response_revision(self):
        rid=encoded('VsHistoryDetail-u-abcdefghijklmnopqrst:REGULAR:20260922T010101_11111111-1111-1111-1111-111111111111')
        detail={'id':rid,'playedTime':'2026-09-22T01:01:01Z','myTeam':{'players':[]},'otherTeams':[]}
        e=response('VsHistoryDetailQuery',{'data':{'vsHistoryDetail':detail}})
        i=self.store.record(e);self.store.project(i,self.p);self.store.record(e)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM responses').fetchone()[0],1)
        detail['future']='changed';self.ingest('VsHistoryDetailQuery',{'data':{'vsHistoryDetail':detail}})
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM documents').fetchone()[0],2)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM matches').fetchone()[0],1)
        self.assertIn('changed',self.store.db.execute('SELECT json_text FROM match_details').fetchone()[0])
    def test_summary_never_replaces_detail_and_accounts_separate(self):
        rid=encoded('VsHistoryDetail-u-abcdefghijklmnopqrst:REGULAR:20260922T010101_x')
        d={'id':rid,'myTeam':{'players':[{'name':'A','result':None}]},'otherTeams':[{'players':[{'name':'B'}]},{'players':[{'name':'C'}]}]}
        self.ingest('VsHistoryDetailQuery',{'data':{'vsHistoryDetail':d}})
        self.ingest('VsHistoryDetailPagerRefetchQuery',{'data':{'vsHistoryDetail':{'id':rid}}})
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM battle_players').fetchone()[0],3)
        self.ingest('VsHistoryDetailQuery',{'data':{'vsHistoryDetail':d}},account='account-b')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM matches').fetchone()[0],2)
    def test_mode_alias_identity(self):
        self.assertEqual(identity(encoded('VsHistoryDetail-u-test:REGULAR:20260922T010101_x')),identity(encoded('VsHistoryDetail-u-test:RECENT:20260922T010101_x')))
    def test_partial_graphql_kept_and_retried(self):
        rid=encoded('CoopHistoryDetail-u-abcdefghijklmnopqrst:20260922T010101_x');v={'coopHistoryDetailId':rid}
        self.store.queue('account-a','CoopHistoryDetailQuery',v)
        self.ingest('CoopHistoryDetailQuery',{'data':{'coopHistoryDetail':{'id':rid}},'errors':[{'message':'partial'}]},variables=v)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM pending_details').fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT state FROM jobs WHERE operation=?',('CoopHistoryDetailQuery',)).fetchone()[0],'retry')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM documents').fetchone()[0],1)
    def test_nullable_root_outcomes_are_not_false_failures(self):
        self.store.queue('account-a','useCurrentFestQuery',{})
        self.ingest('useCurrentFestQuery',{'data':{'currentFest':None}})
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE operation='useCurrentFestQuery'").fetchone()[0],'done')
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM issues WHERE code='INCOMPLETE_RESPONSE'").fetchone()[0],0)

        rid=encoded('VsHistoryDetail-u-demo:REGULAR:20260901T010101_missing')
        variables={'vsResultId':rid}
        self.store.db.execute("INSERT INTO matches VALUES(?,?,?,?,?,NULL)",
            ('account-a','vs','u-demo:20260901T010101_missing',now(),now()))
        self.store.queue('account-a','VsHistoryDetailQuery',variables,'vs','u-demo:20260901T010101_missing')
        self.ingest('VsHistoryDetailQuery',{'data':{'vsHistoryDetail':None}},variables=variables)
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE operation='VsHistoryDetailQuery'").fetchone()[0],'unavailable')
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM issues WHERE code='DETAIL_UNAVAILABLE'").fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM pending_details').fetchone()[0],0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM unavailable_details').fetchone()[0],1)
    def test_non_json_http_failure_preserved(self):
        e=response('LatestBattleHistoriesQuery',b'<html>maintenance</html>',status=503)
        i=self.store.record(e);self.store.project(i,self.p)
        self.assertIsNotNone(self.store.db.execute('SELECT parse_error FROM responses').fetchone()[0])
        self.assertEqual(self.store.db.execute('SELECT body FROM bodies').fetchone()[0],b'<html>maintenance</html>')
    def test_related_sideorder_and_event_routes(self):
        route=list(self.p.related('SideOrderTryResult',{'id':'side-id'},'JP'))
        self.assertIn(('SideOrderChallengeDetailQuery',{'tryResultId':'side-id'}),route)
        self.assertIn('SideOrderChallengeDetailPointContainerPaginationQuery',[n for n,v in route])
        self.assertIn('EventMatchRankingPeriodQuery',[n for n,v in self.p.related('LeagueMatchRankingTimePeriod',{'id':'period'},'JP')])
    def test_pagination_and_repeated_cursor_negative_control(self):
        d={'festRecords':{'edges':[],'pageInfo':{'endCursor':'next','hasNextPage':True}}}
        self.ingest('FestRecordPaginationQuery',{'data':d},variables={'cursor':None,'first':10})
        rows=self.store.db.execute('SELECT variables_json FROM jobs WHERE operation=?',('FestRecordPaginationQuery',)).fetchall()
        self.assertIn(js({'cursor':'next','first':10}),[r[0] for r in rows])
        self.ingest('FestRecordPaginationQuery',{'data':d},variables={'cursor':'next','first':10})
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM issues WHERE code='PAGINATION_STALLED'").fetchone()[0],1)
    def test_spool_crash_recovery(self):
        spool=Path(self.tmp.name)/'spool';spool.mkdir();e=response('HistoryRecordQuery',{'data':{'playHistory':{}}})
        f=spool/'e.json';f.write_text(js(e));self.store.record(e)
        self.store.recover(self.p,spool)
        self.assertFalse(f.exists());self.assertEqual(self.store.db.execute('SELECT count(*) FROM responses').fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT projected FROM responses').fetchone()[0],1)
    def test_actual_public_response_fixtures(self):
        for n in ['VsHistoryDetailQuery','VsHistoryDetailQueryDisconnection','CoopHistoryDetailQuery']:
            data=json.loads((ROOT/'tests/fixtures/public'/f'{n}.json').read_text())
            self.ingest('CoopHistoryDetailQuery' if n.startswith('Coop') else 'VsHistoryDetailQuery',data)
        self.assertGreater(self.store.db.execute('SELECT count(*) FROM battle_players').fetchone()[0],0)
        self.assertGreater(self.store.db.execute('SELECT count(*) FROM salmon_players').fetchone()[0],0)
        self.assertGreater(self.store.db.execute('SELECT count(*) FROM salmon_waves').fetchone()[0],0)
        self.assertEqual(self.store.verify(),[])
    def test_media_bytes_and_host_guard(self):
        self.ingest('PhotoAlbumQuery',{'data':{'photoAlbum':{'image':{'url':'https://example.invalid/test.jpg'}}}})
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM assets').fetchone()[0],1)
        self.assertFalse(allowed_asset('https://nintendo.net.attacker.test/a'))
        self.assertFalse(allowed_asset('http://api.lp1.av5ja.srv.nintendo.net/a'))
        self.assertTrue(allowed_asset('https://api.lp1.av5ja.srv.nintendo.net/a'))
        bucket='ugcstore-toyohr-lp1-persistent'
        self.assertTrue(allowed_asset(f'https://storage.googleapis.com/{bucket}/photo'))
        self.assertTrue(allowed_asset(f'https://storage.googleapis.com/{bucket}/photo?Expires=1&GoogleAccessId=a&Signature=b'))
        self.assertFalse(allowed_asset('https://storage.googleapis.com/other-bucket/photo'))
        self.assertFalse(allowed_asset('https://storage.googleapis.com/'))
        self.assertFalse(allowed_asset(f'http://storage.googleapis.com/{bucket}/photo'))
        self.assertFalse(allowed_asset(f'https://user:pw@storage.googleapis.com/{bucket}/photo'))
        self.assertFalse(allowed_asset(f'https://storage.googleapis.com.attacker.test/{bucket}/photo'))
        self.assertFalse(allowed_asset(f'https://evil.storage.googleapis.com/{bucket}/photo'))
        past=f'https://storage.googleapis.com/{bucket}/photo?Expires=1&GoogleAccessId=a&Signature=b'
        future=f'https://storage.googleapis.com/{bucket}/photo?Expires=4102444800&GoogleAccessId=a&Signature=b'
        self.assertTrue(signed_url_expired(past, 1_700_000_000))
        self.assertFalse(signed_url_expired(future, 1_700_000_000))
        self.assertFalse(signed_url_expired(f'https://storage.googleapis.com/{bucket}/photo', 1_700_000_000))
        self.assertTrue(signed_url_expired(
            f'https://storage.googleapis.com/{bucket}/photo?X-Goog-Date=20200101T000000Z&X-Goog-Expires=60&X-Goog-Signature=b',
            1_700_000_000))
        self.assertFalse(signed_url_expired(
            f'https://storage.googleapis.com/{bucket}/photo?X-Goog-Date=20990101T000000Z&X-Goog-Expires=60&X-Goog-Signature=b',
            1_700_000_000))
        self.assertFalse(signed_url_expired(f'https://storage.googleapis.com/{bucket}/photo?Expires=nope', 1_700_000_000))
    def test_late_recovery_does_not_overwrite_newer_detail(self):
        remote=encoded('VsHistoryDetail-u-demo:REGULAR:20260922T010101_x')
        v={'vsResultId':remote}
        self.store.queue('account-a','VsHistoryDetailQuery',v)
        for day,label in [('2026-09-22','new'),('2026-09-21','old')]:
            e=response('VsHistoryDetailQuery',{'data':{'vsHistoryDetail':{'id':remote,'label':label}}},variables=v)
            e['fetched_at']=day+'T00:00:00Z'
            i=self.store.record(e);self.store.project(i,self.p)
        self.assertIn('new',self.store.db.execute('SELECT json_text FROM match_details').fetchone()[0])
        last=self.store.db.execute("SELECT r.fetched_at FROM jobs j JOIN responses r ON r.id=j.last_response_id WHERE j.operation='VsHistoryDetailQuery'").fetchone()[0]
        self.assertEqual(last,'2026-09-22T00:00:00Z')
    def test_content_reversion_advances_endpoint_head(self):
        values=[]
        for day,label in [('20','A'),('21','B'),('22','A')]:
            e=response('HistoryRecordQuery',{'data':{'playHistory':{'label':label}}})
            e['fetched_at']='2026-09-'+day+'T00:00:00Z'
            rid=self.store.record(e);self.store.project(rid,self.p);values.append(rid)
        self.assertEqual(values[0],values[2])
        self.assertNotEqual(values[0],values[1])
        head=self.store.db.execute("SELECT response_id FROM endpoint_heads WHERE operation='HistoryRecordQuery'").fetchone()[0]
        self.assertEqual(head,values[0])
    def test_missing_selected_field_is_not_silent(self):
        q={'queries':{'Q':{'params':{'operationKind':'query','id':'abc'},'operation':{'argumentDefinitions':[],'selections':[{'kind':'LinkedField','name':'record','alias':None,'concreteType':'Record','selections':[{'kind':'ScalarField','name':'required','alias':None}]}]}}}}
        p=Planner(q);self.store.queue('account-a','Q',{})
        e=response('Q',{'data':{'record':{}}});e['query_id']='abc'
        i=self.store.record(e);self.store.project(i,p)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM issues WHERE code='SELECTED_FIELD_MISSING'").fetchone()[0],1)
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE operation='Q'").fetchone()[0],'retry')
    def test_saturated_history_without_overlap_is_reported(self):
        for key in ['old','new']:
            rid=encoded('VsHistoryDetail-u-demo:REGULAR:'+key)
            self.ingest('LatestBattleHistoriesQuery',{'data':{'latestBattleHistories':{'historyGroups':{'nodes':[{'historyDetails':{'nodes':[{'id':rid}]}}]}}}})
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM issues WHERE code='POSSIBLE_HISTORY_GAP'").fetchone()[0],1)
    def test_unknown_required_variable_is_audited(self):
        m={'queries':{'FutureQuery':{'params':{'operationKind':'query'},'operation':{'argumentDefinitions':[{'name':'unknownFilter','defaultValue':None}],'selections':[]}}}}
        p=Planner(m);self.assertIn('FutureQuery',p.unsupported);self.assertNotIn('FutureQuery',p.routes)
    def test_backup_consistency_and_corruption_negative_control(self):
        self.ingest('HistoryRecordQuery',{'data':{'playHistory':{}}})
        dest=Path(self.tmp.name)/'backup.sqlite'
        db=sqlite3.connect(dest)
        try:self.store.db.backup(db)
        finally:db.close()
        other=Store(dest);self.assertEqual(other.verify(),[]);other.close()
        self.store.db.execute("UPDATE bodies SET body=x'00'");self.store.db.commit()
        self.assertEqual(len(self.store.verify()),1)

if __name__=='__main__':unittest.main()
