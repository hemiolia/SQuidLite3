import json, tempfile, unittest
from pathlib import Path
from test_archive import Store, Planner, MANIFEST, response

class FetchReceiptTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=Store(Path(self.tmp.name)/'a.sqlite')
        self.planner=Planner(MANIFEST)
        self.op='HistoryRecordQuery'
        self.store.queue('account-a',self.op,{})
        self.store.db.commit()
    def tearDown(self):
        self.store.close();self.tmp.cleanup()
    def ingest(self,event):
        rid=self.store.record(event);self.store.project(rid,self.planner);return rid
    def job(self):
        return dict(self.store.db.execute('SELECT * FROM jobs WHERE operation=?',(self.op,)).fetchone())
    def test_identical_refetch_finishes_job_without_duplicate_body_or_document(self):
        e=response(self.op,{'data':{'playHistory':{'udemae':'S+8'}}})
        a=self.ingest(e)
        self.store.db.execute("UPDATE jobs SET state='pending',next_attempt=0");self.store.db.commit()
        fresh=response(self.op,{'data':{'playHistory':{'udemae':'S+8'}}})
        fresh['headers']={'x-test':'new'}
        self.assertEqual(a,self.ingest(fresh))
        self.assertEqual(self.job()['state'],'done')
        self.assertEqual(self.job()['attempts'],2)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM responses').fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM bodies').fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM response_fetches').fetchone()[0],2)
        self.assertEqual(self.store.db.execute('SELECT headers_json FROM response_fetches WHERE event_id=?',(fresh['event_id'],)).fetchone()[0],'{"x-test":"new"}')
        self.ingest(fresh)
        self.assertEqual(self.job()['attempts'],2)
    def test_http_status_is_not_deduplicated_as_a_success(self):
        body={'data':{'playHistory':{}}}
        first=self.ingest(response(self.op,body))
        failed=self.ingest(response(self.op,body,status=503))
        self.assertNotEqual(first,failed)
        self.assertEqual(self.job()['state'],'retry')
    def test_late_receipt_cannot_undo_recent_result(self):
        failed=response(self.op,{'data':{'playHistory':{}}},status=503)
        failed['fetched_at']='2026-09-21T00:00:00Z'
        success=response(self.op,{'data':{'playHistory':{}}})
        success['fetched_at']='2026-09-22T00:00:00+00:00'
        self.ingest(success);self.ingest(failed)
        self.assertEqual(self.job()['state'],'done')
    def test_crash_between_record_and_projection_recovers_duplicate_receipt(self):
        body={'data':{'playHistory':{}}}
        self.ingest(response(self.op,body))
        self.store.db.execute("UPDATE jobs SET state='pending',next_attempt=0");self.store.db.commit()
        e=response(self.op,body);self.store.record(e)
        spool=Path(self.tmp.name)/'spool';spool.mkdir()
        (spool/'test.json').write_text(json.dumps(e))
        self.store.recover(self.planner,spool)
        self.assertEqual(self.job()['state'],'done')
        self.assertEqual(self.job()['attempts'],2)
        self.assertFalse(list(spool.iterdir()))
    def test_sync_moves_past_identical_history_to_other_routes(self):
        from unittest.mock import patch
        from ikarchive.collector import sync
        manifest={**MANIFEST,'queries':{k:MANIFEST['queries'][k] for k in ('RegularBattleHistoriesQuery','HistoryRecordQuery')}}
        manifest['expected']=2
        store=self.store
        seen=[]
        class FakeBridge:
            def call(self,command,**kw):
                if command=='init':return {'account':'account-a','country':'JP'}
                op=kw['operation'];seen.append(op)
                body={'data':{'regularBattleHistories':{'historyGroups':{'nodes':[]}}}} if op=='RegularBattleHistoriesQuery' else {'data':{'playHistory':{}}}
                e=response(op,body,variables=kw['variables'])
                f=store.output_root/'spool'/(e['event_id']+'.json');f.write_text(json.dumps(e));return {'spool_file':str(f)}
            def close(self):pass
        with patch('ikarchive.collector.catalog',return_value=manifest),patch('ikarchive.collector.Bridge',FakeBridge),patch('ikarchive.collector.fetch_assets',return_value=0):
            first=sync(store,budget=5,delay=0)
            self.assertEqual(first['requests'],2)
            store.db.execute("UPDATE jobs SET state='pending',next_attempt=0 WHERE operation='HistoryRecordQuery'");store.db.commit()
            seen.clear();second=sync(store,budget=5,delay=0)
            self.assertEqual(second['requests'],2)
            self.assertEqual(seen,['RegularBattleHistoriesQuery','HistoryRecordQuery'])
    def test_content_reversion_becomes_current_without_duplicate_match(self):
        from test_archive import encoded
        rid=encoded('VsHistoryDetail-u-demo:REGULAR:20260922T010101_x')
        ids=[]
        for day,label in [('20','A'),('21','B'),('22','A')]:
            e=response('VsHistoryDetailQuery',{'data':{'vsHistoryDetail':{'id':rid,'label':label}}},variables={'vsResultId':rid})
            e['fetched_at']='2026-09-'+day+'T00:00:00Z'
            ids.append(self.ingest(e))
        self.assertEqual(ids[0],ids[2])
        self.assertNotEqual(ids[0],ids[1])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM matches').fetchone()[0],1)
        self.assertEqual(self.store.db.execute('SELECT detail_response_id FROM matches').fetchone()[0],ids[0])
        # last_seen は「その試合を載せた新しい内容の応答を最後に保存した時刻」(B の 09-21)。A の取り直し(09-22)では進めない。
        # 取り直しを含む最新の観測時刻 09-22 は match_observations.last_observed_at が持つ(test_store_reobserve.py)。
        self.assertEqual(self.store.db.execute('SELECT last_seen FROM matches').fetchone()[0],'2026-09-21T00:00:00Z')
