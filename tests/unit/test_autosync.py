import json, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from test_archive import Store, MANIFEST, response
from ikarchive.collector import sync
from ikarchive.publish import publish_outputs

class AutomaticSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'database'/'archive.sqlite3')
    def tearDown(self):
        self.store.close();self.tmp.cleanup()
    def test_long_crawl_refreshes_live_history_without_looping_identical_body(self):
        manifest={**MANIFEST,'queries':{k:MANIFEST['queries'][k] for k in ('RegularBattleHistoriesQuery','HistoryRecordQuery')},'expected':2}
        seen=[];store=self.store
        legacy_book=store.output_root/'exports/分析.xlsx'
        legacy_book.parent.mkdir(parents=True,exist_ok=True)
        legacy_book.write_bytes(b'legacy bytes stay unchanged during sync')
        legacy_book.chmod(0o640)
        legacy_state=(legacy_book.read_bytes(),legacy_book.stat().st_mode&0o777,legacy_book.stat().st_mtime_ns)
        class Bridge:
            def call(self,command,**kw):
                if command=='init':return {'account':'account-a'}
                op=kw['operation'];seen.append(op)
                body={'data':{'regularBattleHistories':{'historyGroups':{'nodes':[]}}}} if op=='RegularBattleHistoriesQuery' else {'data':{'playHistory':{}}}
                e=response(op,body,variables=kw['variables']);p=store.output_root/'spool'/(e['event_id']+'.json');p.write_text(json.dumps(e));return {'spool_file':str(p)}
            def close(self):pass
        with patch('ikarchive.collector.catalog',return_value=manifest),patch('ikarchive.collector.Bridge',Bridge),patch('ikarchive.collector.fetch_assets',return_value=0),patch('ikarchive.collector.time.monotonic',side_effect=[0,0,0,121,121,121]):
            result=sync(store,budget=10,delay=0)
        self.assertEqual(seen,['RegularBattleHistoriesQuery','HistoryRecordQuery','RegularBattleHistoriesQuery'])
        self.assertEqual(result['requests'],3)
        self.assertEqual(store.db.execute('SELECT count(*) FROM responses').fetchone()[0],2)
        self.assertTrue((store.output_root/'exports/gui/index.html').is_file())
        self.assertEqual((legacy_book.read_bytes(),legacy_book.stat().st_mode&0o777,legacy_book.stat().st_mtime_ns),legacy_state)
    def test_gui_only_publish_preserves_existing_legacy_workbook(self):
        page=self.store.output_root/'exports/gui/index.html'
        book=self.store.output_root/'exports/分析.xlsx'
        book.parent.mkdir(parents=True,exist_ok=True)
        book.write_bytes(b'legacy workbook bytes must not change')
        book.chmod(0o640)
        old_book=(book.read_bytes(),book.stat().st_mode&0o777,book.stat().st_mtime_ns)
        result=publish_outputs(self.store)
        self.assertEqual(result['scope'],'gui_only')
        self.assertNotIn('xlsx',result)
        self.assertTrue(page.is_file())
        self.assertEqual((book.read_bytes(),book.stat().st_mode&0o777,book.stat().st_mtime_ns),old_book)
        old=page.read_bytes()
        self.assertIsNotNone(self.store._control('exports_updated_at'))
        with patch('ikarchive.publish.write_gui',side_effect=OSError('test')):
            failed=publish_outputs(self.store)
        self.assertEqual(failed,{'error':'OSError'})
        self.assertEqual(self.store.sync_health()['export_error'],'OSError')
        self.assertEqual(page.read_bytes(),old)
        recovered=publish_outputs(self.store)
        self.assertEqual(recovered['scope'],'gui_only')
        self.assertEqual((book.read_bytes(),book.stat().st_mode&0o777,book.stat().st_mtime_ns),old_book)
        self.assertIsNone(self.store._control('export_error'))
    def test_gui_only_publish_does_not_create_legacy_workbook(self):
        book=self.store.output_root/'exports/分析.xlsx'
        result=publish_outputs(self.store)
        self.assertEqual(result['scope'],'gui_only')
        self.assertNotIn('xlsx',result)
        self.assertTrue((self.store.output_root/'exports/gui/index.html').is_file())
        self.assertFalse(book.exists())
    def test_missing_history_is_never_reported_current(self):
        health=self.store.sync_health()
        self.assertEqual(health['state'],'delayed')
        self.assertEqual(len(health['histories']),7)
        self.assertTrue(all(h['stale'] for h in health['histories']))
