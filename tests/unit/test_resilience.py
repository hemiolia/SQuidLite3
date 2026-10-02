import argparse
from contextlib import closing
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / 'src/python') not in sys.path:
    sys.path.insert(0, str(ROOT / 'src/python'))
if str(ROOT / 'tests/unit') not in sys.path:
    sys.path.insert(0, str(ROOT / 'tests/unit'))

from test_archive import Store, MANIFEST, response
from ikarchive.collector import sync, fetch_assets
from ikarchive.store import now

class ResilienceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'database' / 'archive.sqlite3')

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_authoritative_writer_records_replace_implicit_deletions(self):
        from ikarchive.change_feed import install_change_feed
        self.store.db.execute('CREATE TABLE replace_source(value TEXT UNIQUE)')
        self.store.db.execute("INSERT INTO replace_source(rowid,value) VALUES(-8,'original')")
        self.store.db.commit()
        install_change_feed(self.store.db)
        self.store.db.execute("INSERT OR REPLACE INTO replace_source(rowid,value) VALUES(12,'original')")
        operations = [tuple(row) for row in self.store.db.execute(
            "SELECT operation,old_rowid,new_rowid FROM archive_change_feed WHERE table_name='replace_source' ORDER BY event_id")]
        self.assertEqual(operations, [('DELETE', -8, None), ('INSERT', None, 12)])

    def test_storage_critical_blocks_sync_without_initializing_bridge(self):
        """1. storage_healthをcriticalへモックするとsyncがSTORAGE_CRITICALで失敗し、
        Bridgeは初期化されずrun blocked/error記録、auth.reauth_requiredはfalse、last_sync_errorがSTORAGE_CRITICAL。"""
        with patch('ikarchive.collector.storage_health', return_value={'state': 'critical'}), \
             patch('ikarchive.collector.Bridge') as mock_bridge:
            with self.assertRaisesRegex(RuntimeError, 'STORAGE_CRITICAL'):
                sync(self.store)
        mock_bridge.assert_not_called()
        row = self.store.db.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row['status'], 'blocked')
        self.assertEqual(row['error'], 'STORAGE_CRITICAL')
        self.assertIsNotNone(row['finished_at'])

        auth = self.store.auth_status()
        self.assertFalse(auth['reauth_required'])
        self.assertEqual(auth['last_sync_error'], 'STORAGE_CRITICAL')
        self.assertRegex(auth['last_sync_error_at'], r'^\d{4}-\d{2}-\d{2}T')

    def test_storage_low_defers_non_history_queries_and_resumes_when_normal(self):
        """2. lowへモック、manifest RegularBattleHistoriesQueryとHistoryRecordQueryの2つ（test_autosync.py参照）。
        FakeBridgeで履歴だけ取得され、HistoryRecordQueryはpending保持。後でnormalへ戻しsyncすると残が処理される。fetch_assetsを0へモック。"""
        manifest = {
            **MANIFEST,
            'queries': {k: MANIFEST['queries'][k] for k in ('RegularBattleHistoriesQuery', 'HistoryRecordQuery')},
            'expected': 2,
        }
        seen = []
        store = self.store

        class FakeBridge:
            def call(self, command, **kw):
                if command == 'init':
                    return {'account': 'account-a'}
                op = kw['operation']
                seen.append(op)
                body = (
                    {'data': {'regularBattleHistories': {'historyGroups': {'nodes': []}}}}
                    if op == 'RegularBattleHistoriesQuery'
                    else {'data': {'playHistory': {}}}
                )
                e = response(op, body, variables=kw['variables'])
                p = store.output_root / 'spool' / (e['event_id'] + '.json')
                p.write_text(json.dumps(e))
                return {'spool_file': str(p)}

            def close(self):
                pass

        # 1. low空間では履歴 (priority 0) のみ取得され、HistoryRecordQuery (priority 2) は pending 保持
        with patch('ikarchive.collector.catalog', return_value=manifest), \
             patch('ikarchive.collector.Bridge', FakeBridge), \
             patch('ikarchive.collector.fetch_assets', return_value=0), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'low'}):
            res_low = sync(self.store, budget=10, delay=0)

        self.assertEqual(seen, ['RegularBattleHistoriesQuery'])
        self.assertEqual(res_low['state'], 'partial')
        history_job = self.store.db.execute("SELECT state FROM jobs WHERE operation='RegularBattleHistoriesQuery'").fetchone()
        self.assertEqual(history_job['state'], 'done')
        record_job = self.store.db.execute("SELECT state FROM jobs WHERE operation='HistoryRecordQuery'").fetchone()
        self.assertEqual(record_job['state'], 'pending')

        # 2. normal空間へ戻してsyncすると残りのHistoryRecordQueryが取得されdoneになる
        with patch('ikarchive.collector.catalog', return_value=manifest), \
             patch('ikarchive.collector.Bridge', FakeBridge), \
             patch('ikarchive.collector.fetch_assets', return_value=0), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}):
            res_norm = sync(self.store, budget=10, delay=0)

        self.assertIn('HistoryRecordQuery', seen)
        record_job_after = self.store.db.execute("SELECT state FROM jobs WHERE operation='HistoryRecordQuery'").fetchone()
        self.assertEqual(record_job_after['state'], 'done')
        pending_count = self.store.db.execute("SELECT count(*) FROM jobs WHERE state IN ('pending', 'retry')").fetchone()[0]
        self.assertEqual(pending_count, 0)
        self.assertEqual(res_norm['state'], 'available_routes_collected')

    def test_previous_running_run_marked_interrupted_preserving_partial(self):
        """3. 旧runsにrunning/partialを挿入しsync時前runningだけinterruptedへ変わる（新runは普通に終わる）。"""
        self.store.db.execute("INSERT INTO runs(id, started_at, status, account) VALUES(1, ?, 'running', 'account-a')", (now(),))
        self.store.db.execute("INSERT INTO runs(id, started_at, status, account) VALUES(2, ?, 'partial', 'account-a')", (now(),))
        self.store.db.commit()

        manifest = {
            **MANIFEST,
            'queries': {k: MANIFEST['queries'][k] for k in ('RegularBattleHistoriesQuery',)},
            'expected': 1,
        }
        store = self.store

        class FakeBridge:
            def call(self, command, **kw):
                if command == 'init':
                    return {'account': 'account-a'}
                op = kw['operation']
                body = {'data': {'regularBattleHistories': {'historyGroups': {'nodes': []}}}}
                e = response(op, body, variables=kw['variables'])
                p = store.output_root / 'spool' / (e['event_id'] + '.json')
                p.write_text(json.dumps(e))
                return {'spool_file': str(p)}
            def close(self):
                pass

        with patch('ikarchive.collector.catalog', return_value=manifest), \
             patch('ikarchive.collector.Bridge', FakeBridge), \
             patch('ikarchive.collector.fetch_assets', return_value=0), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}):
            sync(self.store, budget=5, delay=0)

        r1 = self.store.db.execute("SELECT * FROM runs WHERE id=1").fetchone()
        self.assertEqual(r1['status'], 'interrupted')
        self.assertEqual(r1['error'], 'PROCESS_INTERRUPTED')
        self.assertIsNotNone(r1['finished_at'])

        r2 = self.store.db.execute("SELECT * FROM runs WHERE id=2").fetchone()
        self.assertEqual(r2['status'], 'partial')
        self.assertIsNone(r2['error'])

        new_run = self.store.db.execute("SELECT * FROM runs WHERE id > 2 ORDER BY id DESC LIMIT 1").fetchone()
        self.assertIsNotNone(new_run)
        self.assertEqual(new_run['status'], 'available_routes_collected')
        self.assertIsNone(new_run['error'])
        self.assertIsNotNone(new_run['finished_at'])

    def test_fetch_assets_skips_on_low_or_critical_and_breaks_on_time_limit(self):
        """4. fetch_assetsはlow/criticalでダウンロードしない。normalでは時間枠を越えたら残をpendingのまま返す。通信はFakeOpenerで代替。"""
        class FakeResponse:
            def __init__(self, data=b'fake-image-bytes', content_type='image/png'):
                self.data = data
                self.headers = {'content-type': content_type}
            def read(self):
                return self.data
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass

        class FakeOpener:
            def __init__(self):
                self.opened = []
            def open(self, url, timeout=45):
                self.opened.append((url, timeout))
                return FakeResponse()

        fake_opener = FakeOpener()

        urls = [f'https://api.lp1.nintendo.net/assets/{i}.png' for i in range(4)]
        with self.store.db:
            for u in urls:
                self.store.db.execute("INSERT INTO assets(url, state) VALUES(?, 'pending')", (u,))

        with patch('ikarchive.collector.urllib.request.build_opener', return_value=fake_opener):
            # 1. low容量ではダウンロードしない
            with patch('ikarchive.collector.storage_health', return_value={'state': 'low'}):
                c_low = fetch_assets(self.store, budget=10, delay=0)
            self.assertEqual(c_low, 0)
            self.assertEqual(len(fake_opener.opened), 0)

            # 2. critical容量でもダウンロードしない
            with patch('ikarchive.collector.storage_health', return_value={'state': 'critical'}):
                c_crit = fetch_assets(self.store, budget=10, delay=0)
            self.assertEqual(c_crit, 0)
            self.assertEqual(len(fake_opener.opened), 0)

            # 3. normal容量で時間枠（HISTORY_REFRESH_SECONDS=120秒）を超過した場合、残をpendingのまま返す
            with patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}), \
                 patch('ikarchive.collector.time.monotonic', side_effect=[0, 0, 121, 121]):
                c_norm = fetch_assets(self.store, budget=10, delay=0)

            self.assertEqual(c_norm, 1)
            self.assertEqual(len(fake_opener.opened), 1)
            done_count = self.store.db.execute("SELECT count(*) FROM assets WHERE state='done'").fetchone()[0]
            self.assertEqual(done_count, 1)
            pending_count = self.store.db.execute("SELECT count(*) FROM assets WHERE state='pending'").fetchone()[0]
            self.assertEqual(pending_count, 3)

    def test_expired_signed_url_is_not_requested(self):
        bucket = 'ugcstore-toyohr-lp1-persistent'
        clock = 1_700_000_000
        expired = f'https://storage.googleapis.com/{bucket}/old?Expires=1&GoogleAccessId=a&Signature=b'
        fresh = f'https://storage.googleapis.com/{bucket}/new?Expires=4102444800&GoogleAccessId=a&Signature=b'
        blocked = 'https://storage.googleapis.com/other-bucket/photo?Expires=1'
        class FakeResponse:
            def __init__(self):
                self.headers = {'content-type': 'image/png'}
            def read(self):
                return b'png'
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
        class FakeOpener:
            def __init__(self):
                self.opened = []
            def open(self, url, timeout=45):
                self.opened.append(url)
                return FakeResponse()
        opener = FakeOpener()
        with self.store.db:
            for url in (expired, fresh, blocked):
                self.store.db.execute("INSERT INTO assets(url, state, next_attempt) VALUES(?, 'pending', 0)", (url,))
        with patch('ikarchive.collector.urllib.request.build_opener', return_value=opener), \
             patch('ikarchive.collector.storage_health', return_value={'state': 'normal'}), \
             patch('ikarchive.collector.time.time', return_value=clock):
            started = time.monotonic()
            attempted = fetch_assets(self.store, budget=10, delay=0)
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(attempted, 3)
        self.assertEqual(opener.opened, [fresh])
        rows = {
            row['url']: row
            for row in self.store.db.execute('SELECT url, state, attempts, next_attempt, last_error FROM assets')
        }
        self.assertEqual(rows[expired]['state'], 'retry')
        self.assertEqual(rows[expired]['last_error'], 'AssetUrlExpired')
        self.assertEqual(rows[expired]['attempts'], 1)
        self.assertEqual(rows[expired]['next_attempt'], clock + 86400)
        self.assertEqual(rows[fresh]['state'], 'done')
        self.assertIsNone(rows[fresh]['last_error'])
        self.assertEqual(rows[blocked]['state'], 'retry')
        self.assertEqual(rows[blocked]['last_error'], 'ValueError')
        self.assertEqual(rows[blocked]['next_attempt'], clock + 3600)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM assets WHERE state!='done'").fetchone()[0], 2)

    def test_auth_expired_sets_reauth_required_and_remember_auth_recovers(self):
        """5. AUTH_EXPIRED記録後はreauth_required trueとなりremember_auth成功でfalseへ戻る。"""
        # 初期状態
        auth = self.store.auth_status()
        self.assertFalse(auth['reauth_required'])
        self.assertIsNone(auth['last_failure'])

        # AUTH_EXPIRED 記録
        self.store.remember_auth_failure('AUTH_EXPIRED')
        auth_after_failure = self.store.auth_status()
        self.assertTrue(auth_after_failure['reauth_required'])
        self.assertEqual(auth_after_failure['last_failure'], 'AUTH_EXPIRED')
        self.assertIsNotNone(auth_after_failure['last_failure_at'])

        # remember_auth 成功で復旧
        now_ms = int(time.time() * 1000)
        self.store.remember_auth({
            'session_iat': int(time.time()),
            'session_expires_at': now_ms + 86400 * 1000,
            'bullet_expires_at': now_ms + 7200 * 1000,
        })
        auth_recovered = self.store.auth_status()
        self.assertFalse(auth_recovered['reauth_required'])
        self.assertIsNone(auth_recovered['last_failure'])
        self.assertIsNone(auth_recovered['last_failure_at'])
        self.assertIsNotNone(auth_recovered['last_ok_at'])

    def test_archive_dispatch_backup_resilience(self):
        """6. archive.pyをimportlibで読み込みdispatch backupを人工SQLiteで実行し、
        正常バックアップのintegrity、既存先非上書き、space不足時不完全ファイル不作成、
        backup途中例外時tmpとjournalが残らず既存ファイル不変を検査（sqlite connectのfake proxyでbackup例外等）。"""
        spec = importlib.util.spec_from_file_location("archive", ROOT / "archive.py")
        archive_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(archive_mod)
        dispatch = archive_mod.dispatch

        backup_dir = Path(self.tmp.name) / 'backups'
        backup_dest = backup_dir / 'valid_backup.sqlite3'
        args_valid = argparse.Namespace(command='backup', destination=backup_dest)

        # 1. 正常バックアップの integrity
        with patch('ikarchive.storage.storage_health', return_value={'free_bytes': 10 * 1024**3, 'total_bytes': 100 * 1024**3, 'low_space_bytes': 2 * 1024**3, 'critical_space_bytes': 512 * 1024**2, 'state': 'normal'}):
            with io.StringIO() as buf, patch('sys.stdout', buf):
                code = dispatch(self.store, args_valid)
                output = json.loads(buf.getvalue())

        self.assertEqual(code, 0)
        self.assertEqual(output['integrity'], 'ok')
        self.assertEqual(output['backup'], str(backup_dest.resolve()))
        self.assertTrue(backup_dest.is_file())

        with closing(sqlite3.connect(backup_dest)) as bconn, bconn:
            bconn.row_factory = sqlite3.Row
            integrity = bconn.execute('PRAGMA integrity_check').fetchone()[0]
            self.assertEqual(integrity, 'ok')
            version_count = bconn.execute('SELECT count(*) FROM schema_version').fetchone()[0]
            self.assertGreaterEqual(version_count, 1)

        # 2. 既存先非上書き
        dest_mtime = backup_dest.stat().st_mtime_ns
        dest_bytes = backup_dest.read_bytes()
        with self.assertRaises(ValueError):
            dispatch(self.store, args_valid)
        self.assertEqual(backup_dest.read_bytes(), dest_bytes)
        self.assertEqual(backup_dest.stat().st_mtime_ns, dest_mtime)

        # 3. space不足時不完全ファイル不作成
        insufficient_dest = backup_dir / 'insufficient.sqlite3'
        args_insufficient = argparse.Namespace(command='backup', destination=insufficient_dest)
        with patch('ikarchive.storage.storage_health', return_value={'free_bytes': 0, 'state': 'critical'}):
            with self.assertRaisesRegex(RuntimeError, 'BACKUP_INSUFFICIENT_SPACE'):
                dispatch(self.store, args_insufficient)
        self.assertFalse(insufficient_dest.exists())
        self.assertEqual(list(backup_dir.glob('insufficient.sqlite3.*.tmp*')), [])

        # 4. backup途中例外時tmpとjournalが残らず既存ファイル不変を検査
        failing_dest = backup_dir / 'failing_backup.sqlite3'
        args_failing = argparse.Namespace(command='backup', destination=failing_dest)

        orig_db = self.store.db

        class FakeFailingDB:
            def __init__(self, real):
                self.real = real
            def backup(self, target):
                cur = target.cursor()
                cur.execute('PRAGMA database_list')
                db_file = cur.fetchone()[2]
                Path(f'{db_file}-journal').touch()
                raise sqlite3.OperationalError('simulated backup error')
            def __getattr__(self, name):
                return getattr(self.real, name)

        self.store.db = FakeFailingDB(orig_db)
        try:
            with patch('ikarchive.storage.storage_health', return_value={'free_bytes': 10 * 1024**3, 'total_bytes': 100 * 1024**3, 'low_space_bytes': 2 * 1024**3, 'critical_space_bytes': 512 * 1024**2, 'state': 'normal'}):
                with self.assertRaises(sqlite3.OperationalError):
                    dispatch(self.store, args_failing)
        finally:
            self.store.db = orig_db

        self.assertFalse(failing_dest.exists())
        self.assertEqual(list(backup_dir.glob('failing_backup.sqlite3.*.tmp*')), [])
        self.assertEqual(list(backup_dir.glob('*-journal')), [])
        # 既存の valid_backup.sqlite3 が不変であること
        self.assertEqual(backup_dest.read_bytes(), dest_bytes)
        self.assertEqual(backup_dest.stat().st_mtime_ns, dest_mtime)

if __name__ == '__main__':
    unittest.main()
