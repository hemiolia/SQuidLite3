"""稼働中（WAL あり）の DB を開く前の判定が、WAL にしか無い変更を見落とさないことの検査。"""
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src/python'))
from ikarchive.store import _immutable_uri, database_is_slice  # noqa: E402


class WalProbeTest(unittest.TestCase):
    def test_probe_reads_schema_that_exists_only_in_wal(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'live.sqlite3'
            writer = sqlite3.connect(path)
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('CREATE TABLE a(x)')
            writer.commit()
            writer.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            # ここからの変更は WAL にだけある（本体ファイルへ未反映）。
            writer.execute('CREATE TABLE slice_meta(key, value)')
            writer.commit()
            self.assertTrue(Path(str(path) + '-wal').stat().st_size > 0)
            self.assertNotIn('immutable=1', _immutable_uri(path))
            # 旧実装（immutable=1）なら WAL を読まず slice_meta を見落とす。
            old = sqlite3.connect(path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
            self.assertIsNone(old.execute("SELECT 1 FROM sqlite_master WHERE name='slice_meta'").fetchone())
            old.close()
            self.assertTrue(database_is_slice(path))
            writer.close()

    def test_static_file_without_wal_uses_immutable(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'static.sqlite3'
            c = sqlite3.connect(path)
            c.execute('CREATE TABLE a(x)')
            c.commit()
            c.close()
            self.assertIn('immutable=1', _immutable_uri(path))
            self.assertFalse(database_is_slice(path))


if __name__ == '__main__':
    unittest.main()
