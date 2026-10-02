"""Existing workbook bytes only. These tests must not open or create a database."""

import hashlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/python'))
sys.path.insert(0, str(ROOT))

from ikarchive.xlsx_export import (SHEETS, LEGACY_ANALYSIS_XLSX_RETIRED,
                                   PublishedXlsxRetired, read_published_xlsx)

PAYLOAD = b'PK\x03\x04artificial-workbook'


def side_files(root):
    names = []
    for path in root.rglob('*'):
        if path.name.endswith(('-wal', '-shm', '-journal')) or path.suffix in {'.wal', '.shm'}:
            names.append(path.name)
    return names


class TestPublishedXlsx(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / 'database' / 'archive.sqlite3'
        self.db.parent.mkdir()
        self.book = self.root / 'exports' / '分析.xlsx'

    def tearDown(self):
        self.temp.cleanup()

    def write_book(self, payload=PAYLOAD):
        self.book.parent.mkdir(exist_ok=True)
        self.book.write_bytes(payload)

    def file_state(self, path):
        info = path.stat()
        return (hashlib.sha256(path.read_bytes()).hexdigest(),
                info.st_mode & 0o777, info.st_mtime_ns, info.st_size)

    def test_sheet_names_stay_on_the_existing_list(self):
        genres = [view.removeprefix('analysis_') for _, view, _ in SHEETS]
        self.assertEqual(genres, [
            'fest', 'nawabari', 'bankara_challenge', 'bankara_open', 'xmatch', 'event',
            'private_four_vs_four', 'private_three_vs_three', 'private_two_vs_two',
            'private_one_vs_one', 'private_other', 'salmon_regular', 'big_run',
            'team_contest', 'hold',
        ])

    def test_reader_retires_without_resolving_or_opening_any_path(self):
        self.write_book()
        self.db.write_bytes(b'not a database; bytes must remain untouched')
        self.book.chmod(0o640)
        self.db.chmod(0o640)
        before_book, before_db = self.file_state(self.book), self.file_state(self.db)
        with patch('ikarchive.xlsx_export.published_xlsx_path', side_effect=AssertionError('path resolved')), \
             patch('sqlite3.connect', side_effect=AssertionError('sqlite opened')):
            with self.assertRaises(PublishedXlsxRetired) as caught:
                read_published_xlsx(self.db)
        self.assertEqual(caught.exception.category, LEGACY_ANALYSIS_XLSX_RETIRED)
        self.assertEqual(self.file_state(self.book), before_book)
        self.assertEqual(self.file_state(self.db), before_db)
        self.assertEqual(side_files(self.root), [])

    def test_published_cli_returns_retired_category_and_preserves_inputs(self):
        self.write_book()
        self.db.write_bytes(b'database bytes must remain untouched')
        self.book.chmod(0o640)
        self.db.chmod(0o640)
        before_book, before_db = self.file_state(self.book), self.file_state(self.db)
        result = subprocess.run(
            [sys.executable, str(ROOT / 'archive.py'), '--db', str(self.db), 'published-xlsx'],
            capture_output=True,
        )
        self.assertEqual(result.returncode, 4, result.stderr)
        self.assertEqual(result.stdout, b'')
        self.assertEqual(result.stderr, (LEGACY_ANALYSIS_XLSX_RETIRED+'\n').encode())
        self.assertEqual(self.file_state(self.book), before_book)
        self.assertEqual(self.file_state(self.db), before_db)
        self.assertEqual(side_files(self.root), [])

    def test_export_cli_rejects_before_opening_store_or_creating_destination(self):
        self.write_book()
        self.db.write_bytes(b'database bytes must remain untouched')
        self.book.chmod(0o640)
        self.db.chmod(0o640)
        before_book, before_db = self.file_state(self.book), self.file_state(self.db)
        destination = self.root / 'exports' / 'new.xlsx'
        result = subprocess.run(
            [sys.executable, str(ROOT / 'archive.py'), '--db', str(self.db),
             'export-xlsx', str(destination)],
            capture_output=True,
        )
        self.assertEqual(result.returncode, 4, result.stderr)
        self.assertEqual(result.stdout, b'')
        self.assertIn(LEGACY_ANALYSIS_XLSX_RETIRED.encode(), result.stderr)
        self.assertIn(b'scripts/export_full_xlsx.py', result.stderr)
        self.assertFalse(destination.exists())
        self.assertEqual(self.file_state(self.book), before_book)
        self.assertEqual(self.file_state(self.db), before_db)
        self.assertEqual(side_files(self.root), [])


if __name__ == '__main__':
    unittest.main()
