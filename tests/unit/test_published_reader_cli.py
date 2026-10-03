from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests/unit"))

from ikarchive.slice_selectors import rule_token  # noqa: E402
import test_published_sqlite_reader as published_reader_fixtures  # noqa: E402
import scripts.published_sqlite_reader as published_reader_module  # noqa: E402
from ikarchive import store as store_module  # noqa: E402

BASELINE_ID = published_reader_fixtures.BASELINE_ID


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _decode_cell(cell: dict) -> object:
    sqlite_type = cell["sqlite_type"]
    encoding = cell["encoding"]
    value = cell["value"]
    if sqlite_type == "null":
        if encoding != "none" or value is not None:
            raise AssertionError(f"invalid NULL representation: {cell!r}")
        return None
    if sqlite_type == "integer":
        if encoding != "decimal" or not isinstance(value, str):
            raise AssertionError(f"invalid INTEGER representation: {cell!r}")
        return int(value, 10)
    if sqlite_type == "real":
        if encoding != "float.hex" or not isinstance(value, str):
            raise AssertionError(f"invalid REAL representation: {cell!r}")
        return float.fromhex(value)
    if sqlite_type == "text":
        if encoding != "unicode" or not isinstance(value, str):
            raise AssertionError(f"invalid TEXT representation: {cell!r}")
        return value
    if sqlite_type == "blob":
        if encoding != "hex" or not isinstance(value, str):
            raise AssertionError(f"invalid BLOB representation: {cell!r}")
        return bytes.fromhex(value)
    raise AssertionError(f"unknown SQLite type: {sqlite_type!r}")


class PublishedReaderCliTests(unittest.TestCase):
    def setUp(self):
        self.fixture = published_reader_fixtures.PublishedSQLiteReaderTests("test_baseline_only_pins_latest_and_exposes_full_baseline_metadata")
        self.fixture.setUp()
        self.case = self.fixture.case
        self.control = self.fixture.control
        self.delta_root = self.fixture.delta_root
        self.baseline_package = self.fixture.baseline_package

    def tearDown(self):
        self.fixture.tearDown()

    def _run(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env["IKARING_ARCHIVE_DATA_DIR"] = str(self.case.root / "unused-data-root")
        unused_store = self.case.root / "unused-store.sqlite3"
        command = [
            sys.executable,
            str(ROOT / "archive.py"),
            "--db",
            str(unused_store),
            *arguments,
        ]
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertFalse(
            unused_store.exists(),
            "Store database must never be created or opened by published CLI commands",
        )
        return result

    def _read_jsonl(self, result: subprocess.CompletedProcess[str]) -> tuple[dict, list[dict], dict]:
        self.assertEqual(result.returncode, 0, f"CLI exited {result.returncode} with stderr: {result.stderr}")
        self.assertEqual(result.stderr, "")
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        self.assertGreaterEqual(len(lines), 2, "JSONL stream must contain at least header and footer")
        rows = [json.loads(line) for line in lines]
        self.assertEqual(rows[0]["type"], "header")
        self.assertEqual(rows[-1]["type"], "footer")
        return rows[0], rows[1:-1], rows[-1]

    def test_baseline_published_list_and_read_unfiltered_and_selector(self):
        self.fixture._copy_publisher_controls()
        latest_raw = (self.control / "latest.json").read_bytes()
        latest_sha = _sha(latest_raw)

        # 1. published-list
        list_result = self._run(
            "published-list",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--generation", BASELINE_ID,
        )
        self.assertEqual(list_result.returncode, 0, list_result.stderr)
        self.assertEqual(list_result.stderr, "")
        listing = json.loads(list_result.stdout)
        self.assertEqual(listing["role"], "published_lossless_sqlite_reader")
        self.assertEqual(listing["reader_role"], "published_lossless_sqlite_reader")
        self.assertEqual(listing["baseline_generation"], BASELINE_ID)
        self.assertEqual(listing["latest_generation"], BASELINE_ID)
        self.assertEqual(listing["generation"], BASELINE_ID)
        self.assertEqual(listing["pinned_latest_sha256"], latest_sha)
        self.assertIsNotNone(listing.get("captured_at"))
        self.assertTrue(listing["published_control_binding_verified"])
        self.assertFalse(listing["published_deltas_verified"])
        self.assertFalse(listing["all_remote_artifacts_verified"])
        self.assertFalse(listing["realtime_synchronized"])
        self.assertEqual(listing["scope"], "local control + local SQLite package")
        self.assertIn("records", {t["name"] for t in listing["tables"]})
        self.assertIn("matches", {t["name"] for t in listing["tables"]})
        self.assertGreaterEqual(len(listing["schema_objects"]), 4)

        # 2. published-read unfiltered (records table)
        read_result = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "records",
        )
        header, rows, footer = self._read_jsonl(read_result)
        self.assertEqual(header["role"], "published_lossless_sqlite_reader")
        self.assertEqual(header["baseline_generation"], BASELINE_ID)
        self.assertEqual(header["latest_generation"], BASELINE_ID)
        self.assertEqual(header["table"], "records")
        self.assertEqual(header["table_full_row_count"], 1)
        self.assertEqual(header["ordinal_kind"], "current_table_stream_ordinal")
        self.assertEqual(header["source_rowid_kind"], "nullable_original_source_rowid")
        self.assertEqual(header["source_rowid_projection"], "original_source")
        self.assertFalse(header["all_remote_artifacts_verified"])
        self.assertFalse(header["realtime_synchronized"])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ordinal"], 0)
        self.assertEqual(_decode_cell(rows[0]["source_rowid"]), -42)
        decoded_values = [_decode_cell(c) for c in rows[0]["values"]]
        self.assertEqual(decoded_values, [-42, "original", 1.5, b"\x00base\xff"])

        self.assertEqual(footer["returned_rows"], 1)
        self.assertFalse(footer["truncated"])
        self.assertIsNone(footer["limit"])
        self.assertEqual(footer["pinned_latest_sha256"], latest_sha)
        self.assertEqual(footer["generation"], BASELINE_ID)

        # 3. published-read with mode selector (matches table)
        sel_result = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "matches",
            "--mode", "xmatch",
        )
        sel_header, sel_rows, sel_footer = self._read_jsonl(sel_result)
        self.assertEqual(sel_header["ordinal_kind"], "selector_stream_ordinal")
        self.assertEqual(sel_header["source_rowid_projection"], "unavailable_for_derived_rows")
        self.assertEqual(sel_header["scope"]["kind"], "mode_selector")
        self.assertIn("unavailable", sel_header["scope"]["source_rowid"])
        self.assertIn("full current source remains available", sel_header["scope"]["source_value_closure"])

        self.assertEqual(len(sel_rows), 1)
        self.assertEqual(sel_rows[0]["ordinal"], 0)
        # Verify source_rowid is NOT forged in derived selector rows
        self.assertNotIn("source_rowid", sel_rows[0])
        decoded_match = [_decode_cell(c) for c in sel_rows[0]["values"]]
        self.assertEqual(decoded_match, ["acct", "regular", "match-1", b"match\x00bytes"])

        self.assertEqual(sel_footer["returned_rows"], 1)
        self.assertFalse(sel_footer["truncated"])

    def test_two_delta_typed_rows_limit_footer_and_rule_selector(self):
        first_id, second_id = self.fixture._publish_reset_and_incremental()
        latest_raw = (self.control / "latest.json").read_bytes()
        latest_sha = _sha(latest_raw)

        # 1. published-list on two-delta chain
        list_result = self._run(
            "published-list",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--generation", second_id,
        )
        self.assertEqual(list_result.returncode, 0, list_result.stderr)
        listing = json.loads(list_result.stdout)
        self.assertEqual(listing["baseline_generation"], BASELINE_ID)
        self.assertEqual(listing["latest_generation"], second_id)
        self.assertTrue(listing["published_control_binding_verified"])
        self.assertTrue(listing["published_deltas_verified"])
        self.assertFalse(listing["all_remote_artifacts_verified"])
        self.assertFalse(listing["realtime_synchronized"])

        # 2. published-read on updated records (typed values, negative rowid, NUL bytes)
        read_result = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "records",
        )
        header, rows, footer = self._read_jsonl(read_result)
        self.assertEqual(header["latest_generation"], second_id)
        self.assertEqual(header["pinned_latest_sha256"], latest_sha)
        self.assertEqual(len(rows), 1)
        self.assertEqual(_decode_cell(rows[0]["source_rowid"]), -42)
        decoded = [_decode_cell(c) for c in rows[0]["values"]]
        self.assertEqual(decoded, [-42, b"final\x00blob", 2.25, b"\x00second\xff"])
        self.assertEqual(footer["returned_rows"], 1)
        self.assertFalse(footer["truncated"])
        self.assertEqual(footer["pinned_latest_sha256"], latest_sha)
        self.assertEqual(footer["generation"], second_id)

        # 3. Rule selector matching TOWER
        matching_rule = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "matches",
            "--mode", "xmatch",
            "--rule", rule_token("TOWER"),
        )
        m_header, m_rows, m_footer = self._read_jsonl(matching_rule)
        self.assertEqual(m_header["scope"]["kind"], "mode_rule_selector")
        self.assertEqual(m_header["scope"]["rule_token"], rule_token("TOWER"))
        self.assertEqual(len(m_rows), 1)
        self.assertNotIn("source_rowid", m_rows[0])
        self.assertEqual([_decode_cell(c) for c in m_rows[0]["values"]],
                         ["acct", "regular", "match-1", b"selected\x00final"])
        self.assertEqual(m_footer["returned_rows"], 1)
        self.assertFalse(m_footer["truncated"])

        # 4. Rule selector non-matching AREA
        non_matching_rule = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "matches",
            "--mode", "xmatch",
            "--rule", rule_token("AREA"),
        )
        nm_header, nm_rows, nm_footer = self._read_jsonl(non_matching_rule)
        self.assertEqual(len(nm_rows), 0)
        self.assertEqual(nm_footer["returned_rows"], 0)
        self.assertFalse(nm_footer["truncated"])

        # 5. Limit and footer truncation
        limited = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "records",
            "--limit", "1",
        )
        lim_header, lim_rows, lim_footer = self._read_jsonl(limited)
        self.assertEqual(len(lim_rows), 1)
        self.assertEqual(lim_footer["returned_rows"], 1)
        self.assertFalse(lim_footer["truncated"])
        self.assertEqual(lim_footer["limit"], 1)
        self.assertEqual(lim_footer["pinned_latest_sha256"], latest_sha)
        self.assertEqual(lim_footer["generation"], second_id)

    def test_limit_truncates_stream_when_rows_exceed_limit(self):
        self.fixture._copy_publisher_controls()
        # matches table has 1 match; insert another row into baseline source for limit test
        # Let's add multiple rows to a temporary copy of baseline to test truncation
        conn = sqlite3.connect(self.case.current_path)
        try:
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("INSERT INTO records VALUES(100, 'extra-1', 3.0, x'01')")
            conn.execute("INSERT INTO records VALUES(101, 'extra-2', 4.0, x'02')")
            conn.commit()
        finally:
            conn.close()

        first_id = "20261003T010101Z-a0010001"
        first_dir, _first_plan = self.case.prepare(first_id)
        self.assertEqual(self.case.publish(first_dir)["status"], "complete")
        self.fixture._copy_publisher_controls((first_id,))
        local = self.delta_root / first_id
        shutil.copytree(first_dir, local)
        (local / "index.json").write_bytes(self.case.remote.objects[
            f"{published_reader_fixtures.REMOTE}/deltas/generations/{first_id}/index.json"
        ])

        limited = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "records",
            "--limit", "1",
        )
        header, rows, footer = self._read_jsonl(limited)
        self.assertEqual(len(rows), 1)
        self.assertEqual(footer["returned_rows"], 1)
        self.assertTrue(footer["truncated"], "Footer must report truncated=True when limit terminates stream early")
        self.assertEqual(footer["limit"], 1)

    def test_published_read_close_failure_prevents_success_footer_and_closes_reader(self):
        class ClosingIterator:
            def __init__(self, rows, error, next_error=None):
                self.rows = iter(rows)
                self.error = error
                self.next_error = next_error
                self.next_count = 0
                self.closed = False

            def __iter__(self):
                return self

            def __next__(self):
                if self.next_count == 1 and self.next_error is not None:
                    raise self.next_error
                self.next_count += 1
                return next(self.rows)

            def close(self):
                self.closed = True
                raise self.error

        scenarios = ((False, False), (True, False), (False, True))
        for selector, fail_while_reading in scenarios:
            with self.subTest(selector=selector, fail_while_reading=fail_while_reading), \
                 tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                unused_store = root / "unused-store.sqlite3"
                close_error = published_reader_module.PublishedSQLiteReaderError(
                    "PUBLISHED_CONTROL_CHANGED"
                )
                rows = ClosingIterator(
                    [("first",), ("second",)] if selector else
                    [(0, 101, ("first",)), (1, 102, ("second",))],
                    close_error,
                    next_error=ValueError("injected row failure") if fail_while_reading else None,
                )

                class FakeReader:
                    def __init__(self):
                        self.context_closed = False

                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, exc, traceback):
                        self.context_closed = True
                        return False

                    def tables(self):
                        return [
                            {"name": "records", "columns": ["value"]},
                            {"name": "matches", "columns": ["value"]},
                        ]

                    @property
                    def baseline_generation_id(self):
                        return "20261003T010101Z-a0010001"

                    @property
                    def generation_id(self):
                        return "20261003T010101Z-a0010001"

                    @property
                    def pinned_latest_sha256(self):
                        return "a" * 64

                    @property
                    def captured_at(self):
                        return "2026-10-03T01:01:01Z"

                    @property
                    def published_control_binding_verified(self):
                        return True

                    @property
                    def published_deltas_verified(self):
                        return False

                    def iter_rows(self, table):
                        return rows

                    def iter_selected_matches(self, mode, rule_token=None):
                        return rows

                    def row_count(self, table):
                        return 2

                    def columns(self, table):
                        return [{"cid": 0, "name": "value"}]

                    def foreign_keys(self, table):
                        return []

                fake_reader = FakeReader()
                argv = [
                    str(ROOT / "archive.py"), "--db", str(unused_store),
                    "published-read", "--controls", str(root / "controls"),
                    "--root", str(root / "baseline"),
                    "--delta-root", str(root / "deltas"),
                    "--table", "matches" if selector else "records",
                    "--limit", "1",
                ]
                if selector:
                    argv.extend(["--mode", "xmatch"])

                stdout = io.StringIO()
                stderr = io.StringIO()
                with patch.object(sys, "argv", argv), \
                     patch.object(sys, "stdout", stdout), \
                     patch.object(sys, "stderr", stderr), \
                     patch.dict(os.environ, {"IKARING_ARCHIVE_DATA_DIR": str(root / "data")} ), \
                     patch.object(published_reader_module, "PublishedSQLiteReader", return_value=fake_reader), \
                     patch.object(store_module, "Store", side_effect=AssertionError("Store must not open")) as store_ctor:
                    with self.assertRaises(SystemExit) as raised:
                        runpy.run_path(str(ROOT / "archive.py"), run_name="__main__")

                self.assertEqual(raised.exception.code, 1)
                self.assertEqual(json.loads(stderr.getvalue()), {"error": "PUBLISHED_CONTROL_CHANGED"})
                output_lines = [line for line in stdout.getvalue().splitlines() if line.strip()]
                self.assertEqual(len(output_lines), 2, "header and one data row are allowed; footer is not")
                self.assertEqual(json.loads(output_lines[0])["type"], "header")
                self.assertEqual(json.loads(output_lines[1])["ordinal"], 0)
                self.assertFalse(any(json.loads(line).get("type") == "footer" for line in output_lines))
                self.assertTrue(rows.closed)
                self.assertTrue(fake_reader.context_closed)
                store_ctor.assert_not_called()
                self.assertFalse(unused_store.exists())

    def test_published_cli_context_guard_failure_prevents_success_output(self):
        class TrackingIterator:
            def __init__(self):
                self.rows = iter([(0, 101, ("value",))])
                self.closed = False

            def __iter__(self):
                return self

            def __next__(self):
                return next(self.rows)

            def close(self):
                self.closed = True

        for command in ("published-list", "published-read"):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                unused_store = root / "unused-store.sqlite3"
                exit_error = published_reader_module.PublishedSQLiteReaderError(
                    "PUBLISHED_CONTROL_CHANGED"
                )
                rows = TrackingIterator()

                class FakeReader:
                    def __init__(self):
                        self.context_closed = False

                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, exc, traceback):
                        self.context_closed = True
                        raise exit_error

                    def tables(self):
                        return [{"name": "records", "columns": ["value"]}]

                    def schema_objects(self):
                        return [{"type": "table", "name": "records"}]

                    @property
                    def baseline_generation_id(self):
                        return "20261003T010101Z-a0010001"

                    @property
                    def generation_id(self):
                        return "20261003T010101Z-a0010001"

                    @property
                    def pinned_latest_sha256(self):
                        return "b" * 64

                    @property
                    def captured_at(self):
                        return "2026-10-03T01:01:01Z"

                    @property
                    def published_control_binding_verified(self):
                        return True

                    @property
                    def published_deltas_verified(self):
                        return False

                    def iter_rows(self, table):
                        return rows

                    def row_count(self, table):
                        return 1

                    def columns(self, table):
                        return [{"cid": 0, "name": "value"}]

                    def foreign_keys(self, table):
                        return []

                fake_reader = FakeReader()
                argv = [
                    str(ROOT / "archive.py"), "--db", str(unused_store), command,
                    "--controls", str(root / "controls"), "--root", str(root / "baseline"),
                    "--delta-root", str(root / "deltas"),
                ]
                if command == "published-read":
                    argv.extend(["--table", "records"])

                stdout = io.StringIO()
                stderr = io.StringIO()
                with patch.object(sys, "argv", argv), \
                     patch.object(sys, "stdout", stdout), \
                     patch.object(sys, "stderr", stderr), \
                     patch.dict(os.environ, {"IKARING_ARCHIVE_DATA_DIR": str(root / "data")} ), \
                     patch.object(published_reader_module, "PublishedSQLiteReader", return_value=fake_reader), \
                     patch.object(store_module, "Store", side_effect=AssertionError("Store must not open")) as store_ctor:
                    with self.assertRaises(SystemExit) as raised:
                        runpy.run_path(str(ROOT / "archive.py"), run_name="__main__")

                self.assertEqual(raised.exception.code, 1)
                self.assertEqual(json.loads(stderr.getvalue()), {"error": "PUBLISHED_CONTROL_CHANGED"})
                output_lines = [line for line in stdout.getvalue().splitlines() if line.strip()]
                if command == "published-list":
                    self.assertEqual(output_lines, [], "list JSON must wait for the context guard")
                else:
                    self.assertEqual(len(output_lines), 2, "header and row may stream; success footer must wait")
                    self.assertEqual(json.loads(output_lines[0])["type"], "header")
                    self.assertEqual(json.loads(output_lines[1])["ordinal"], 0)
                    self.assertFalse(any(json.loads(line).get("type") == "footer" for line in output_lines))
                    self.assertTrue(rows.closed)
                self.assertTrue(fake_reader.context_closed)
                store_ctor.assert_not_called()
                self.assertFalse(unused_store.exists())

    def test_expected_generation_mismatch_fails_with_stable_category(self):
        self.fixture._copy_publisher_controls()
        mismatch_id = "20261003T090909Z-a9999999"

        # published-list mismatch
        list_result = self._run(
            "published-list",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--generation", mismatch_id,
        )
        self.assertNotEqual(list_result.returncode, 0)
        self.assertEqual(list_result.stdout, "")
        err = json.loads(list_result.stderr)
        self.assertEqual(err, {"error": "PUBLISHED_EXPECTED_GENERATION_MISMATCH"})

        # published-read mismatch
        read_result = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "records",
            "--generation", mismatch_id,
        )
        self.assertNotEqual(read_result.returncode, 0)
        self.assertEqual(read_result.stdout, "")
        err = json.loads(read_result.stderr)
        self.assertEqual(err, {"error": "PUBLISHED_EXPECTED_GENERATION_MISMATCH"})

    def test_prepared_index_missing_fails_with_stable_category(self):
        first_id, second_id = self.fixture._publish_reset_and_incremental()
        (self.delta_root / first_id / "index.json").unlink()

        list_result = self._run(
            "published-list",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--generation", second_id,
        )
        self.assertNotEqual(list_result.returncode, 0)
        self.assertEqual(list_result.stdout, "")
        err = json.loads(list_result.stderr)
        self.assertIn("error", err)
        self.assertIn(err["error"], {"PUBLISHED_CONTROL_FILE_INVALID", "PUBLISHED_VALIDATION_FAILED", "PUBLISHED_CONTROL_MISSING"})

    def test_invalid_params_rejected_before_package_open(self):
        dummy_controls = self.case.root / "does-not-exist-controls"
        dummy_root = self.case.root / "does-not-exist-root"
        dummy_delta = self.case.root / "does-not-exist-delta"

        # 1. rule without mode
        res1 = self._run(
            "published-read",
            "--controls", str(dummy_controls),
            "--root", str(dummy_root),
            "--delta-root", str(dummy_delta),
            "--table", "matches",
            "--rule", "TOWER",
        )
        self.assertNotEqual(res1.returncode, 0)
        self.assertEqual(res1.stdout, "")
        self.assertEqual(json.loads(res1.stderr), {"error": "PUBLISHED_RULE_REQUIRES_MODE"})

        # 2. limit 0
        res2 = self._run(
            "published-read",
            "--controls", str(dummy_controls),
            "--root", str(dummy_root),
            "--delta-root", str(dummy_delta),
            "--table", "matches",
            "--limit", "0",
        )
        self.assertNotEqual(res2.returncode, 0)
        self.assertEqual(res2.stdout, "")
        self.assertEqual(json.loads(res2.stderr), {"error": "PUBLISHED_LIMIT_MUST_BE_POSITIVE"})

        # 3. limit negative
        res3 = self._run(
            "published-read",
            "--controls", str(dummy_controls),
            "--root", str(dummy_root),
            "--delta-root", str(dummy_delta),
            "--table", "matches",
            "--limit", "-3",
        )
        self.assertNotEqual(res3.returncode, 0)
        self.assertEqual(res3.stdout, "")
        self.assertEqual(json.loads(res3.stderr), {"error": "PUBLISHED_LIMIT_MUST_BE_POSITIVE"})

        # 4. selector on non-matches table
        res4 = self._run(
            "published-read",
            "--controls", str(dummy_controls),
            "--root", str(dummy_root),
            "--delta-root", str(dummy_delta),
            "--table", "records",
            "--mode", "xmatch",
        )
        self.assertNotEqual(res4.returncode, 0)
        self.assertEqual(res4.stdout, "")
        self.assertEqual(json.loads(res4.stderr), {"error": "PUBLISHED_SELECTOR_REQUIRES_MATCHES"})

        # 5. invalid generation format
        res5 = self._run(
            "published-read",
            "--controls", str(dummy_controls),
            "--root", str(dummy_root),
            "--delta-root", str(dummy_delta),
            "--table", "matches",
            "--generation", "invalid/traversal..id",
        )
        self.assertNotEqual(res5.returncode, 0)
        self.assertEqual(res5.stdout, "")
        self.assertEqual(json.loads(res5.stderr), {"error": "PUBLISHED_GENERATION_INVALID"})

        # 6. missing table on valid package
        self.fixture._copy_publisher_controls()
        res6 = self._run(
            "published-read",
            "--controls", str(self.control),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
            "--table", "non_existent_table",
        )
        self.assertNotEqual(res6.returncode, 0)
        self.assertEqual(res6.stdout, "")
        self.assertEqual(json.loads(res6.stderr), {"error": "PUBLISHED_TABLE_NOT_FOUND"})

    def test_control_symlink_fails_with_stable_category(self):
        self.fixture._copy_publisher_controls()
        link = self.case.root / "control-link"
        link.symlink_to(self.control, target_is_directory=True)

        res = self._run(
            "published-list",
            "--controls", str(link),
            "--root", str(self.baseline_package),
            "--delta-root", str(self.delta_root),
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertEqual(res.stdout, "")
        self.assertEqual(json.loads(res.stderr), {"error": "PUBLISHED_PATH_SYMLINK"})

    def test_old_shard_cli_compatibility(self):
        # Verify shard-list and shard-read commands remain functional
        list_res = self._run("shard-list", "--root", str(self.baseline_package), "--generation", BASELINE_ID)
        self.assertEqual(list_res.returncode, 0, list_res.stderr)
        listing = json.loads(list_res.stdout)
        self.assertEqual(listing["reader_role"], "baseline_only_lossless_shard_reader")
        self.assertTrue(listing["baseline_only"])
        self.assertEqual(listing["generation"], BASELINE_ID)

        read_res = self._run("shard-read", "--root", str(self.baseline_package), "--table", "records")
        header, rows, footer = self._read_jsonl(read_res)
        self.assertEqual(header["generation"], BASELINE_ID)
        self.assertEqual(header["table"], "records")
        self.assertEqual(len(rows), 1)
        self.assertEqual(footer["returned_rows"], 1)


if __name__ == "__main__":
    unittest.main()
