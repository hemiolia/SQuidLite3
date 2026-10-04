from __future__ import annotations

import base64
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests/unit"))

import test_published_sqlite_reader as reader_fixtures  # noqa: E402
import test_full_data_delta_publish as delta_fixtures  # noqa: E402
import published_reader_session as session_module  # noqa: E402
from published_reader_session import (  # noqa: E402
    MAX_REQUEST_LINE_BYTES,
    RESPONSE_CHUNK_BYTES,
    PublishedSQLiteReaderError,
    serve_session,
)
from published_sqlite_reader import PublishedSQLiteReader  # noqa: E402


BASELINE_ID = reader_fixtures.BASELINE_ID


def _cell(sqlite_type: str, encoding: str, value) -> dict:
    return {"sqlite_type": sqlite_type, "encoding": encoding, "value": value}


def _request(request_id: str, op: str, **fields) -> bytes:
    value = {"version": 1, "request_id": request_id, "op": op, **fields}
    return (json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n").encode("ascii")


def _read_frames(raw: bytes) -> list[dict]:
    return [json.loads(line) for line in raw.splitlines()]


def _response_payload(frames: list[dict], request_id: str) -> tuple[dict, list[dict], dict]:
    start = next(i for i, frame in enumerate(frames)
                 if frame.get("type") == "response_begin" and frame.get("request_id") == request_id)
    begin = frames[start]
    chunks = []
    index = start + 1
    while index < len(frames) and frames[index].get("type") == "response_chunk":
        chunks.append(frames[index])
        index += 1
    end = frames[index]
    assert end["type"] == "response_end"
    raw_payload = b"".join(base64.b64decode(frame["data_base64"], validate=True) for frame in chunks)
    assert len(raw_payload) == begin["response_bytes"] == end["response_bytes"]
    assert hashlib.sha256(raw_payload).hexdigest() == begin["response_sha256"] == end["response_sha256"]
    assert begin["generation_id"] == end["generation_id"]
    assert begin["pinned_latest_sha256"] == end["pinned_latest_sha256"]
    return json.loads(raw_payload), chunks, end


class _FakeReader:
    def __init__(self, *, values=None, rowid=7, fail_guard_at=None, exit_error=None,
                 lookup_error=None):
        self.values = tuple(values if values is not None else (1, "text", 1.5, b"blob", None))
        self.rowid = rowid
        self.fail_guard_at = fail_guard_at
        self.exit_error = exit_error
        self.lookup_error = lookup_error
        self.guard_calls = 0
        self.enter_count = 0
        self.exit_count = 0
        self.lookup_count = 0
        self.criteria_seen = None

    def __enter__(self):
        self.enter_count += 1
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.exit_count += 1
        if self.exit_error is not None:
            raise self.exit_error
        return False

    @property
    def generation_id(self):
        return BASELINE_ID

    @property
    def baseline_generation_id(self):
        return self.generation_id

    @property
    def pinned_latest_sha256(self):
        return "a" * 64

    @property
    def captured_at(self):
        return "2026-10-03T01:01:01Z"

    @property
    def published_control_binding_verified(self):
        self.guard_calls += 1
        if self.fail_guard_at == self.guard_calls:
            raise PublishedSQLiteReaderError("PUBLISHED_CONTROL_CHANGED")
        return True

    @property
    def published_deltas_verified(self):
        return False

    def tables(self):
        return [{
            "name": "records",
            "columns": ["id", "nullable", "ratio", "payload", "future_列"],
            "column_schema": [],
            "foreign_keys": [],
            "row_count": 1,
            "schema": {"name": "records", "type": "table", "sql": "CREATE TABLE records"},
            "future_reader_metadata": {"kept": True},
        }]

    def columns(self, table):
        if table != "records":
            raise PublishedSQLiteReaderError("DELTA_LOOKUP_TABLE_UNKNOWN")
        return [
            {"cid": index, "name": name, "type": "", "notnull": 0,
             "dflt_value": None, "pk": 0, "hidden": 0}
            for index, name in enumerate(("id", "nullable", "ratio", "payload", "future_列"))
        ]

    def foreign_keys(self, table):
        return [{"id": 0, "seq": 0, "table": "parent", "from": "id", "to": "id"}]

    def schema_objects(self):
        return [{"name": "records", "type": "table", "tbl_name": "records",
                 "sql": "CREATE TABLE records"}]

    def get_unique_row(self, table, criteria):
        self.lookup_count += 1
        self.criteria_seen = criteria
        if self.lookup_error is not None:
            raise self.lookup_error
        if table != "records":
            raise PublishedSQLiteReaderError("DELTA_LOOKUP_TABLE_UNKNOWN")
        return {"source_rowid": self.rowid, "values": self.values}


class PublishedReaderSessionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = reader_fixtures.PublishedSQLiteReaderTests(
            "test_baseline_only_pins_latest_and_exposes_full_baseline_metadata"
        )
        self.fixture.setUp()
        self.fixture._copy_publisher_controls()

    def tearDown(self):
        self.fixture.tearDown()

    def _serve(self, factory, raw_requests: bytes) -> tuple[int, bytes]:
        output = io.BytesIO()
        code = serve_session(
            factory,
            controls=self.fixture.control,
            root=self.fixture.baseline_package,
            delta_root=self.fixture.delta_root,
            expected_generation_id=BASELINE_ID,
            input_stream=io.BytesIO(raw_requests),
            output_stream=output,
        )
        return code, output.getvalue()

    def _replace_baseline_with_large_blob(self, blob: bytes) -> None:
        case = self.fixture.case
        connection = sqlite3.connect(case.baseline_path)
        try:
            connection.execute("UPDATE records SET blob_value=? WHERE id=-42", (blob,))
            connection.commit()
        finally:
            connection.close()
        raw_source = case.baseline_path.read_bytes()
        case.baseline_sha = hashlib.sha256(raw_source).hexdigest()
        manifest = json.loads(case.manifest_path.read_text(encoding="utf-8"))
        manifest["raw_snapshot"]["bytes"] = len(raw_source)
        manifest["raw_snapshot"]["sha256"] = case.baseline_sha
        case.manifest_path.write_bytes(delta_fixtures._compact(manifest))
        case._install_baseline_remote()

        shutil.rmtree(self.fixture.baseline_package)
        delta_fixtures._make_lossless_baseline_package(
            case.baseline_path, self.fixture.baseline_package, BASELINE_ID,
        )
        self.fixture._sync_baseline_remote()
        self.fixture._copy_publisher_controls()

    def test_real_reader_opens_once_and_returns_metadata_and_all_native_values(self):
        calls = []

        def factory(*args, **kwargs):
            calls.append((args, kwargs))
            return PublishedSQLiteReader(*args, **kwargs)

        requests = (
            _request("meta-1", "metadata")
            + _request("row-1", "get_unique_row", table="records", criteria={
                "id": _cell("integer", "decimal", "-42"),
            })
            + _request("missing-1", "get_unique_row", table="records", criteria={
                "id": _cell("integer", "decimal", "-999"),
            })
            + _request("close-1", "close")
        )
        code, raw = self._serve(factory, requests)
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1]["expected_generation_id"], BASELINE_ID)
        frames = _read_frames(raw)
        self.assertEqual(frames[0]["type"], "ready")
        self.assertEqual(frames[0]["generation_id"], BASELINE_ID)
        self.assertRegex(frames[0]["pinned_latest_sha256"], r"^[0-9a-f]{64}$")
        self.assertFalse(frames[0]["all_remote_artifacts_verified"])
        self.assertFalse(frames[0]["realtime_synchronized"])

        metadata, _chunks, _end = _response_payload(frames, "meta-1")
        self.assertEqual(metadata["kind"], "metadata")
        self.assertFalse(metadata["all_remote_artifacts_verified"])
        self.assertFalse(metadata["realtime_synchronized"])
        records = next(table for table in metadata["tables"] if table["name"] == "records")
        self.assertEqual(records["columns"], ["id", "payload", "ratio", "blob_value"])
        self.assertEqual([c["name"] for c in records["column_schema"]], records["columns"])
        self.assertIn("foreign_keys", records)
        self.assertTrue(any(obj["name"] == "records" and obj["sql"] for obj in metadata["schema_objects"]))

        row, _chunks, _end = _response_payload(frames, "row-1")
        self.assertTrue(row["found"])
        self.assertEqual(row["columns"], ["id", "payload", "ratio", "blob_value"])
        self.assertEqual(row["source_rowid"], _cell("integer", "decimal", "-42"))
        self.assertEqual(row["values"], [
            _cell("integer", "decimal", "-42"),
            _cell("text", "unicode", "original"),
            _cell("real", "float.hex", (1.5).hex()),
            _cell("blob", "base64", base64.b64encode(b"\x00base\xff").decode("ascii")),
        ])
        missing, _chunks, _end = _response_payload(frames, "missing-1")
        self.assertEqual(missing["columns"], records["columns"])
        self.assertFalse(missing["found"])
        self.assertIsNone(missing["source_rowid"])
        self.assertIsNone(missing["values"])
        self.assertEqual(frames[-1]["type"], "closed")
        self.assertEqual(frames[-1]["request_id"], "close-1")

    def test_real_reader_unknown_table_continues_and_without_rowid_is_null(self):
        output = io.BytesIO()
        code = serve_session(
            PublishedSQLiteReader,
            controls=self.fixture.control,
            root=self.fixture.baseline_package,
            delta_root=self.fixture.delta_root,
            expected_generation_id=BASELINE_ID,
            input_stream=io.BytesIO(
                _request("unknown-table", "get_unique_row", table="absent", criteria={})
                + _request("without-rowid", "get_unique_row", table="lookup_without_rowid",
                           criteria={"code": _cell("text", "unicode", "wr-key")})
                + _request("still-live", "get_unique_row", table="records", criteria={
                    "id": _cell("integer", "decimal", "-42"),
                })
                + _request("close", "close")
            ),
            output_stream=output,
        )
        self.assertEqual(code, 0)
        frames = _read_frames(output.getvalue())
        self.assertEqual(frames[1]["type"], "error")
        self.assertEqual(frames[1]["category"], "DELTA_LOOKUP_TABLE_UNKNOWN")
        self.assertEqual(frames[1]["request_id"], "unknown-table")
        without_rowid, _chunks, _end = _response_payload(frames, "without-rowid")
        self.assertTrue(without_rowid["found"])
        self.assertIsNone(without_rowid["source_rowid"])
        self.assertEqual(without_rowid["values"], [
            _cell("text", "unicode", "wr-key"),
            _cell("blob", "base64", base64.b64encode(b"without-rowid").decode("ascii")),
        ])
        row, _chunks, _end = _response_payload(frames, "still-live")
        self.assertEqual(row["source_rowid"], _cell("integer", "decimal", "-42"))
        self.assertEqual(frames[-1]["type"], "closed")

    def test_large_typed_response_is_spooled_and_split_without_truncation(self):
        blob = (b"\x00\xfflarge-body" * 18000) + b"tail"
        values = (_INT64_MIN, "NUL\x00雪", float("inf"), blob, None)
        fake = _FakeReader(values=values, rowid=_INT64_MAX)

        def factory(*_args, **_kwargs):
            return fake

        request = _request("large", "get_unique_row", table="records", criteria={
            "id": _cell("integer", "decimal", str(_INT64_MIN)),
            "nullable": _cell("null", "none", None),
            "ratio": _cell("real", "float.hex", float("inf").hex()),
            "payload": _cell("blob", "base64", base64.b64encode(b"\x00\xff").decode("ascii")),
            "future_列": _cell("text", "unicode", "criterion\x00雪"),
        })
        request = _request("fake-meta", "metadata") + request + _request("close", "close")
        code, raw = self._serve(factory, request)
        self.assertEqual(code, 0)
        frames = _read_frames(raw)
        fake_metadata, _chunks, _end = _response_payload(frames, "fake-meta")
        future_table = next(table for table in fake_metadata["tables"] if table["name"] == "records")
        self.assertEqual(future_table["future_reader_metadata"], {"kept": True})
        payload, chunks, end = _response_payload(frames, "large")
        self.assertGreater(len(chunks), 1)
        self.assertEqual(end["response_bytes"], payload_bytes := sum(
            len(base64.b64decode(item["data_base64"], validate=True)) for item in chunks
        ))
        self.assertGreater(payload_bytes, RESPONSE_CHUNK_BYTES)
        self.assertTrue(all(len(base64.b64decode(item["data_base64"])) <= RESPONSE_CHUNK_BYTES
                            for item in chunks))
        self.assertEqual(payload["source_rowid"], _cell("integer", "decimal", str(_INT64_MAX)))
        self.assertEqual(payload["values"], [
            _cell("integer", "decimal", str(_INT64_MIN)),
            _cell("text", "unicode", "NUL\x00雪"),
            _cell("real", "float.hex", float("inf").hex()),
            _cell("blob", "base64", base64.b64encode(blob).decode("ascii")),
            _cell("null", "none", None),
        ])
        self.assertEqual(payload["columns"][-1], "future_列")
        self.assertEqual(fake.criteria_seen, {
            "id": _INT64_MIN,
            "nullable": None,
            "ratio": float("inf"),
            "payload": b"\x00\xff",
            "future_列": "criterion\x00雪",
        })
        self.assertIs(type(fake.criteria_seen["id"]), int)
        self.assertIs(type(fake.criteria_seen["ratio"]), float)
        self.assertIs(type(fake.criteria_seen["payload"]), bytes)
        self.assertEqual(fake.enter_count, 1)
        self.assertEqual(fake.exit_count, 1)

    def test_strict_request_validation_continues_only_with_validation_errors(self):
        fake = _FakeReader()

        def factory(*_args, **_kwargs):
            return fake

        malformed = b'{"version":1,"request_id":"dup","op":"metadata","op":"close"}\n'
        wrong_version = _request("bool-version", "metadata").replace(b'"version":1', b'"version":true')
        extra = _request("extra", "metadata").replace(b'"op":"metadata"', b'"op":"metadata","path":"/private"')
        bad_int = _request("bad-int", "get_unique_row", table="records", criteria={
            "id": _cell("integer", "decimal", "01"),
        })
        bad_real = _request("bad-real", "get_unique_row", table="records", criteria={
            "ratio": _cell("real", "float.hex", "0x1p+0"),
        })
        bad_blob = _request("bad-blob", "get_unique_row", table="records", criteria={
            "payload": _cell("blob", "base64", "AA=A"),
        })
        bad_bool_cell = _request("bad-bool-cell", "get_unique_row", table="records", criteria={
            "id": _cell("integer", "decimal", True),
        })
        nonstandard_json_constant = (
            b'{"version":1,"request_id":"nan-json","op":"get_unique_row",'
            b'"table":"records","criteria":{"ratio":{"sqlite_type":"real",'
            b'"encoding":"float.hex","value":NaN}}}\n'
        )
        overflow_json_number = (
            b'{"version":1,"request_id":"overflow-json","op":"metadata",'
            b'"unexpected":1e9999}\n'
        )
        code, raw = self._serve(
            factory,
            malformed + wrong_version + extra + bad_int + bad_real + bad_blob + bad_bool_cell
            + nonstandard_json_constant + overflow_json_number
            + _request("close", "close"),
        )
        self.assertEqual(code, 0)
        frames = _read_frames(raw)
        errors = [frame["category"] for frame in frames if frame["type"] == "error"]
        self.assertEqual(errors, [
            "SESSION_REQUEST_JSON_INVALID",
            "SESSION_PROTOCOL_VERSION_INVALID",
            "SESSION_REQUEST_FIELDS_INVALID",
            "SESSION_CRITERIA_CELL_INVALID",
            "SESSION_CRITERIA_CELL_INVALID",
            "SESSION_CRITERIA_CELL_INVALID",
            "SESSION_CRITERIA_CELL_INVALID",
            "SESSION_REQUEST_JSON_INVALID",
            "SESSION_REQUEST_JSON_INVALID",
        ])
        error_frames = [frame for frame in frames if frame["type"] == "error"]
        self.assertEqual([frame["request_id"] for frame in error_frames], [
            None,
            "bool-version",
            "extra",
            "bad-int",
            "bad-real",
            "bad-blob",
            "bad-bool-cell",
            None,
            None,
        ])
        self.assertEqual(fake.lookup_count, 0)
        self.assertEqual(frames[-1]["type"], "closed")
        self.assertTrue(all("/private" not in line for line in raw.decode("ascii").splitlines()))

    def test_deep_json_recursion_error_is_recoverable_and_reader_opens_once(self):
        fake = _FakeReader()
        calls = []

        def factory(*args, **kwargs):
            calls.append((args, kwargs))
            return fake

        deeply_nested = b"[" * 1400 + b"0" + b"]" * 1400 + b"\n"
        self.assertLessEqual(len(deeply_nested), MAX_REQUEST_LINE_BYTES)
        requests = (
            deeply_nested + _request("after-depth-error", "metadata")
            + _request("close-after-depth-error", "close")
        )
        actual_json_loads = json.loads

        def decoder_with_deep_recursion_error(value, *args, **kwargs):
            if type(value) is str and value.startswith("[" * 100):
                raise RecursionError("synthetic parser recursion boundary")
            return actual_json_loads(value, *args, **kwargs)

        # Some supported Python JSON decoders reject this depth themselves;
        # newer decoders are iterative, so inject the same parser failure.
        with patch.object(session_module.json, "loads", decoder_with_deep_recursion_error):
            code, raw = self._serve(factory, requests)
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 1)
        frames = _read_frames(raw)
        self.assertEqual(frames[1], {
            "type": "error", "version": 1, "request_id": None,
            "category": "SESSION_REQUEST_JSON_INVALID",
        })
        metadata, _chunks, _end = _response_payload(frames, "after-depth-error")
        self.assertEqual(metadata["kind"], "metadata")
        self.assertEqual(frames[-1]["type"], "closed")
        self.assertEqual(frames[-1]["request_id"], "close-after-depth-error")
        self.assertEqual(fake.enter_count, 1)
        self.assertEqual(fake.exit_count, 1)

    def test_cli_requires_expected_generation_before_reader_creation(self):
        output_buffer = io.BytesIO()

        class _Output:
            buffer = output_buffer

        previous_umask = os.umask(0o077)
        try:
            with patch.object(session_module.sys, "stdout", _Output()), \
                    patch.object(session_module.sys, "stderr", io.StringIO()), \
                    patch.object(session_module, "PublishedSQLiteReader") as reader_factory:
                with self.assertRaises(SystemExit) as raised:
                    session_module.main([
                        "--controls", "/private/controls",
                        "--root", "/private/root",
                        "--delta-root", "/private/deltas",
                    ])
            self.assertEqual(raised.exception.code, 2)
            reader_factory.assert_not_called()
        finally:
            os.umask(previous_umask)

        frames = _read_frames(output_buffer.getvalue())
        self.assertEqual(frames, [{
            "type": "error", "version": 1, "request_id": None,
            "category": "SESSION_ARGUMENTS_INVALID",
        }])
        self.assertNotIn(b"/private", output_buffer.getvalue())

    def test_oversized_line_is_fatal_without_reading_remainder(self):
        fake = _FakeReader()

        class BombInput:
            def __init__(self):
                self.calls = 0

            def readline(self, size=-1):
                self.calls += 1
                if self.calls > 1:
                    raise AssertionError("oversized line remainder must not be read")
                self.requested_size = size
                return b"x" * (MAX_REQUEST_LINE_BYTES + 1)

        source = BombInput()
        output = io.BytesIO()
        code = serve_session(
            lambda *_args, **_kwargs: fake,
            controls=self.fixture.control,
            root=self.fixture.baseline_package,
            delta_root=self.fixture.delta_root,
            expected_generation_id=BASELINE_ID,
            input_stream=source,
            output_stream=output,
        )
        frames = _read_frames(output.getvalue())
        self.assertEqual(code, 2)
        self.assertEqual(source.calls, 1)
        self.assertEqual(source.requested_size, MAX_REQUEST_LINE_BYTES + 1)
        self.assertEqual(frames[-1]["category"], "SESSION_REQUEST_TOO_LARGE")
        self.assertFalse(any(frame["type"] == "closed" for frame in frames))
        self.assertEqual(fake.exit_count, 1)

    def test_real_control_mutation_during_chunk_output_has_no_end_frame(self):
        blob = b"chunked-source-value\x00" * 7000
        self._replace_baseline_with_large_blob(blob)
        readers = []

        def factory(*args, **kwargs):
            reader = PublishedSQLiteReader(*args, **kwargs)
            readers.append(reader)
            return reader

        class MutatingOutput(io.BytesIO):
            def __init__(self, control_path):
                super().__init__()
                self.control_path = control_path
                self.mutated = False

            def write(self, raw):
                count = super().write(raw)
                if not self.mutated and b'"type":"response_chunk"' in raw:
                    self.control_path.write_bytes(self.control_path.read_bytes() + b" ")
                    self.mutated = True
                return count

        baseline_index = self.fixture.control / "generations" / BASELINE_ID / "index.json"
        output = MutatingOutput(baseline_index)
        code = serve_session(
            factory,
            controls=self.fixture.control,
            root=self.fixture.baseline_package,
            delta_root=self.fixture.delta_root,
            expected_generation_id=BASELINE_ID,
            input_stream=io.BytesIO(
                _request("mutated-on-wire", "get_unique_row", table="records", criteria={
                    "id": _cell("integer", "decimal", "-42"),
                })
                + _request("unused-close", "close")
            ),
            output_stream=output,
        )
        self.assertTrue(output.mutated)
        self.assertEqual(code, 2)
        frames = _read_frames(output.getvalue())
        self.assertTrue(any(frame["type"] == "response_begin" for frame in frames))
        self.assertTrue(any(frame["type"] == "response_chunk" for frame in frames))
        self.assertFalse(any(frame["type"] == "response_end" for frame in frames))
        self.assertFalse(any(frame["type"] == "closed" for frame in frames))
        self.assertEqual(frames[-1]["type"], "error")
        self.assertEqual(frames[-1]["category"], "PUBLISHED_CONTROL_CHANGED")
        self.assertEqual(frames[-1]["request_id"], "mutated-on-wire")
        self.assertEqual(len(readers), 1)
        self.assertFalse(readers[0]._active)

    def test_prebegin_guard_failure_emits_no_response_begin(self):
        fake = _FakeReader(fail_guard_at=3)

        code, raw = self._serve(
            lambda *_args, **_kwargs: fake,
            _request("mutated-before", "get_unique_row", table="records", criteria={})
            + _request("unused-close", "close"),
        )
        self.assertEqual(code, 2)
        frames = _read_frames(raw)
        self.assertFalse(any(frame["type"] == "response_begin" for frame in frames))
        self.assertEqual(frames[-1], {
            "type": "error", "version": 1, "request_id": "mutated-before",
            "category": "PUBLISHED_CONTROL_CHANGED",
        })
        self.assertEqual(fake.exit_count, 1)

    def test_preend_guard_failure_leaves_only_uncertified_chunks(self):
        blob = b"z" * (RESPONSE_CHUNK_BYTES * 2 + 17)
        fake = _FakeReader(values=(1, "text", 1.5, blob, None), fail_guard_at=4)

        code, raw = self._serve(
            lambda *_args, **_kwargs: fake,
            _request("mutated-before-end", "get_unique_row", table="records", criteria={})
            + _request("unused-close", "close"),
        )
        self.assertEqual(code, 2)
        frames = _read_frames(raw)
        self.assertTrue(any(frame["type"] == "response_begin" for frame in frames))
        self.assertTrue(any(frame["type"] == "response_chunk" for frame in frames))
        self.assertFalse(any(frame["type"] == "response_end" for frame in frames))
        self.assertEqual(frames[-1]["category"], "PUBLISHED_CONTROL_CHANGED")
        self.assertEqual(frames[-1]["request_id"], "mutated-before-end")
        self.assertEqual(fake.exit_count, 1)

    def test_close_guard_failure_never_emits_closed_and_interrupt_is_preserved(self):
        exit_error = PublishedSQLiteReaderError("PUBLISHED_CONTROL_CHANGED")
        fake = _FakeReader(exit_error=exit_error)
        code, raw = self._serve(
            lambda *_args, **_kwargs: fake,
            _request("close-fail", "close"),
        )
        self.assertEqual(code, 2)
        frames = _read_frames(raw)
        self.assertFalse(any(frame["type"] == "closed" for frame in frames))
        self.assertEqual(frames[-1]["category"], "PUBLISHED_CONTROL_CHANGED")
        self.assertEqual(frames[-1]["request_id"], "close-fail")

        interrupt = KeyboardInterrupt("private interrupt object")
        fake_interrupt = _FakeReader(
            exit_error=PublishedSQLiteReaderError("PUBLISHED_CONTROL_CHANGED"),
            lookup_error=interrupt,
        )
        with self.assertRaises(KeyboardInterrupt) as raised:
            self._serve(
                lambda *_args, **_kwargs: fake_interrupt,
                _request("interrupt", "get_unique_row", table="records", criteria={})
                + _request("close", "close"),
            )
        self.assertIs(raised.exception, interrupt)
        self.assertEqual(fake_interrupt.exit_count, 1)


_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


if __name__ == "__main__":
    unittest.main()
