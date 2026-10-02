"""Artificial backend and real loopback HTTP tests; no NAS or live data."""

import contextlib
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import selectors
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import urlencode, urlsplit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts import desktop_app

FAKE_BACKEND = '''import json, os, sys, time
from pathlib import Path
mode, pid_path, command, *args = sys.argv[1:]
Path(pid_path).write_text(str(os.getpid()))
sys.stderr.write("PRIVATE_SSH_STDERR_TOKEN")
state_path = Path(pid_path).with_name("tags.json")

def load_state():
    if state_path.exists():
        return json.loads(state_path.read_text())
    return {
        "open-key": {"analysis_set": "bankara_open", "genre": "bankara_open", "tags": []},
        "private-key": {
            "analysis_set": "private_four_vs_four", "genre": "private",
            "tags": [{"tag": "エンジョイ", "note": None, "created_at": "t", "updated_at": "t"}],
        },
        "x-key": {"analysis_set": "xmatch", "genre": "xmatch", "tags": []},
        "bare-key": {"analysis_set": None, "genre": None, "tags": []},
    }

def save_state(state):
    state_path.write_text(json.dumps(state, ensure_ascii=False))

if mode == "fail":
    print("PRIVATE_BACKEND_BODY")
    sys.exit(1)
if mode == "timeout":
    time.sleep(10)
if mode == "exit_timeout":
    os.close(1)
    time.sleep(10)
if mode == "oversize":
    sys.stdout.write("x" * 2048)
    sys.exit(0)
if mode == "invalid":
    print("PRIVATE_INVALID_JSON")
    sys.exit(0)
if mode == "nonfinite":
    print("NaN")
    sys.exit(0)
if command == "status":
    result = {"healthy": True}
elif command == "sql":
    assert args == ["SELECT DISTINCT account FROM matches ORDER BY account"]
    result = [{"account": "artificial-account-a"}, {"account": "artificial-account-b"}]
elif command == "records" and args[0] == "tag-summary":
    result = {"sets": [
        {"analysis_set": "bankara_open", "matches": 2, "untagged": 1,
         "tags": [{"tag": "下げラン", "matches": 1}]},
        {"analysis_set": "private_two_vs_two", "matches": 0, "untagged": 0, "tags": []},
    ]}
elif command == "records" and args[0] == "rule-results":
    result = {"sets": [
        {"analysis_set": "bankara_open", "rules": [
            {"rule_raw": "AREA", "rule_name": "ガチエリア", "matches": 2,
             "wins": 1, "losses": 1, "draws": 0, "other": 0},
        ]},
    ]}
elif command == "records" and args[0] == "rate-summary":
    result = {"series": [
        {"series_id": "bankara_open|AREA|bankaraPower", "label": "バンカラパワー",
         "genre": "bankara_open", "rule_raw": "AREA", "source": "api",
         "priority": "primary", "unit": "number", "count": 2,
         "latest": 2100.0, "previous": 2000.0, "delta": 100.0,
         "minimum": 2000.0, "maximum": 2100.0},
    ]}
elif command == "records" and args[0] == "facets":
    result = {
        "analysis_sets": ["nawabari", "unclassified"],
        "rules": ["AREA"],
        "weapons": ["わかばシューター"],
        "tags": ["エンジョイ"],
    }
elif command == "records" and args[0] == "list":
    account = args[args.index("--account") + 1]
    result = {"total": 1, "limit": int(args[args.index("--limit") + 1]),
              "offset": int(args[args.index("--offset") + 1]),
              "items": [{"account": account, "kind": "vs", "match_key": "artificial-key",
                          "detail_state": "unresolved"}]}
elif command == "records" and args[0] == "get":
    key = args[args.index("--match-key") + 1]
    result = None if key == "missing" else {"match_key": key, "detail_json": "{}",
                                           "source": {"body_base64": "e30="}}
elif command == "records" and args[0] == "tag-target":
    key = args[args.index("--match-key") + 1]
    row = load_state().get(key)
    result = None if row is None else {
        "items": [{"kind": "vs", "genre": row["genre"], "analysis_set": row["analysis_set"]}],
        "tags": row["tags"],
    }
elif command == "slice-list":
    assert args == []
    if mode == "dataset_leak":
        result = {"items": [{"token": "unified", "axis": "unified", "file": "/tmp/secret.sqlite3"}]}
    else:
        result = {"items": [
            {"token": "unified", "axis": "unified"},
            {"token": "mode:bankara_open", "axis": "mode", "analysis_set": "bankara_open", "matches": 2},
            {"token": "rule:bankara_open:AREA", "axis": "rule", "analysis_set": "bankara_open", "rule_raw": "AREA", "matches": 1},
        ]}
elif command == "published-xlsx":
    if mode == "xlsx_missing":
        sys.stderr.write("PUBLISHED_XLSX_MISSING")
        sys.stdout.buffer.write(b"PRIVATE_XLSX_BODY")
        sys.exit(2)
    if mode == "xlsx_unusable":
        sys.stderr.write("PUBLISHED_XLSX_UNUSABLE")
        sys.stdout.buffer.write(b"PRIVATE_XLSX_BODY")
        sys.exit(3)
    if mode == "xlsx_not_pk":
        sys.stdout.buffer.write(b"NOT_A_WORKBOOK")
        sys.exit(0)
    payload = b"PK\x03\x04artificial-workbook"
    sys.stdout.buffer.write(payload)
    sys.exit(0)
elif command == "tag":
    action = args[0]
    key = args[args.index("--match-key") + 1]
    tag = args[args.index("--tag") + 1]
    state = load_state()
    row = state.get(key)
    if action not in ("add", "remove") or row is None:
        sys.exit(1)
    if action == "add":
        if not any(item["tag"] == tag for item in row["tags"]):
            row["tags"].append({"tag": tag, "note": None, "created_at": "t2", "updated_at": "t2"})
    else:
        row["tags"] = [item for item in row["tags"] if item["tag"] != tag]
    save_state(state)
    result = {"action": action, "match_key": key}
else:
    sys.exit(1)
print(json.dumps(result))
'''


class TestDesktopHTTP(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.fake = self.root / "backend.py"
        self.fake.write_text(FAKE_BACKEND, encoding="utf-8")
        self.pid_file = self.root / "backend.pid"
        self.mode = "normal"
        self.calls = []
        static = {}
        for route, content, mime in (
            ("/", b"<!doctype html><p>artificial app</p>", "text/html; charset=utf-8"),
            ("/index.html", b"<!doctype html>", "text/html; charset=utf-8"),
            ("/app.js", b"'use strict';", "text/javascript; charset=utf-8"),
            ("/app.css", b"body {}", "text/css; charset=utf-8"),
            ("/font.otf", b"artificial font", "font/otf"),
        ):
            path = self.root / (route.strip("/") or "root.html")
            path.write_bytes(content)
            static[route] = (path, mime)
        self.token = "PRIVATE_TEST_TOKEN_ASCII"

        def build(command, args, dataset="unified"):
            recorded = (command, list(args)) if dataset == "unified" else (command, list(args), dataset)
            self.calls.append(recorded)
            return [sys.executable, str(self.fake), self.mode, str(self.pid_file), command, *args]

        self.server = desktop_app.DesktopHTTPServer(
            ("127.0.0.1", 0), self.token, build, static_files=static,
            backend_timeout=0.3, backend_max_bytes=1024, connection_timeout=0.15,
        )
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()
        self.host = f"127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def request(self, target, *, method="GET", auth=True, headers=None):
        headers = [("Host", self.host), *([] if headers is None else headers)]
        if auth:
            headers.append(("Authorization", "Bearer " + self.token))
        return self.request_headers(target, headers, method=method)

    def request_headers(self, target, headers, *, method="GET", body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        try:
            conn.putrequest(method, target, skip_host=True, skip_accept_encoding=True)
            for key, value in headers:
                conn.putheader(key, value)
            conn.endheaders()
            if body:
                conn.send(body)
            result = conn.getresponse()
            return result.status, dict(result.getheaders()), result.read()
        finally:
            conn.close()

    def post_json(self, target, payload, *, origin=True, auth=True, content_type="application/json"):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = [("Host", self.host), ("Content-Type", content_type), ("Content-Length", str(len(body)))]
        if origin:
            headers.insert(1, ("Origin", "http://" + self.host))
        if auth:
            headers.append(("Authorization", "Bearer " + self.token))
        return self.request_headers(target, headers, method="POST", body=body)

    def error(self, target, status, category, **kwargs):
        actual, headers, body = self.request(target, **kwargs)
        self.assertEqual(actual, status, body)
        self.assertEqual(json.loads(body), {"error": category})
        self.security_headers(headers)

    def security_headers(self, headers):
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Referrer-Policy"], "no-referrer")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["Content-Security-Policy"], desktop_app.CSP)
        self.assertIn("font-src 'self'", headers["Content-Security-Policy"])
        self.assertFalse(any(key.lower().startswith("access-control-") for key in headers))

    def _request_with_dataset_payload(self, payload):
        with patch.object(desktop_app, "read_backend_json", return_value=payload) as backend:
            status, headers, body = self.request("/api/datasets")
        backend.assert_called_once()
        self.assertEqual(backend.call_args.args[0][-1], "slice-list")
        self.security_headers(headers)
        return status, json.loads(body)

    @staticmethod
    def _file_bytes(root):
        return {
            path.relative_to(root).as_posix(): path.read_bytes()
            for path in root.rglob("*") if path.is_file()
        }

    def test_api_datasets_accepts_exact_current_producer_envelopes(self):
        sys.path.insert(0, str(ROOT / "src/python"))
        from ikarchive.slices import list_datasets

        generation_id = "20261003T123456Z-a1b2c3d4"
        counts = {
            "mode_files": 2,
            "rule_files": 6,
            "distinct_modes_with_matches": 2,
            "distinct_rules": 3,
            "rule_mode_product": 6,
        }
        for producer_state in ("absent", "legacy", "verified"):
            with self.subTest(producer_state=producer_state):
                producer_root = self.root / producer_state
                database_dir = producer_root / "database"
                database_dir.mkdir(parents=True)
                database = database_dir / "archive.sqlite3"
                # list_datasets only consults the path and generated manifest; it must not
                # open this artificial placeholder as a database.
                database.write_bytes(b"artificial source placeholder")
                slices = database_dir / "slices"
                if producer_state == "legacy":
                    slices.mkdir()
                    (slices / "manifest.json").write_text('{"legacy":true}\n', encoding="utf-8")
                elif producer_state == "verified":
                    data = slices / "generations" / generation_id / "data"
                    data.mkdir(parents=True)
                    source_sha = "a" * 64
                    (slices / "current-generation.json").write_text(json.dumps({
                        "version": 2,
                        "generation_id": generation_id,
                        "root": f"generations/{generation_id}/data",
                        "source": {"sha256": source_sha},
                        "status": "verified",
                    }), encoding="utf-8")
                    (data / "manifest.json").write_text(json.dumps({
                        "role": "lossless_sqlite_shards",
                        "version": 2,
                        "snapshot_identifier": generation_id,
                        "counts": counts,
                    }), encoding="utf-8")
                    (data / "verification.json").write_text(json.dumps({
                        "status": "verified",
                        "source_sha256": source_sha,
                    }), encoding="utf-8")

                before = self._file_bytes(producer_root)
                self.calls.clear()

                def real_producer(command, **_kwargs):
                    self.assertEqual(command[-1], "slice-list")
                    return list_datasets(database)

                with patch.object(desktop_app, "read_backend_json", side_effect=real_producer) as backend:
                    status, headers, body = self.request("/api/datasets")
                backend.assert_called_once()
                self.assertEqual(self.calls, [("slice-list", [])])
                self.assertEqual(status, 200, body)
                self.security_headers(headers)
                payload = json.loads(body)
                self.assertEqual(payload["items"], [{"token": "unified", "axis": "unified"}])
                if producer_state == "verified":
                    self.assertEqual(payload, {
                        "items": [{"token": "unified", "axis": "unified"}],
                        "partition_status": "verified_store_reader_pending",
                        "generation_id": generation_id,
                        "counts": counts,
                    })
                else:
                    expected_status = "absent" if producer_state == "absent" else "legacy_incomplete"
                    self.assertEqual(payload, {
                        "items": [{"token": "unified", "axis": "unified"}],
                        "partition_status": expected_status,
                    })
                self.assertEqual(self._file_bytes(producer_root), before)

    def test_api_datasets_rejects_bad_status_counts_and_metadata(self):
        good_counts = {
            "mode_files": 2,
            "rule_files": 6,
            "distinct_modes_with_matches": 2,
            "distinct_rules": 3,
            "rule_mode_product": 6,
        }
        unified = [{"token": "unified", "axis": "unified"}]
        verified = {
            "items": unified,
            "partition_status": "verified_store_reader_pending",
            "generation_id": "20261003T123456Z-a1b2c3d4",
            "counts": good_counts,
        }
        bad_payloads = [
            {"items": unified, "partition_status": "unknown"},
            {"items": unified, "partition_status": "absent", "generation_id": "20261003T123456Z-a1b2c3d4"},
            {"items": unified, "partition_status": "legacy_incomplete", "counts": good_counts},
            {"items": unified, "partition_status": "verified_store_reader_pending", "generation_id": "../../escape", "counts": good_counts},
            {**verified, "counts": {**good_counts, "mode_files": True}},
            {**verified, "counts": {**good_counts, "distinct_rules": -1}},
            {**verified, "counts": {key: value for key, value in good_counts.items() if key != "rule_files"}},
            {**verified, "counts": {**good_counts, "unlisted": 1}},
            {**verified, "counts": {**good_counts, "rule_mode_product": 5}},
            {**verified, "counts": {**good_counts, "mode_files": 1}},
            {**verified, "items": [*unified, {"token": "mode:bankara_open", "axis": "mode", "analysis_set": "bankara_open"}]},
            {"items": unified, "partition_status": "absent", "path": "/private/source.sqlite3"},
            {"items": [{**unified[0], "file": "/private/source.sqlite3"}], "partition_status": "absent"},
        ]
        for index, payload in enumerate(bad_payloads):
            with self.subTest(index=index):
                status, result = self._request_with_dataset_payload(payload)
                self.assertEqual(status, 502)
                self.assertEqual(result, {"error": "BACKEND_FAILED"})

    def test_static_routes_are_fixed_and_do_not_require_token(self):
        for route in ("/", "/index.html", "/app.js", "/app.css", "/font.otf"):
            with self.subTest(route=route):
                status, headers, body = self.request(route, auth=False)
                self.assertEqual(status, 200)
                self.assertTrue(body)
                self.security_headers(headers)
                self.assertNotIn(self.token.encode(), body)
        self.assertEqual(self.request("/font.otf", auth=False)[1]["Content-Type"], "font/otf")
        for route in ("/../archive.py", "/%2e%2e/archive.py", "/assets/app/index.html", "/etc/passwd"):
            self.error(route, 404, "NOT_FOUND", auth=False)
        self.assertEqual(self.calls, [])

    def test_host_origin_auth_and_cookie_fail_closed(self):
        invalid_headers = (
            [],
            [("Host", "localhost:" + str(self.server.server_port))],
            [("Host", self.host), ("Host", self.host)],
            [("Host", self.host), ("Origin", "https://attacker.invalid")],
            [("Host", self.host), ("Origin", "null")],
            [("Host", self.host), ("Origin", "http://" + self.host), ("Origin", "http://" + self.host)],
            [("Host", self.host), ("Cookie", "token=" + self.token)],
        )
        for headers in invalid_headers:
            with self.subTest(headers=headers):
                status, response_headers, body = self.request_headers("/api/status", headers)
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(body), {"error": "FORBIDDEN"})
                self.security_headers(response_headers)
        for auth_headers in (
            [], [("Authorization", "Bearer wrong")], [("Authorization", "Bearer é")],
            [("Authorization", "Bearer " + self.token), ("Authorization", "Bearer " + self.token)],
        ):
            with self.subTest(auth_headers=auth_headers):
                status, headers, body = self.request_headers("/api/status", [("Host", self.host), *auth_headers])
                self.assertEqual(status, 401)
                self.assertEqual(json.loads(body), {"error": "UNAUTHORIZED"})
                self.security_headers(headers)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.request("/api/status", headers=[("Origin", "http://" + self.host)])[0], 200)

    def test_non_get_methods_never_run_backend(self):
        for method in ("POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "UNRECOGNIZED"):
            self.error("/api/status", 405, "METHOD_NOT_ALLOWED", method=method)
        self.assertEqual(self.request("/api/status", method="HEAD")[0], 405)
        self.assertEqual(self.calls, [])

    def test_fixed_api_dispatch_success_and_not_found(self):
        status, _, body = self.request("/api/status")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"healthy": True})
        status, _, body = self.request("/api/accounts")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"items": [
            {"account": "artificial-account-a", "label": "アカウント1"},
            {"account": "artificial-account-b", "label": "アカウント2"},
        ]})
        self.assertEqual(self.calls[-1], ("sql", ["SELECT DISTINCT account FROM matches ORDER BY account"]))
        status, _, body = self.request("/api/records?" + urlencode({"account": "a ; $(x)", "kind": "coop", "limit": 2, "offset": 3}))
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["limit"], 2)
        self.assertEqual(self.calls[-1], ("records", ["list", "--account", "a ; $(x)", "--kind", "coop", "--limit", "2", "--offset", "3"]))
        status, _, body = self.request("/api/record?account=a&kind=vs&match_key=artificial-key")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["source"], {"body_base64": "e30="})
        self.error("/api/record?account=a&kind=vs&match_key=missing", 404, "RECORD_NOT_FOUND")
        self.error("/api/unlisted", 404, "NOT_FOUND")
        status, _, body = self.request("/api/record-facets?account=a")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["weapons"], ["わかばシューター"])
        self.assertEqual(self.calls[-1], ("records", ["facets", "--account", "a"]))
        status, _, body = self.request("/api/tag-summary?account=a")
        self.assertEqual(status, 200)
        summary = json.loads(body)
        self.assertEqual(summary["sets"][0]["analysis_set"], "bankara_open")
        self.assertEqual(summary["sets"][0]["matches"], 2)
        self.assertNotIn("total", summary)
        self.assertEqual(self.calls[-1], ("records", ["tag-summary", "--account", "a"]))
        status, _, body = self.request("/api/rule-results?account=a")
        self.assertEqual(status, 200)
        results = json.loads(body)
        self.assertEqual(set(results), {"sets"})
        self.assertEqual(results["sets"][0]["analysis_set"], "bankara_open")
        self.assertEqual(results["sets"][0]["rules"][0]["wins"], 1)
        self.assertEqual(self.calls[-1], ("records", ["rule-results", "--account", "a"]))
        status, _, body = self.request("/api/rate-summary?account=a")
        self.assertEqual(status, 200)
        rates = json.loads(body)
        self.assertEqual(set(rates), {"series"})
        self.assertEqual(rates["series"][0]["latest"], 2100.0)
        self.assertNotIn("total", rates)
        self.assertEqual(self.calls[-1], ("records", ["rate-summary", "--account", "a"]))
        status, _, body = self.request("/api/records?" + urlencode({
            "account": "a",
            "analysis_set": "bankara_open",
            "rule": "AREA",
            "played_from": "2026-09-01T00:00:00Z",
            "played_to": "2026-09-01T01:00:00Z",
            "weapon": "わかばシューター",
            "tag": "下げラン",
            "q": "ユノハナ",
        }))
        self.assertEqual(status, 200)
        self.assertEqual(self.calls[-1], ("records", [
            "list", "--account", "a",
            "--analysis-set", "bankara_open", "--rule", "AREA",
            "--played-from", "2026-09-01T00:00:00Z", "--played-to", "2026-09-01T01:00:00Z",
            "--weapon", "わかばシューター", "--tag", "下げラン", "--q", "ユノハナ",
            "--limit", "50", "--offset", "0",
        ]))
        for target in (
            "/api/records?" + urlencode({"account": "a", "analysis_set": "オープン"}),
            "/api/records?" + urlencode({"account": "a", "rule": "ガチエリア"}),
            "/api/records?account=a&played_from=2026-09-01",
            "/api/records?account=a&played_from=2026-09-01T01:00:00Z&played_to=2026-09-01T00:00:00Z",
            "/api/records?account=a&weapon=",
            "/api/records?account=a&q=" + "x" * 81,
            "/api/record-facets",
            "/api/tag-summary",
            "/api/tag-summary?account=",
            "/api/tag-summary?account=a&set=xmatch",
            "/api/rule-results",
            "/api/rule-results?account=",
            "/api/rule-results?account=a&rule=AREA",
            "/api/rate-summary",
            "/api/rate-summary?account=",
            "/api/rate-summary?account=a&series=power",
        ):
            self.error(target, 400, "BAD_QUERY")

    def test_query_validation_and_no_arbitrary_sql(self):
        invalid = (
            "/api/status?query=SELECT%20secret", "/api/accounts?sql=DROP%20TABLE%20matches",
            "/api/status?token=" + self.token, "/api/records", "/api/records?account=",
            "/api/records?account=a&account=b", "/api/records?account=a&db=/tmp/other",
            "/api/status?dataset=../x",
            "/api/status?dataset=/data/database/slices/by-mode/bankara_open.sqlite3",
            "/api/status?dataset=mode:../x",
            "/api/status?dataset=",
            "/api/records?account=a&dataset=mode:bankara_open&dataset=mode:xmatch",
            "/api/datasets?dataset=mode:bankara_open",
            "/api/published-xlsx?dataset=unified",
            "/api/records?account=a&kind=other", "/api/records?account=a&kind=",
            "/api/records?account=a&limit=0", "/api/records?account=a&limit=201",
            "/api/records?account=a&limit=+1", "/api/records?account=a&limit=1.0",
            "/api/records?account=a&offset=-1", "/api/records?account=a&offset=9223372036854775808",
            "/api/records?account=%FF", "/api/records?account=%ZZ",
            "/api/record?account=a&kind=vs", "/api/record?account=a&kind=vs&match_key=",
            "/api/record?account=a&kind=invalid&match_key=k",
            "/api/records?account=" + "a" * 1025,
            "/api/record?account=a&kind=vs&match_key=" + "k" * 1025,
        )
        for target in invalid:
            with self.subTest(target=target[:100]):
                self.error(target, 400, "BAD_QUERY")
        self.error("/api/records?account=" + "x" * 8193, 414, "QUERY_TOO_LONG")
        self.assertEqual(self.calls, [])
        self.assertEqual(self.request("/api/records?account=a&limit=200&offset=9223372036854775807")[0], 200)

    def test_published_xlsx_is_retired_without_backend_access(self):
        self.error("/api/published-xlsx", 410, "LEGACY_ANALYSIS_XLSX_RETIRED")
        self.assertEqual(self.calls, [])
        self.error("/api/published-xlsx?account=a", 400, "BAD_QUERY")
        self.assertEqual(self.calls, [])
        self.error("/api/published-xlsx", 401, "UNAUTHORIZED", auth=False)
        self.assertEqual(self.calls, [])
        posted, _, posted_body = self.request("/api/published-xlsx", method="POST")
        self.assertEqual(posted, 405)
        self.assertEqual(json.loads(posted_body), {"error": "METHOD_NOT_ALLOWED"})
        self.assertEqual(self.calls, [])

    def test_published_xlsx_ui_is_disabled_and_status_is_truthful(self):
        html = (ROOT / "assets/app/index.html").read_text(encoding="utf-8")
        self.assertIn("旧分析表には欠落があるため利用できません。全情報の書き出しは準備中です。", html)
        self.assertIn('id="published-xlsx-button" class="btn btn-secondary" disabled', html)
        self.assertIn("旧分析表は利用できません", html)

    def test_backend_failure_timeout_oversize_and_bad_json_do_not_leak(self):
        for mode, status, category in (
            ("fail", 502, "BACKEND_FAILED"), ("timeout", 504, "BACKEND_TIMEOUT"),
            ("exit_timeout", 504, "BACKEND_TIMEOUT"), ("oversize", 502, "BACKEND_TOO_LARGE"),
            ("invalid", 502, "BACKEND_FAILED"), ("nonfinite", 502, "BACKEND_FAILED"),
        ):
            self.mode = mode
            stdout, stderr = io.StringIO(), io.StringIO()
            with self.subTest(mode=mode), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                self.error("/api/status", status, category)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(stderr.getvalue(), "")
            pid = int(self.pid_file.read_text())
            if os.name != "nt":
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)

    def test_idle_and_incomplete_connections_expire(self):
        for prefix in (b"", b"GET / HTTP/1.1\r\nHost: "):
            with self.subTest(prefix=prefix), socket.create_connection(("127.0.0.1", self.server.server_port), timeout=2) as idle:
                if prefix:
                    idle.sendall(prefix)
                time.sleep(0.03)
                started = time.monotonic()
                self.assertEqual(self.request("/", auth=False)[0], 200)
                self.assertLess(time.monotonic() - started, 1.0)


    def test_post_tags_for_open_and_private_only(self):
        status, headers, body = self.post_json("/api/tags", {
            "account": "artificial-account", "match_key": "open-key", "action": "add", "tag": "カスタム",
        })
        self.assertEqual(status, 200, body)
        self.security_headers(headers)
        payload = json.loads(body)
        self.assertEqual(set(payload), {"match_key", "action", "tags"})
        self.assertEqual(payload["action"], "add")
        self.assertEqual([item["tag"] for item in payload["tags"]], ["カスタム"])
        self.assertEqual([call[0] for call in self.calls], ["records", "tag", "records"])
        self.assertEqual(self.calls[1], ("tag", ["add", "--account", "artificial-account", "--match-key", "open-key", "--tag", "カスタム"]))

        self.calls.clear()
        status, _, body = self.post_json("/api/tags", {
            "account": "artificial-account", "match_key": "private-key", "action": "remove", "tag": "エンジョイ",
        })
        self.assertEqual(status, 200, body)
        self.assertEqual(json.loads(body)["tags"], [])
        self.assertEqual(self.calls[1][0], "tag")

        self.calls.clear()
        status, _, body = self.post_json("/api/tags", {
            "account": "artificial-account", "match_key": "x-key", "action": "add", "tag": "ガチ",
        })
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body), {"error": "TAG_GENRE_NOT_ALLOWED"})
        self.assertEqual([call[0] for call in self.calls], ["records"])

        self.calls.clear()
        status, _, body = self.post_json("/api/tags", {
            "account": "artificial-account", "match_key": "bare-key", "action": "add", "tag": "練習",
        })
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body), {"error": "TAG_GENRE_NOT_ALLOWED"})
        self.assertEqual([call[0] for call in self.calls], ["records"])

        self.calls.clear()
        status, _, body = self.post_json("/api/tags", {
            "account": "artificial-account", "match_key": "missing", "action": "add", "tag": "練習",
        })
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "MATCH_NOT_FOUND"})
        self.assertEqual([call[0] for call in self.calls], ["records"])

    def test_post_tags_rejects_bad_input_without_tag_command(self):
        for payload in (
            {"account": "artificial-account", "match_key": "open-key", "action": "add", "tag": "  "},
            {"account": "artificial-account", "match_key": "open-key", "action": "add", "tag": "a\nb"},
            {"account": "artificial-account", "match_key": "open-key", "action": "add", "tag": "あ" * 81},
            {"account": "artificial-account", "match_key": "open-key", "action": "add", "tag": "練習", "extra": 1},
        ):
            with self.subTest(payload=payload):
                self.calls.clear()
                status, _, body = self.post_json("/api/tags", payload)
                self.assertEqual(status, 400, body)
                self.assertEqual(self.calls, [])
        self.calls.clear()
        status, _, body = self.post_json("/api/tags", {
            "account": "artificial-account", "match_key": "open-key", "action": "add", "tag": "練習",
        }, origin=False)
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body), {"error": "FORBIDDEN"})
        self.assertEqual(self.calls, [])
        body = json.dumps({"account": "a", "match_key": "open-key", "action": "add", "tag": "練習"}).encode()
        status, _, response = self.request_headers(
            "/api/status",
            [("Host", self.host), ("Origin", "http://" + self.host),
             ("Authorization", "Bearer " + self.token),
             ("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
            method="POST", body=body)
        self.assertEqual(status, 405)
        self.assertEqual(json.loads(response), {"error": "METHOD_NOT_ALLOWED"})
        self.assertEqual(self.calls, [])

    def test_dataset_token_selects_a_fixed_file_and_rejects_paths(self):
        self.calls.clear()
        status, _, body = self.request("/api/datasets")
        self.assertEqual(status, 200)
        listed = json.loads(body)
        self.assertEqual([item["token"] for item in listed["items"]], [
            "unified", "mode:bankara_open", "rule:bankara_open:AREA",
        ])
        self.assertNotIn("file", json.dumps(listed))
        self.assertEqual(self.calls[-1], ("slice-list", []))
        status, _, body = self.request("/api/status?dataset=mode:bankara_open")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"healthy": True})
        self.assertEqual(self.calls[-1], ("status", [], "mode:bankara_open"))
        status, _, body = self.request("/api/records?" + urlencode({
            "account": "a", "dataset": "rule:bankara_open:AREA",
        }))
        self.assertEqual(status, 200)
        self.assertEqual(self.calls[-1][2], "rule:bankara_open:AREA")
        self.assertNotIn("--db", self.calls[-1][1])
        self.mode = "dataset_leak"
        self.error("/api/datasets", 502, "BACKEND_FAILED")

class TestDesktopStartup(unittest.TestCase):
    def test_backend_builders_are_fixed_arrays(self):
        local = desktop_app.backend_builder(Path("/artificial/archive.sqlite3"), None)
        self.assertEqual(local("records", ["list", "--account", "a"]),
                         [sys.executable, str(ROOT / "archive.py"), "--db", "/artificial/archive.sqlite3", "records", "list", "--account", "a"])
        marker = {"ssh_host": "artificial-nas", "container": "archive", "database": "/data/database/archive.sqlite3"}
        cmd = desktop_app.backend_builder(None, marker)("status", [])
        self.assertEqual(cmd[:4], ["ssh", "-o", "BatchMode=yes", "artificial-nas"])
        self.assertIn("/app/archive.py", cmd[-1])
        self.assertIn("--db /data/database/archive.sqlite3", cmd[-1])
        sliced = desktop_app.backend_builder(None, marker)("records", ["list"], "mode:bankara_open")
        remote = sliced[-1]
        self.assertIn("--db /data/database/slices/by-mode/bankara_open.sqlite3", remote)
        self.assertNotIn("archive.sqlite3", remote.split("--db ", 1)[1].split(" ", 1)[0])
        with self.assertRaises(ValueError):
            desktop_app.backend_builder(None, marker)("status", [], "mode:../x")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            db = root / "archive.sqlite3"
            db.write_bytes(b"unified")
            mode = root / "slices" / "by-mode" / "bankara_open.sqlite3"
            mode.parent.mkdir(parents=True)
            mode.write_bytes(b"mode-file")
            outside = root / "outside.sqlite3"
            outside.write_bytes(b"outside")
            local = desktop_app.backend_builder(db, None)
            self.assertEqual(local("status", [])[3], str(db))
            self.assertEqual(local("status", [], "mode:bankara_open")[3], str(mode.resolve()))
            with self.assertRaises(desktop_app.BackendFailure):
                local("status", [], "mode:xmatch")
            escaped = root / "slices" / "by-mode" / "xmatch.sqlite3"
            escaped.symlink_to(outside)
            with self.assertRaises(desktop_app.BackendFailure):
                local("status", [], "mode:xmatch")
        with self.assertRaises(ValueError):
            desktop_app.backend_builder(None, None)
        with self.assertRaises(ValueError):
            desktop_app.DesktopHTTPServer(("0.0.0.0", 0), "token", local)

    def test_missing_nas_marker_never_falls_back_to_local(self):
        with patch("scripts.nas_archive.read_marker", side_effect=ValueError("invalid artificial marker")), patch.object(desktop_app, "DesktopHTTPServer") as server:
            with self.assertRaises(ValueError):
                desktop_app.main(["--no-open"])
            server.assert_not_called()

    def test_start_url_is_only_printed_for_no_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "artificial.sqlite3"
            db.write_bytes(b"artificial file")
            server = MagicMock()
            server.server_port = 43210
            server.serve_forever.side_effect = KeyboardInterrupt
            factory = MagicMock()
            factory.return_value.__enter__.return_value = server
            for no_open in (False, True):
                out = io.StringIO()
                argv = ["--db", str(db), *(["--no-open"] if no_open else [])]
                with patch.object(desktop_app, "DesktopHTTPServer", factory), patch.object(desktop_app.secrets, "token_urlsafe", return_value="TOKEN_FOR_STARTUP"), patch.object(desktop_app.webbrowser, "open") as browser, patch("scripts.nas_archive.read_marker") as marker, contextlib.redirect_stdout(out):
                    self.assertEqual(desktop_app.main(argv), 0)
                    marker.assert_not_called()
                    if no_open:
                        self.assertEqual(out.getvalue().strip(), "http://127.0.0.1:43210/#token=TOKEN_FOR_STARTUP")
                        browser.assert_not_called()
                    else:
                        self.assertNotIn("TOKEN_FOR_STARTUP", out.getvalue())
                        browser.assert_called_once_with("http://127.0.0.1:43210/#token=TOKEN_FOR_STARTUP")

    def test_actual_cli_with_artificial_small_database_is_read_only(self):
        sys.path.insert(0, str(ROOT / "src/python"))
        from ikarchive.store import Store
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "artificial.sqlite3"
            store = Store(db)
            store.db.execute("INSERT INTO matches VALUES(?,?,?,?,?,NULL)", ("artificial-account", "coop", "artificial-match", "t", "t"))
            store.db.commit()
            store.close()
            before = hashlib.sha256(db.read_bytes()).hexdigest()
            proc = subprocess.Popen([sys.executable, str(ROOT / "scripts/desktop_app.py"), "--db", str(db), "--no-open"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(5), "CLI did not report startup")
                url = urlsplit(proc.stdout.readline().strip())
                self.assertEqual(url.hostname, "127.0.0.1")
                token = url.fragment.removeprefix("token=")
                conn = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
                try:
                    conn.request("GET", "/api/accounts", headers={"Authorization": "Bearer " + token})
                    response = conn.getresponse()
                    self.assertEqual(response.status, 200)
                    self.assertEqual(json.loads(response.read())["items"][0]["account"], "artificial-account")
                finally:
                    conn.close()
                conn = http.client.HTTPConnection(url.hostname, url.port, timeout=5)
                try:
                    conn.request("GET", "/api/record?account=artificial-account&kind=coop&match_key=artificial-match", headers={"Authorization": "Bearer " + token})
                    response = conn.getresponse()
                    self.assertEqual(response.status, 200)
                    result = json.loads(response.read())
                    self.assertEqual(result["detail_state"], "unresolved")
                    self.assertIsNone(result["source"])
                    self.assertEqual(result["tags"], [])
                finally:
                    conn.close()
            finally:
                proc.terminate()
                proc.wait(timeout=5)
                proc.stdout.close()
                proc.stderr.close()
            self.assertEqual(hashlib.sha256(db.read_bytes()).hexdigest(), before)


if __name__ == "__main__":
    unittest.main()
