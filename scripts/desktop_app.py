#!/usr/bin/env python3
"""Loopback browser server for the archive CLI.

閲覧は読み取り専用。書き込めるのはプラベとオープンのタグだけ。
"""

import argparse
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import selectors
import subprocess
import sys
import time
from urllib.parse import parse_qsl, urlsplit
import webbrowser

ROOT = Path(__file__).resolve().parents[1]
STATIC_FILES = {
    "/": (ROOT / "assets/app/index.html", "text/html; charset=utf-8"),
    "/index.html": (ROOT / "assets/app/index.html", "text/html; charset=utf-8"),
    "/app.js": (ROOT / "assets/app/app.js", "text/javascript; charset=utf-8"),
    "/app.css": (ROOT / "assets/app/app.css", "text/css; charset=utf-8"),
    "/font.otf": (ROOT / "assets/fonts/Splatoon2-Unified.otf", "font/otf"),
}
MAX_QUERY_LENGTH = 8192
MAX_IDENTIFIER_LENGTH = 1024
MAX_TAG_BODY = 4096
MAX_BACKEND_BYTES = 8 * 1024 * 1024
BACKEND_TIMEOUT = 60.0
XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
XLSX_DISPOSITION = (
    "attachment; filename=\"analysis.xlsx\"; filename*=UTF-8''%E5%88%86%E6%9E%90.xlsx"
)
CONNECTION_TIMEOUT = 5.0
CSP = (
    "default-src 'none'; script-src 'self'; style-src 'self'; "
    "connect-src 'self'; img-src 'self' data:; font-src 'self'; "
    "base-uri 'none'; frame-ancestors 'none'"
)
DECIMAL_RE = re.compile(r"[0-9]+\Z")
INVALID_PERCENT_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")


class BackendFailure(Exception):
    def __init__(self, status: int, category: str):
        self.status = status
        self.category = category


def read_backend_output(command: list[str], *, timeout: float = BACKEND_TIMEOUT,
                        max_bytes: int = MAX_BACKEND_BYTES) -> tuple[bytes, int]:
    """Read at most max_bytes of CLI stdout and the exit code. Does not interpret them."""
    proc = None
    selector = None
    deadline = time.monotonic() + timeout
    try:
        proc = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, shell=False)
        selector = selectors.DefaultSelector()
        selector.register(proc.stdout, selectors.EVENT_READ)
        output = bytearray()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BackendFailure(504, "BACKEND_TIMEOUT")
            if not selector.select(remaining):
                raise BackendFailure(504, "BACKEND_TIMEOUT")
            chunk = os.read(proc.stdout.fileno(), min(65536, max_bytes + 1 - len(output)))
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > max_bytes:
                raise BackendFailure(502, "BACKEND_TOO_LARGE")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise BackendFailure(504, "BACKEND_TIMEOUT")
        try:
            exit_code = proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            raise BackendFailure(504, "BACKEND_TIMEOUT") from exc
        return bytes(output), exit_code
    except BackendFailure:
        raise
    except (OSError, subprocess.SubprocessError) as exc:
        raise BackendFailure(502, "BACKEND_FAILED") from exc
    finally:
        if selector is not None:
            selector.close()
        if proc is not None:
            if proc.stdout is not None:
                proc.stdout.close()
            if proc.poll() is None:
                proc.kill()
                proc.wait()


def read_backend_json(command: list[str], *, timeout: float = BACKEND_TIMEOUT,
                      max_bytes: int = MAX_BACKEND_BYTES):
    """Read at most max_bytes of CLI stdout, with one deadline for read and exit."""
    output, exit_code = read_backend_output(command, timeout=timeout, max_bytes=max_bytes)
    if exit_code != 0:
        raise BackendFailure(502, "BACKEND_FAILED")
    try:
        return json.loads(output.decode("utf-8"),
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise BackendFailure(502, "BACKEND_FAILED") from exc


def read_backend_xlsx(command: list[str], *, timeout: float = BACKEND_TIMEOUT,
                      max_bytes: int = MAX_BACKEND_BYTES) -> bytes:
    """Return an existing workbook. Exit 2 is absence. Anything else unusable is a failure."""
    output, exit_code = read_backend_output(command, timeout=timeout, max_bytes=max_bytes)
    if exit_code == 2:
        raise BackendFailure(404, "PUBLISHED_XLSX_MISSING")
    if exit_code != 0 or not output.startswith(b"PK"):
        raise BackendFailure(502, "BACKEND_FAILED")
    return output


def parse_query(query: str, allowed: set[str], required: set[str]) -> dict[str, str]:
    if len(query) > MAX_QUERY_LENGTH:
        raise BackendFailure(414, "QUERY_TOO_LONG")
    if INVALID_PERCENT_RE.search(query):
        raise BackendFailure(400, "BAD_QUERY")
    try:
        pairs = parse_qsl(query, keep_blank_values=True, encoding="utf-8", errors="strict")
    except (ValueError, UnicodeError) as exc:
        raise BackendFailure(400, "BAD_QUERY") from exc
    result = {}
    for key, value in pairs:
        if key not in allowed or key in result:
            raise BackendFailure(400, "BAD_QUERY")
        result[key] = value
    if not required.issubset(result):
        raise BackendFailure(400, "BAD_QUERY")
    return result


def identifier(value: str) -> str:
    if not value or len(value) > MAX_IDENTIFIER_LENGTH:
        raise BackendFailure(400, "BAD_QUERY")
    return value


def gui_tag_allowed(items) -> bool:
    """プラベとオープンだけを、画面からのタグ追加の対象にする。"""
    if not isinstance(items, list) or not items:
        return False
    for item in items:
        if not isinstance(item, dict):
            return False
        analysis = item.get("analysis_set")
        genre = item.get("genre")
        if isinstance(analysis, str) and analysis:
            allowed = analysis == "bankara_open" or analysis.startswith("private_")
        else:
            allowed = genre in ("bankara_open", "private")
        if not allowed:
            return False
    return True


def parse_tag_text(value):
    if not isinstance(value, str):
        raise BackendFailure(400, "BAD_TAG")
    tag = value.strip()
    if not tag or len(tag) > 80 or any(ord(ch) < 32 or ord(ch) == 127 for ch in tag):
        raise BackendFailure(400, "BAD_TAG")
    return tag


_ANALYSIS_SET_RE = re.compile(r'(?:unclassified|[a-z0-9_]{1,64})\Z')
_RULE_RE = re.compile(r'[A-Za-z0-9_]{1,64}\Z')
_PLAYED_RE = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z')
_GENERATION_ID_RE = re.compile(r'[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\Z', re.ASCII)


def bounded_text(value: str, *, limit: int = 80, pattern=None) -> str:
    if not value or len(value) > limit or any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise BackendFailure(400, "BAD_QUERY")
    if pattern is not None and not pattern.fullmatch(value):
        raise BackendFailure(400, "BAD_QUERY")
    return value


def decimal(value: str, *, lower: int, upper: int | None = None) -> int:
    if not DECIMAL_RE.fullmatch(value):
        raise BackendFailure(400, "BAD_QUERY")
    # SQLite offsets are signed 64-bit. Reject values the fixed CLI cannot bind.
    if len(value) > 19:
        raise BackendFailure(400, "BAD_QUERY")
    number = int(value)
    if number < lower or number > (upper if upper is not None else 2**63 - 1):
        raise BackendFailure(400, "BAD_QUERY")
    return number


def _nas_archive():
    if __package__:
        from . import nas_archive
    else:
        import nas_archive
    return nas_archive


def dataset_token(value: str) -> str:
    if not isinstance(value, str) or not _nas_archive().DATASET_RE.fullmatch(value):
        raise BackendFailure(400, "BAD_QUERY")
    return value


def local_dataset_database(root: Path, token: str) -> str:
    """ローカルの --db から、固定の名前で派生ファイルを選ぶ。名前はパスにしない。"""
    if token == "unified":
        return str(root)
    try:
        text = _nas_archive().dataset_database(str(root), token)
    except ValueError as exc:
        raise BackendFailure(400, "BAD_QUERY") from exc
    candidate = Path(text)
    slices_dir = root.parent / "slices"
    if slices_dir.is_symlink() or candidate.is_symlink() or not candidate.is_file():
        raise BackendFailure(404, "DATASET_NOT_FOUND")
    resolved = candidate.resolve()
    try:
        resolved.relative_to(slices_dir.resolve())
    except ValueError as exc:
        raise BackendFailure(404, "DATASET_NOT_FOUND") from exc
    return str(resolved)


_DATASET_ITEM_KEYS = {"token", "axis", "analysis_set", "rule_raw", "matches"}


def sanitize_datasets(payload):
    """一覧にパスを残さない。固定の名前と、表示に使う区分だけを返す。"""
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise BackendFailure(502, "BACKEND_FAILED")
    payload_keys = set(payload)
    status = payload.get("partition_status")
    has_partition_status = "partition_status" in payload
    if not has_partition_status:
        if payload_keys != {"items"}:
            raise BackendFailure(502, "BACKEND_FAILED")
    elif status in ("absent", "legacy_incomplete"):
        if payload_keys != {"items", "partition_status"}:
            raise BackendFailure(502, "BACKEND_FAILED")
    elif status == "verified_store_reader_pending":
        if payload_keys != {"items", "partition_status", "generation_id", "counts"}:
            raise BackendFailure(502, "BACKEND_FAILED")
        generation_id = payload.get("generation_id")
        if not isinstance(generation_id, str) or not _GENERATION_ID_RE.fullmatch(generation_id):
            raise BackendFailure(502, "BACKEND_FAILED")
        counts = payload.get("counts")
        count_keys = {
            "mode_files", "rule_files", "distinct_modes_with_matches",
            "distinct_rules", "rule_mode_product",
        }
        if not isinstance(counts, dict) or set(counts) != count_keys:
            raise BackendFailure(502, "BACKEND_FAILED")
        if any(type(value) is not int or value < 0 for value in counts.values()):
            raise BackendFailure(502, "BACKEND_FAILED")
        if (counts["rule_files"] != counts["rule_mode_product"]
                or counts["rule_mode_product"] !=
                counts["distinct_modes_with_matches"] * counts["distinct_rules"]
                or counts["mode_files"] < counts["distinct_modes_with_matches"]):
            raise BackendFailure(502, "BACKEND_FAILED")
    else:
        raise BackendFailure(502, "BACKEND_FAILED")
    if not payload["items"]:
        raise BackendFailure(502, "BACKEND_FAILED")
    if has_partition_status and (
            len(payload["items"]) != 1
            or not isinstance(payload["items"][0], dict)
            or payload["items"][0].get("token") != "unified"
            or payload["items"][0].get("axis") != "unified"):
        raise BackendFailure(502, "BACKEND_FAILED")
    items = []
    seen = set()
    for item in payload["items"]:
        if not isinstance(item, dict) or not set(item) <= _DATASET_ITEM_KEYS:
            raise BackendFailure(502, "BACKEND_FAILED")
        if "token" not in item or "axis" not in item:
            raise BackendFailure(502, "BACKEND_FAILED")
        token = dataset_token(item["token"])
        axis = item["axis"]
        if axis not in ("unified", "mode", "rule") or token in seen:
            raise BackendFailure(502, "BACKEND_FAILED")
        if (token == "unified") != (axis == "unified"):
            raise BackendFailure(502, "BACKEND_FAILED")
        if token.startswith("mode:") and axis != "mode":
            raise BackendFailure(502, "BACKEND_FAILED")
        if token.startswith("rule:") and axis != "rule":
            raise BackendFailure(502, "BACKEND_FAILED")
        clean = {"token": token, "axis": axis}
        if "analysis_set" in item:
            if not isinstance(item["analysis_set"], str):
                raise BackendFailure(502, "BACKEND_FAILED")
            clean["analysis_set"] = bounded_text(item["analysis_set"], limit=64, pattern=_ANALYSIS_SET_RE)
        if axis in ("mode", "rule") and "analysis_set" not in clean:
            raise BackendFailure(502, "BACKEND_FAILED")
        if "rule_raw" in item and item["rule_raw"] is not None:
            if not isinstance(item["rule_raw"], str):
                raise BackendFailure(502, "BACKEND_FAILED")
            clean["rule_raw"] = bounded_text(item["rule_raw"], limit=64, pattern=_RULE_RE)
        if "matches" in item:
            if type(item["matches"]) is not int or item["matches"] < 0:
                raise BackendFailure(502, "BACKEND_FAILED")
            clean["matches"] = item["matches"]
        if axis == "mode" and token != "mode:" + clean["analysis_set"]:
            raise BackendFailure(502, "BACKEND_FAILED")
        if axis == "rule":
            mode_part, rule_part = token.split(":", 2)[1:]
            if mode_part != clean["analysis_set"]:
                raise BackendFailure(502, "BACKEND_FAILED")
            if "rule_raw" in clean and clean["rule_raw"] != rule_part and not rule_part.startswith("rule_"):
                raise BackendFailure(502, "BACKEND_FAILED")
        seen.add(token)
        items.append(clean)
    if items[0]["token"] != "unified":
        raise BackendFailure(502, "BACKEND_FAILED")
    result = {"items": items}
    if has_partition_status:
        result["partition_status"] = status
        if status == "verified_store_reader_pending":
            result["generation_id"] = generation_id
            result["counts"] = dict(counts)
    return result


def backend_builder(db_path: Path | None, marker: dict | None):
    if db_path is not None:
        def build(command, args, dataset="unified"):
            database = local_dataset_database(db_path, dataset)
            return [sys.executable, str(ROOT / "archive.py"), "--db", database, command, *args]
        return build
    if marker is None:
        raise ValueError("NAS marker required when --db is not specified")
    nas_archive = _nas_archive()
    def build(command, args, dataset="unified"):
        return nas_archive.archive_command(marker, command, args, dataset=dataset)
    return build


class DesktopHTTPServer(HTTPServer):
    def __init__(self, address, token: str, build_command, *, static_files=None,
                 backend_timeout=BACKEND_TIMEOUT, backend_max_bytes=MAX_BACKEND_BYTES,
                 connection_timeout=CONNECTION_TIMEOUT):
        if address[0] != "127.0.0.1":
            raise ValueError("Loopback bind required")
        super().__init__(address, DesktopHandler)
        self.token = token
        self.build_command = build_command
        self.static_files = STATIC_FILES if static_files is None else static_files
        self.backend_timeout = backend_timeout
        self.backend_max_bytes = backend_max_bytes
        self.connection_timeout = connection_timeout

    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(self.connection_timeout)
        return sock, address

    def handle_error(self, request, client_address):
        # Broken connections and unexpected handler errors must never log requests.
        pass


class DesktopHandler(BaseHTTPRequestHandler):
    server_version = "ikaring-archive"
    sys_version = ""

    def log_message(self, format, *args):
        pass

    def version_string(self):
        return self.server_version

    def send_error(self, code, message=None, explain=None):
        self._json(code, {"error": "BAD_REQUEST"})

    def _headers(self, status: int, content_type: str, content_length: int, extra=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(content_length))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", CSP)
        if extra:
            for key, value in extra:
                self.send_header(key, value)
        self.end_headers()

    def _json(self, status: int, value):
        try:
            body = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError, UnicodeError):
            status = 502
            body = b'{"error":"BACKEND_FAILED"}'
        self._headers(status, "application/json; charset=utf-8", len(body))
        self.wfile.write(body)

    def _method_not_allowed(self):
        try:
            self._validate_request()
            self._json(405, {"error": "METHOD_NOT_ALLOWED"})
        except BackendFailure as exc:
            self._json(exc.status, {"error": exc.category})

    def __getattr__(self, name):
        if name.startswith("do_"):
            return self._method_not_allowed
        raise AttributeError(name)

    def do_GET(self):
        try:
            self._get()
        except BackendFailure as exc:
            self._json(exc.status, {"error": exc.category})
        except Exception:
            self._json(500, {"error": "INTERNAL_ERROR"})

    def do_POST(self):
        try:
            self._post()
        except BackendFailure as exc:
            self._json(exc.status, {"error": exc.category})
        except Exception:
            self._json(500, {"error": "INTERNAL_ERROR"})

    def _validate_request(self):
        host = self.headers.get_all("Host", [])
        expected = f"127.0.0.1:{self.server.server_port}"
        if len(host) != 1 or host[0] != expected:
            raise BackendFailure(403, "FORBIDDEN")
        origins = self.headers.get_all("Origin", [])
        if len(origins) > 1 or (origins and origins[0] != f"http://{expected}"):
            raise BackendFailure(403, "FORBIDDEN")
        if self.headers.get_all("Cookie", []):
            raise BackendFailure(403, "FORBIDDEN")

    def _get(self):
        self._validate_request()
        target = urlsplit(self.path)
        if target.scheme or target.netloc or target.fragment:
            raise BackendFailure(400, "BAD_REQUEST")
        if len(target.query) > MAX_QUERY_LENGTH:
            raise BackendFailure(414, "QUERY_TOO_LONG")
        if target.path in self.server.static_files:
            if target.query:
                raise BackendFailure(400, "BAD_QUERY")
            path, content_type = self.server.static_files[target.path]
            try:
                body = path.read_bytes()
            except OSError:
                raise BackendFailure(404, "NOT_FOUND")
            self._headers(200, content_type, len(body))
            self.wfile.write(body)
            return
        if not target.path.startswith("/api/"):
            raise BackendFailure(404, "NOT_FOUND")
        auth = self.headers.get_all("Authorization", [])
        if len(auth) != 1 or not auth[0].startswith("Bearer ") or not hmac.compare_digest(
            auth[0][7:].encode("utf-8"), self.server.token.encode("ascii")
        ):
            raise BackendFailure(401, "UNAUTHORIZED")
        if target.path == "/api/published-xlsx":
            parse_query(target.query, set(), set())
            raise BackendFailure(410, "LEGACY_ANALYSIS_XLSX_RETIRED")
        result = self._api(target.path, target.query)
        self._json(200, result)

    def _dataset(self, params: dict) -> str:
        if "dataset" not in params:
            return "unified"
        return dataset_token(params["dataset"])

    def _backend(self, command: str, args: list[str], dataset: str = "unified"):
        if dataset == "unified":
            command_line = self.server.build_command(command, args)
        else:
            command_line = self.server.build_command(command, args, dataset)
        return read_backend_json(command_line,
                                 timeout=self.server.backend_timeout,
                                 max_bytes=self.server.backend_max_bytes)

    def _api(self, path: str, query: str):
        if path == "/api/datasets":
            parse_query(query, set(), set())
            return sanitize_datasets(self._backend("slice-list", []))
        if path == "/api/status":
            dataset = self._dataset(parse_query(query, {"dataset"}, set()))
            return self._backend("status", [], dataset)
        if path == "/api/accounts":
            dataset = self._dataset(parse_query(query, {"dataset"}, set()))
            rows = self._backend("sql", ["SELECT DISTINCT account FROM matches ORDER BY account"], dataset)
            if not isinstance(rows, list) or any(
                not isinstance(row, dict) or not isinstance(row.get("account"), str)
                for row in rows
            ):
                raise BackendFailure(502, "BACKEND_FAILED")
            return {"items": [{"account": row["account"], "label": f"アカウント{i}"}
                              for i, row in enumerate(rows, 1)]}
        if path == "/api/record-facets":
            params = parse_query(query, {"account", "dataset"}, {"account"})
            account = identifier(params["account"])
            return self._backend("records", ["facets", "--account", account], self._dataset(params))
        if path == "/api/tag-summary":
            params = parse_query(query, {"account", "dataset"}, {"account"})
            account = identifier(params["account"])
            result = self._backend("records", ["tag-summary", "--account", account], self._dataset(params))
            if not isinstance(result, dict) or not isinstance(result.get("sets"), list):
                raise BackendFailure(502, "BACKEND_FAILED")
            return result
        if path == "/api/rule-results":
            params = parse_query(query, {"account", "dataset"}, {"account"})
            account = identifier(params["account"])
            result = self._backend("records", ["rule-results", "--account", account], self._dataset(params))
            if not isinstance(result, dict) or set(result) != {"sets"} or not isinstance(result.get("sets"), list):
                raise BackendFailure(502, "BACKEND_FAILED")
            return result
        if path == "/api/rate-summary":
            params = parse_query(query, {"account", "dataset"}, {"account"})
            account = identifier(params["account"])
            result = self._backend("records", ["rate-summary", "--account", account], self._dataset(params))
            if not isinstance(result, dict) or set(result) != {"series"} or not isinstance(result.get("series"), list):
                raise BackendFailure(502, "BACKEND_FAILED")
            return result
        if path == "/api/records":
            params = parse_query(query, {
                "account", "kind", "limit", "offset", "analysis_set", "rule",
                "played_from", "played_to", "weapon", "tag", "q", "dataset",
            }, {"account"})
            account = identifier(params["account"])
            args = ["list", "--account", account]
            if "kind" in params:
                if params["kind"] not in ("vs", "coop"):
                    raise BackendFailure(400, "BAD_QUERY")
                args += ["--kind", params["kind"]]
            if "analysis_set" in params:
                args += ["--analysis-set", bounded_text(params["analysis_set"], limit=64, pattern=_ANALYSIS_SET_RE)]
            if "rule" in params:
                args += ["--rule", bounded_text(params["rule"], limit=64, pattern=_RULE_RE)]
            played_from = None
            played_to = None
            if "played_from" in params:
                played_from = bounded_text(params["played_from"], limit=20, pattern=_PLAYED_RE)
                args += ["--played-from", played_from]
            if "played_to" in params:
                played_to = bounded_text(params["played_to"], limit=20, pattern=_PLAYED_RE)
                args += ["--played-to", played_to]
            if played_from is not None and played_to is not None and played_from > played_to:
                raise BackendFailure(400, "BAD_QUERY")
            if "weapon" in params:
                args += ["--weapon", bounded_text(params["weapon"])]
            if "tag" in params:
                args += ["--tag", bounded_text(params["tag"])]
            if "q" in params:
                args += ["--q", bounded_text(params["q"])]
            limit = decimal(params.get("limit", "50"), lower=1, upper=200)
            offset = decimal(params.get("offset", "0"), lower=0)
            args += ["--limit", str(limit), "--offset", str(offset)]
            return self._backend("records", args, self._dataset(params))
        if path == "/api/record":
            params = parse_query(query, {"account", "kind", "match_key", "dataset"},
                                 {"account", "kind", "match_key"})
            account = identifier(params["account"])
            match_key = identifier(params["match_key"])
            kind = params["kind"]
            if kind not in ("vs", "coop"):
                raise BackendFailure(400, "BAD_QUERY")
            result = self._backend("records", ["get", "--account", account,
                                                "--kind", kind, "--match-key", match_key],
                                   self._dataset(params))
            if result is None:
                raise BackendFailure(404, "RECORD_NOT_FOUND")
            return result
        raise BackendFailure(404, "NOT_FOUND")

    def _authorized(self):
        auth = self.headers.get_all("Authorization", [])
        if len(auth) != 1 or not auth[0].startswith("Bearer ") or not hmac.compare_digest(
            auth[0][7:].encode("utf-8"), self.server.token.encode("ascii")
        ):
            raise BackendFailure(401, "UNAUTHORIZED")

    def _post(self):
        self._validate_request()
        target = urlsplit(self.path)
        if target.scheme or target.netloc or target.fragment or target.query:
            raise BackendFailure(400, "BAD_REQUEST")
        if target.path != "/api/tags":
            raise BackendFailure(405, "METHOD_NOT_ALLOWED")
        expected = f"http://127.0.0.1:{self.server.server_port}"
        if self.headers.get_all("Origin", []) != [expected]:
            raise BackendFailure(403, "FORBIDDEN")
        self._authorized()
        lengths = self.headers.get_all("Content-Length", [])
        if len(lengths) != 1 or not DECIMAL_RE.fullmatch(lengths[0]):
            raise BackendFailure(400, "BAD_REQUEST")
        length = int(lengths[0])
        if length < 2 or length > MAX_TAG_BODY:
            raise BackendFailure(400, "BAD_REQUEST")
        types = self.headers.get_all("Content-Type", [])
        if len(types) != 1 or types[0].split(";", 1)[0].strip().lower() != "application/json":
            raise BackendFailure(400, "BAD_REQUEST")
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise BackendFailure(400, "BAD_REQUEST")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise BackendFailure(400, "BAD_QUERY") from exc
        if (not isinstance(payload, dict) or set(payload) - {"account", "match_key", "action", "tag", "note"}
                or not {"account", "match_key", "action", "tag"} <= set(payload)):
            raise BackendFailure(400, "BAD_QUERY")
        if not isinstance(payload["account"], str) or not isinstance(payload["match_key"], str):
            raise BackendFailure(400, "BAD_QUERY")
        if payload["action"] not in ("add", "remove"):
            raise BackendFailure(400, "BAD_QUERY")
        account = identifier(payload["account"])
        match_key = identifier(payload["match_key"])
        tag = parse_tag_text(payload["tag"])
        note = None
        if "note" in payload and payload["note"] is not None:
            if not isinstance(payload["note"], str):
                raise BackendFailure(400, "BAD_TAG")
            note = payload["note"].strip()
            if len(note) > 200 or any(ord(ch) < 32 or ord(ch) == 127 for ch in note):
                raise BackendFailure(400, "BAD_TAG")
            if not note:
                note = None
        current = self._backend("records", ["tag-target", "--account", account, "--match-key", match_key])
        if current is None:
            raise BackendFailure(404, "MATCH_NOT_FOUND")
        if not isinstance(current, dict) or not gui_tag_allowed(current.get("items")):
            raise BackendFailure(403, "TAG_GENRE_NOT_ALLOWED")
        command = [payload["action"], "--account", account, "--match-key", match_key, "--tag", tag]
        if payload["action"] == "add" and note is not None:
            command += ["--note", note]
        self._backend("tag", command)
        refreshed = self._backend("records", ["tag-target", "--account", account, "--match-key", match_key])
        if not isinstance(refreshed, dict) or not isinstance(refreshed.get("tags"), list):
            raise BackendFailure(502, "BACKEND_FAILED")
        self._json(200, {"match_key": match_key, "action": payload["action"], "tags": refreshed["tags"]})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--no-open", action="store_true")
    args = parser.parse_args(argv)
    if args.port < 0 or args.port > 65535:
        parser.error("--port must be between 0 and 65535")
    if args.db is not None:
        db_path = args.db.expanduser().resolve(strict=True)
        if not db_path.is_file():
            parser.error("--db must name an existing file")
        marker = None
    else:
        if __package__:
            from . import nas_archive
        else:
            import nas_archive
        marker = nas_archive.read_marker()
        db_path = None
    token = secrets.token_urlsafe(32)
    build_command = backend_builder(db_path, marker)
    with DesktopHTTPServer(("127.0.0.1", args.port), token, build_command) as server:
        url = f"http://127.0.0.1:{server.server_port}/#token={token}"
        if args.no_open:
            print(url, flush=True)
        else:
            print(f"閲覧サーバー起動: 127.0.0.1:{server.server_port}", flush=True)
            webbrowser.open(url)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
