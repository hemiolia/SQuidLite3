#!/usr/bin/env python3
"""Exercise image-acquisition evidence on a disposable local Store.

Uses only Python's standard library and the code selected by ``--code-root``.
It does not contact a network service, NAS, or Drive. Output contains only
fixed check names, aggregate counts, and a hash of the tested source inputs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sqlite3
import stat
import sys
import tempfile
from typing import Any


class FixtureFailure(RuntimeError):
    """A stable, non-sensitive fixture failure code."""


CODE_INPUTS = (
    "archive.py",
    "src/python/ikarchive/asset_evidence.py",
    "src/python/ikarchive/store.py",
    "sql/schema.sql",
)


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise FixtureFailure(code)


def _code_sha256(code_root: pathlib.Path) -> str:
    digest = hashlib.sha256()
    for relative in CODE_INPUTS:
        path = code_root / relative
        if path.is_symlink() or not path.is_file():
            raise FixtureFailure("CODE_INPUT_MISSING_OR_UNSAFE")
        raw = path.read_bytes()
        digest.update(relative.encode("ascii"))
        digest.update(b"\0")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.hexdigest()


def _snapshot(db: sqlite3.Connection) -> dict[str, list[tuple[Any, ...]]]:
    return {
        "assets": [
            tuple(row)
            for row in db.execute(
                "SELECT url,state,body_sha256,content_type,attempts,next_attempt,last_error "
                "FROM assets ORDER BY url"
            )
        ],
        "bodies": [
            tuple(row)
            for row in db.execute(
                "SELECT sha256,body,byte_length FROM bodies ORDER BY sha256"
            )
        ],
        "asset_refs": [
            tuple(row)
            for row in db.execute(
                "SELECT response_id,url,path FROM asset_refs ORDER BY response_id,path"
            )
        ],
    }


def _run(code_root: pathlib.Path) -> dict[str, Any]:
    code_root = code_root.resolve(strict=True)
    sys.path.insert(0, str(code_root))
    sys.path.insert(0, str(code_root / "src/python"))
    from archive import audit
    from ikarchive.asset_evidence import asset_acquisition_evidence
    from ikarchive.store import Store

    code_sha = _code_sha256(code_root)
    with tempfile.TemporaryDirectory(prefix="asset-evidence-fixture-") as raw_temp:
        private_root = pathlib.Path(raw_temp)
        os.chmod(private_root, 0o700)
        _require(stat.S_IMODE(private_root.stat().st_mode) == 0o700,
                 "TEMP_DIRECTORY_NOT_PRIVATE")
        store = Store(private_root / "database" / "archive.sqlite3")
        try:
            saved_key = "fixture-private-body-key"
            saved_url = "https://fixture.example/image?secret=saved"
            expired_url = "https://FIXTURE.example:443/image?secret=expired#private"
            missing_url = "https://missing.fixture.example/image"
            with store.db:
                store.db.execute(
                    "INSERT INTO bodies(sha256,body,byte_length) VALUES(?,?,?)",
                    (saved_key, b"fixture-private-image-bytes", 27),
                )
                store.db.execute(
                    "INSERT INTO assets(url,state,body_sha256,last_error) VALUES(?,?,?,?)",
                    (saved_url, "done", saved_key, None),
                )
                store.db.execute(
                    "INSERT INTO assets(url,state,body_sha256,last_error) VALUES(?,?,?,?)",
                    (expired_url, "retry", None, "AssetUrlExpired"),
                )
                store.db.execute(
                    "INSERT INTO assets(url,state,body_sha256,last_error) VALUES(?,?,?,?)",
                    (missing_url, "done", None, None),
                )
                store.db.execute(
                    "INSERT INTO responses(event_id,account,fetched_at,operation,variables_json,"
                    "headers_json,http_status,body_sha256,json_text) "
                    "VALUES('fixture-event','fixture-account','fixture-time','fixture-op',"
                    "'{}','{}',200,?,'{}')",
                    (saved_key,),
                )
                response_id = store.db.execute(
                    "SELECT id FROM responses WHERE event_id='fixture-event'"
                ).fetchone()[0]
                store.db.executemany(
                    "INSERT INTO asset_refs(response_id,url,path) VALUES(?,?,?)",
                    [(response_id, saved_url, "fixture-path-a"),
                     (response_id, saved_url, "fixture-path-b")],
                )
            before = _snapshot(store.db)
            changes_before = store.db.total_changes
            store.db.execute("BEGIN")

            def authorizer(action: int, arg1: str | None, arg2: str | None,
                           database: str | None, source: str | None) -> int:
                if action == sqlite3.SQLITE_READ and arg1 == "asset_refs":
                    return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and arg1 == "bodies" and arg2 == "body":
                    return sqlite3.SQLITE_DENY
                if action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE,
                              sqlite3.SQLITE_DELETE):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            store.db.set_authorizer(authorizer)
            try:
                evidence = asset_acquisition_evidence(store.db)
            finally:
                store.db.set_authorizer(None)
            _require(store.db.total_changes == changes_before,
                     "AUDIT_CHANGED_DATABASE")
            _require(store.db.in_transaction, "AUDIT_ENDED_CALLER_TRANSACTION")
            after = _snapshot(store.db)
            _require(after == before, "DATABASE_ROWS_CHANGED")

            _require(evidence["asset_url_rows"] == 3, "ASSET_URL_ROW_COUNT_MISMATCH")
            _require(evidence["state_url_rows"] == {
                "pending": 0, "retry": 1, "done": 2, "other": 0,
            }, "STATE_URL_ROW_COUNTS_MISMATCH")
            _require(evidence["done_body_reference_present_url_rows"] == 1,
                     "DONE_BODY_REFERENCE_PRESENT_MISMATCH")
            _require(evidence["done_body_reference_missing_url_rows"] == 1,
                     "DONE_BODY_REFERENCE_MISSING_MISMATCH")
            _require(evidence["expired_urls_with_saved_same_address"] == 1,
                     "EXPIRED_SAME_ADDRESS_MISMATCH")
            _require(evidence["expired_url_rows"] == 1,
                     "EXPIRED_URL_ROW_COUNT_MISMATCH")

            audit_result = audit(store)
            _require("image_acquisition_evidence" in audit_result,
                     "AUDIT_EVIDENCE_KEY_MISSING")
            _require(audit_result["image_acquisition_evidence"] == evidence,
                     "AUDIT_EVIDENCE_VALUE_MISMATCH")
            serialized = json.dumps(audit_result, ensure_ascii=False)
            for private_value in (
                saved_key, saved_url, expired_url, missing_url,
                "fixture-private-image-bytes", "fixture-account", "fixture-event",
            ):
                _require(private_value not in serialized, "AUDIT_PRIVATE_VALUE_LEAK")

            store.db.rollback()
            final = _snapshot(store.db)
            _require(final == before, "ROLLBACK_CHANGED_DATABASE_ROWS")
            return {
                "status": "ok",
                "checks": [
                    "asset_refs_reads_denied",
                    "body_blob_reads_denied",
                    "caller_transaction_preserved",
                    "no_database_mutations",
                    "assets_bodies_and_refs_unchanged",
                    "url_rows_not_reference_edges",
                    "audit_evidence_key_present",
                    "private_values_not_emitted",
                ],
                "counts": {
                    "asset_url_rows": evidence["asset_url_rows"],
                    "done_body_reference_present_url_rows": evidence[
                        "done_body_reference_present_url_rows"
                    ],
                    "done_body_reference_missing_url_rows": evidence[
                        "done_body_reference_missing_url_rows"
                    ],
                    "expired_url_rows": evidence["expired_url_rows"],
                    "asset_ref_rows": len(before["asset_refs"]),
                },
                "code_sha256": code_sha,
            }
        finally:
            store.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = _run(args.code_root)
    except FixtureFailure as exc:
        print(json.dumps({"status": "error", "failure": str(exc)}, sort_keys=True),
              file=sys.stderr)
        return 1
    except Exception:
        print(json.dumps({"status": "error", "failure": "UNEXPECTED_FIXTURE_FAILURE"},
                         sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
