#!/usr/bin/env python3
"""Exercise catalog-origin safeguards with the real Store and Planner.

The fixture uses only Python's standard library plus the code under
``--code-root``. It creates disposable stores in a private temporary directory
and never contacts a live API, NAS, Drive, or other network service.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import pathlib
import stat
import sys
import tempfile
from contextlib import ExitStack
from typing import Any


ACCOUNT = "catalog-fixture-account"
OLD_VERSION = "fixture-app-old"
NEW_VERSION = "fixture-app-new"
OLD_DETAIL_QUERY_ID = "fixture-detail-qid-old"
NEW_DETAIL_QUERY_ID = "fixture-detail-qid-new"
OLD_ROOT_QUERY_ID = "fixture-root-qid-old"
NEW_ROOT_QUERY_ID = "fixture-root-qid-new"


class FixtureFailure(RuntimeError):
    """A stable, non-sensitive fixture check failure."""


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise FixtureFailure(code)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _source_code_sha(code_root: pathlib.Path) -> str:
    relatives = (
        "src/python/ikarchive/catalog_binding.py",
        "src/python/ikarchive/classify.py",
        "src/python/ikarchive/planner.py",
        "src/python/ikarchive/rates.py",
        "src/python/ikarchive/store.py",
        "sql/schema.sql",
    )
    digest = hashlib.sha256()
    for relative in relatives:
        path = code_root / relative
        if path.is_symlink() or not path.is_file():
            raise FixtureFailure("CODE_INPUT_MISSING_OR_UNSAFE")
        raw = path.read_bytes()
        digest.update(relative.encode("ascii"))
        digest.update(b"\0")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.hexdigest()


def _scalar(name: str) -> dict[str, Any]:
    return {"kind": "ScalarField", "name": name, "alias": None}


def _linked(
    name: str,
    selections: list[dict[str, Any]],
    *,
    concrete: str,
    plural: bool = False,
    args: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    field: dict[str, Any] = {
        "kind": "LinkedField",
        "name": name,
        "alias": None,
        "plural": plural,
        "concreteType": concrete,
        "selections": selections,
    }
    if args is not None:
        field["args"] = args
    return field


def _operation(
    query_id: str,
    selections: list[dict[str, Any]],
    argument_definitions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "params": {"operationKind": "query", "id": query_id},
        "operation": {
            "argumentDefinitions": argument_definitions or [],
            "selections": selections,
        },
    }


def _detail_operation(query_id: str, *, current: bool) -> dict[str, Any]:
    selections = [_scalar("id"), _scalar("oldValue")]
    if current:
        selections.append(_scalar("newRequired"))
    return _operation(
        query_id,
        [
            _linked(
                "vsHistoryDetail",
                selections,
                concrete="VsHistoryDetail",
                args=[{
                    "kind": "Variable",
                    "name": "id",
                    "variableName": "vsResultId",
                }],
            )
        ],
        [{"name": "vsResultId", "defaultValue": None}],
    )


def _root_operation(query_id: str, *, current: bool) -> dict[str, Any]:
    selections = [
        _linked(
            "pageResults",
            [
                _linked(
                    "pageInfo",
                    [_scalar("hasNextPage"), _scalar("endCursor")],
                    concrete="PageInfo",
                ),
                _linked("nodes", [_scalar("id")], concrete="Node", plural=True),
            ],
            concrete="PageResults",
            args=[{
                "kind": "Variable",
                "name": "after",
                "variableName": "after",
            }],
        )
    ]
    if current:
        selections.append(_scalar("currentOnly"))
    return _operation(
        query_id,
        selections,
        [{"name": "after", "defaultValue": None}],
    )


def _manifest(version: str, *, current: bool) -> dict[str, Any]:
    return {
        "version": version,
        "queries": {
            "VsHistoryDetailQuery": _detail_operation(
                NEW_DETAIL_QUERY_ID if current else OLD_DETAIL_QUERY_ID,
                current=current,
            ),
            "RootQuery": _root_operation(
                NEW_ROOT_QUERY_ID if current else OLD_ROOT_QUERY_ID,
                current=current,
            ),
        },
    }


def _manifest_text(manifest: dict[str, Any]) -> str:
    return json.dumps(
        manifest,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _save_manifest(store: Any, manifest: dict[str, Any]) -> tuple[str, str]:
    text = _manifest_text(manifest)
    sha = _sha256(text.encode("utf-8"))
    with store.db:
        store.db.execute(
            "INSERT INTO manifests(sha256,fetched_at,json_text) VALUES(?,?,?)",
            (sha, "2026-10-03T00:00:00Z", text),
        )
    return sha, text


def _save_manifest_raw(
    store: Any,
    raw: bytes,
    *,
    sha_override: str | None = None,
) -> str:
    text = raw.decode("utf-8")
    sha = sha_override or _sha256(raw)
    with store.db:
        store.db.execute(
            "INSERT INTO manifests(sha256,fetched_at,json_text) VALUES(?,?,?)",
            (sha, "2026-10-03T00:01:00Z", text),
        )
    return sha


def _open_store(path: pathlib.Path, stack: ExitStack) -> Any:
    from ikarchive.store import Store

    store = Store(path)
    stack.callback(store.close)
    _require(stat.S_IMODE(path.stat().st_mode) == 0o600, "STORE_FILE_NOT_PRIVATE")
    _require(stat.S_IMODE(path.parent.stat().st_mode) == 0o700, "STORE_DIRECTORY_NOT_PRIVATE")
    return store


def _record(
    store: Any,
    *,
    operation: str,
    query_id: str,
    app_version: str,
    raw: bytes,
    fetched_at: str,
    variables: dict[str, Any],
    event_id: str,
) -> tuple[int, str]:
    event = {
        "event_id": event_id,
        "account": ACCOUNT,
        "fetched_at": fetched_at,
        "operation": operation,
        "variables": variables,
        "query_id": query_id,
        "app_version": app_version,
        "status": 200,
        "headers": {"x-fixture": "catalog-origin"},
        "body_base64": base64.b64encode(raw).decode("ascii"),
    }
    response_id = store.record(event)
    return response_id, event_id


def _body(store: Any, response_id: int) -> bytes:
    row = store.db.execute(
        "SELECT b.body FROM responses r JOIN bodies b ON b.sha256=r.body_sha256 WHERE r.id=?",
        (response_id,),
    ).fetchone()
    _require(row is not None, "RESPONSE_BODY_MISSING")
    return bytes(row[0])


def _assert_raw_and_origin(
    store: Any,
    response_id: int,
    *,
    raw: bytes,
    query_id: str,
    app_version: str,
) -> None:
    row = store.db.execute(
        "SELECT query_id,app_version,body_sha256 FROM responses WHERE id=?",
        (response_id,),
    ).fetchone()
    _require(row is not None, "RESPONSE_ORIGIN_MISSING")
    _require(row["query_id"] == query_id, "RESPONSE_QUERY_ORIGIN_CHANGED")
    _require(row["app_version"] == app_version, "RESPONSE_VERSION_ORIGIN_CHANGED")
    stored = _body(store, response_id)
    _require(stored == raw, "RAW_RESPONSE_BYTES_CHANGED")
    _require(row["body_sha256"] == _sha256(raw), "RAW_RESPONSE_SHA_CHANGED")


def _response_issue(store: Any, response_id: int, code: str) -> dict[str, Any] | None:
    row = store.db.execute(
        "SELECT context FROM issues WHERE response_id=? AND code=? ORDER BY id DESC LIMIT 1",
        (response_id, code),
    ).fetchone()
    return json.loads(row[0]) if row else None


def _detail_body(remote_id: str, *, current: bool) -> bytes:
    extra = b',"newRequired":"present"' if current else b""
    return (
        b'{ "data" : { "vsHistoryDetail" : { "id" : "'
        + remote_id.encode("ascii")
        + b'", "oldValue" : "complete-old-value"'
        + extra
        + b',"int64":9223372036854775807,"decimal":1.2300e+04,'
        + b'"nul":"\\u0000","astral":"'
        + "\U00010000".encode("utf-8")
        + b'","unknownField":{"retained":true} } } }\n'
    )


def _root_body() -> bytes:
    return (
        b'{"data":{"pageResults":{"pageInfo":{"hasNextPage":true,'
        b'"endCursor":"fixture-next-cursor"},"nodes":[]}}}\n'
    )


def _store_body_checks(raw: bytes) -> None:
    for required in (
        b"9223372036854775807",
        b"1.2300e+04",
        b'"nul":"\\u0000"',
        "\U00010000".encode("utf-8"),
        b'"unknownField"',
    ):
        _require(required in raw, "RAW_FIXTURE_CASE_MISSING")


def _run_primary_fixture(root: pathlib.Path, stack: ExitStack) -> None:
    from ikarchive.planner import Planner

    store = _open_store(root / "primary.sqlite", stack)
    old_manifest = _manifest(OLD_VERSION, current=False)
    current_manifest = _manifest(NEW_VERSION, current=True)
    _save_manifest(store, old_manifest)
    _save_manifest(store, current_manifest)
    current_planner = Planner(current_manifest)

    remote_id = base64.b64encode(
        b"VsHistoryDetail-u-catalog-fixture:REGULAR:20261003T000001_fixture"
    ).decode("ascii")
    detail_variables = {"vsResultId": remote_id}
    old_detail = _detail_body(remote_id, current=False)
    current_detail = _detail_body(remote_id, current=True)
    _store_body_checks(old_detail)
    _store_body_checks(current_detail)

    old_detail_id, _ = _record(
        store,
        operation="VsHistoryDetailQuery",
        query_id=OLD_DETAIL_QUERY_ID,
        app_version=OLD_VERSION,
        raw=old_detail,
        fetched_at="2026-10-03T00:02:00Z",
        variables=detail_variables,
        event_id="fixture-old-detail-original",
    )
    store.project(old_detail_id, current_planner)
    _assert_raw_and_origin(
        store,
        old_detail_id,
        raw=old_detail,
        query_id=OLD_DETAIL_QUERY_ID,
        app_version=OLD_VERSION,
    )
    old_detail_issue = _response_issue(
        store, old_detail_id, "RESPONSE_CATALOG_MISMATCH",
    )
    _require(
        old_detail_issue == {
            "binding_status": "known_saved",
            "operation": "VsHistoryDetailQuery",
        },
        "OLD_DETAIL_ORIGIN_NOT_REJECTED",
    )
    _require(
        store.db.execute(
            "SELECT count(*) FROM issues WHERE response_id=? AND code='SELECTED_FIELD_MISSING'",
            (old_detail_id,),
        ).fetchone()[0] == 0,
        "OLD_DETAIL_WAS_CHECKED_WITH_CURRENT_FIELDS",
    )
    old_match = store.db.execute(
        "SELECT detail_response_id FROM matches WHERE account=? AND kind='vs'",
        (ACCOUNT,),
    ).fetchone()
    _require(old_match is not None and old_match[0] is None, "OLD_DETAIL_BECAME_CANONICAL")
    _require(
        store.db.execute(
            "SELECT count(*) FROM endpoint_heads WHERE account=? AND operation='VsHistoryDetailQuery'",
            (ACCOUNT,),
        ).fetchone()[0] == 0,
        "OLD_DETAIL_ADVANCED_HEAD",
    )

    store.queue(ACCOUNT, "RootQuery", {"after": None})
    old_root = _root_body()
    old_root_id, _ = _record(
        store,
        operation="RootQuery",
        query_id=OLD_ROOT_QUERY_ID,
        app_version=OLD_VERSION,
        raw=old_root,
        fetched_at="2026-10-03T00:03:00Z",
        variables={"after": None},
        event_id="fixture-old-root-page",
    )
    store.project(old_root_id, current_planner)
    _assert_raw_and_origin(
        store,
        old_root_id,
        raw=old_root,
        query_id=OLD_ROOT_QUERY_ID,
        app_version=OLD_VERSION,
    )
    old_root_issue = _response_issue(
        store, old_root_id, "RESPONSE_CATALOG_MISMATCH",
    )
    _require(
        old_root_issue == {"binding_status": "known_saved", "operation": "RootQuery"},
        "OLD_ROOT_ORIGIN_NOT_REJECTED",
    )
    _require(
        store.db.execute(
            "SELECT count(*) FROM issues WHERE response_id=? AND code='SELECTED_FIELD_MISSING'",
            (old_root_id,),
        ).fetchone()[0] == 0,
        "OLD_ROOT_WAS_CHECKED_WITH_CURRENT_FIELDS",
    )
    root_jobs = store.db.execute(
        "SELECT variables_json,state FROM jobs WHERE account=? AND operation='RootQuery' ORDER BY variables_json",
        (ACCOUNT,),
    ).fetchall()
    _require(
        len(root_jobs) == 1
        and root_jobs[0]["variables_json"] == '{"after":null}'
        and root_jobs[0]["state"] == "retry",
        "OLD_ROOT_PAGE_CONTINUATION_WAS_QUEUED",
    )
    _require(
        store.db.execute(
            "SELECT count(*) FROM endpoint_heads WHERE account=? AND operation='RootQuery'",
            (ACCOUNT,),
        ).fetchone()[0] == 0,
        "OLD_ROOT_ADVANCED_HEAD",
    )

    current_id, _ = _record(
        store,
        operation="VsHistoryDetailQuery",
        query_id=NEW_DETAIL_QUERY_ID,
        app_version=NEW_VERSION,
        raw=current_detail,
        fetched_at="2026-10-03T00:04:00Z",
        variables=detail_variables,
        event_id="fixture-current-detail",
    )
    store.project(current_id, current_planner)
    _assert_raw_and_origin(
        store,
        current_id,
        raw=current_detail,
        query_id=NEW_DETAIL_QUERY_ID,
        app_version=NEW_VERSION,
    )
    _require(
        store.db.execute(
            "SELECT state FROM jobs WHERE account=? AND operation='VsHistoryDetailQuery' AND variables_json=?",
            (ACCOUNT, json.dumps(detail_variables, ensure_ascii=False, separators=(",", ":"), sort_keys=True)),
        ).fetchone()[0] == "done",
        "CURRENT_DETAIL_DID_NOT_COMPLETE",
    )
    _require(
        store.db.execute(
            "SELECT detail_response_id FROM matches WHERE account=? AND kind='vs'",
            (ACCOUNT,),
        ).fetchone()[0] == current_id,
        "CURRENT_DETAIL_NOT_CANONICAL",
    )
    _require(
        store.db.execute(
            "SELECT response_id FROM endpoint_heads WHERE account=? AND operation='VsHistoryDetailQuery'",
            (ACCOUNT,),
        ).fetchone()[0] == current_id,
        "CURRENT_DETAIL_HEAD_NOT_ADVANCED",
    )
    _require(
        _response_issue(store, current_id, "RESPONSE_CATALOG_MISMATCH") is None
        and _response_issue(store, current_id, "RESPONSE_CATALOG_UNRESOLVED") is None,
        "CURRENT_DETAIL_WRONGLY_REJECTED",
    )

    duplicate_id, duplicate_event_id = _record(
        store,
        operation="VsHistoryDetailQuery",
        query_id=OLD_DETAIL_QUERY_ID,
        app_version=OLD_VERSION,
        raw=old_detail,
        fetched_at="2026-10-03T00:05:00Z",
        variables=detail_variables,
        event_id="fixture-old-detail-late-duplicate",
    )
    _require(duplicate_id == old_detail_id, "OLD_DUPLICATE_DID_NOT_REUSE_RESPONSE")
    store.project(duplicate_id, current_planner)
    _assert_raw_and_origin(
        store,
        duplicate_id,
        raw=old_detail,
        query_id=OLD_DETAIL_QUERY_ID,
        app_version=OLD_VERSION,
    )
    _require(
        store.db.execute(
            "SELECT acknowledged FROM response_fetches WHERE event_id=?",
            (duplicate_event_id,),
        ).fetchone()[0] == 1,
        "OLD_DUPLICATE_RECEIPT_NOT_ACKNOWLEDGED",
    )
    _require(
        store.db.execute(
            "SELECT state FROM jobs WHERE account=? AND operation='VsHistoryDetailQuery' AND variables_json=?",
            (ACCOUNT, json.dumps(detail_variables, ensure_ascii=False, separators=(",", ":"), sort_keys=True)),
        ).fetchone()[0] == "retry",
        "OLD_DUPLICATE_RECEIPT_DID_NOT_RETRY_JOB",
    )
    _require(
        store.db.execute(
            "SELECT detail_response_id FROM matches WHERE account=? AND kind='vs'",
            (ACCOUNT,),
        ).fetchone()[0] == current_id,
        "OLD_DUPLICATE_OVERWROTE_CANONICAL",
    )
    _require(
        store.db.execute(
            "SELECT response_id FROM endpoint_heads WHERE account=? AND operation='VsHistoryDetailQuery'",
            (ACCOUNT,),
        ).fetchone()[0] == current_id,
        "OLD_DUPLICATE_OVERWROTE_HEAD",
    )
    _require(not store.verify(), "PRIMARY_STORE_INTEGRITY_FAILED")


def _duplicate_version_manifest(manifest: dict[str, Any]) -> bytes:
    version = json.dumps(manifest["version"], ensure_ascii=False, separators=(",", ":"))
    queries = json.dumps(
        manifest["queries"],
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return (
        "{\"version\":" + version
        + ",\"version\":" + version
        + ",\"queries\":" + queries + "}"
    ).encode("utf-8")


def _run_negative_manifest_fixture(
    root: pathlib.Path,
    stack: ExitStack,
    *,
    duplicate_key: bool,
) -> None:
    from ikarchive.planner import Planner

    suffix = "duplicate" if duplicate_key else "corrupt"
    store = _open_store(root / f"{suffix}.sqlite", stack)
    current_manifest = _manifest(NEW_VERSION, current=True)
    _, current_text = _save_manifest(store, current_manifest)
    current_raw = current_text.encode("utf-8")
    if duplicate_key:
        duplicate_raw = _duplicate_version_manifest(current_manifest)
        _require(duplicate_raw.count(b'"version"') == 2, "DUPLICATE_FIXTURE_NOT_DUPLICATED")
        _save_manifest_raw(store, duplicate_raw)
        expected_status = "catalog_invalid"
    else:
        wrong_sha = "0" * 64
        if wrong_sha == _sha256(current_raw):
            wrong_sha = "1" * 64
        _save_manifest_raw(store, current_raw, sha_override=wrong_sha)
        expected_status = "catalog_corrupt"

    current_planner = Planner(current_manifest)
    remote_id = base64.b64encode(
        b"VsHistoryDetail-u-catalog-fixture:REGULAR:20261003T000002_negative"
    ).decode("ascii")
    raw = _detail_body(remote_id, current=True)
    response_id, _ = _record(
        store,
        operation="VsHistoryDetailQuery",
        query_id=NEW_DETAIL_QUERY_ID,
        app_version=NEW_VERSION,
        raw=raw,
        fetched_at="2026-10-03T00:06:00Z",
        variables={"vsResultId": remote_id},
        event_id=f"fixture-{suffix}-manifest-response",
    )
    store.project(response_id, current_planner)
    _assert_raw_and_origin(
        store,
        response_id,
        raw=raw,
        query_id=NEW_DETAIL_QUERY_ID,
        app_version=NEW_VERSION,
    )
    issue = _response_issue(store, response_id, "RESPONSE_CATALOG_UNRESOLVED")
    _require(
        issue == {
            "binding_status": expected_status,
            "operation": "VsHistoryDetailQuery",
        },
        f"{suffix.upper()}_MANIFEST_WAS_NOT_REFUSED",
    )
    match = store.db.execute(
        "SELECT detail_response_id FROM matches WHERE account=? AND kind='vs'",
        (ACCOUNT,),
    ).fetchone()
    _require(match is not None and match[0] is None, f"{suffix.upper()}_MANIFEST_BECAME_CANONICAL")
    _require(
        store.db.execute(
            "SELECT count(*) FROM endpoint_heads WHERE account=? AND operation='VsHistoryDetailQuery'",
            (ACCOUNT,),
        ).fetchone()[0] == 0,
        f"{suffix.upper()}_MANIFEST_ADVANCED_HEAD",
    )
    _require(not store.verify(), f"{suffix.upper()}_STORE_INTEGRITY_FAILED")


def _run(code_root: pathlib.Path) -> dict[str, Any]:
    supplied_root = pathlib.Path(code_root)
    if supplied_root.is_symlink():
        raise FixtureFailure("CODE_ROOT_INVALID")
    code_root = supplied_root.resolve(strict=True)
    if not code_root.is_dir() or code_root.is_symlink():
        raise FixtureFailure("CODE_ROOT_INVALID")
    source_path = code_root / "src/python"
    if source_path.is_symlink() or not source_path.is_dir():
        raise FixtureFailure("SOURCE_ROOT_INVALID")
    sys.path.insert(0, str(source_path))
    code_sha = _source_code_sha(code_root)

    previous_umask = os.umask(0o077)
    try:
        with tempfile.TemporaryDirectory(prefix="nas-catalog-fixture-") as temporary, ExitStack() as stack:
            private_root = pathlib.Path(temporary)
            private_root.chmod(0o700)
            _require(stat.S_IMODE(private_root.stat().st_mode) == 0o700, "TEMP_DIRECTORY_NOT_PRIVATE")
            _run_primary_fixture(private_root, stack)
            _run_negative_manifest_fixture(
                private_root, stack, duplicate_key=False,
            )
            _run_negative_manifest_fixture(
                private_root, stack, duplicate_key=True,
            )
    finally:
        os.umask(previous_umask)

    return {
        "status": "ok",
        "checks": [
            "old_saved_detail_retries_without_canonical_or_head",
            "old_saved_page_response_does_not_queue_continuation",
            "current_complete_detail_updates_canonical_and_head",
            "late_old_duplicate_is_acknowledged_without_overwrite",
            "raw_body_bytes_and_sha256_preserved",
            "corrupt_saved_manifest_is_refused",
            "duplicate_key_saved_manifest_is_refused",
            "original_query_id_and_app_version_preserved",
        ],
        "counts": {
            "fixture_stores": 3,
            "unique_responses": 5,
            "recorded_fetch_events": 6,
        },
        "code_sha256": code_sha,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = _run(args.code_root)
    except FixtureFailure as exc:
        print(json.dumps({"status": "error", "failure": str(exc)}, sort_keys=True), file=sys.stderr)
        return 1
    except Exception:
        print(json.dumps({"status": "error", "failure": "UNEXPECTED_FIXTURE_FAILURE"}, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
