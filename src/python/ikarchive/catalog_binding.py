"""Bind a stored response to the Relay operation definition that issued it.

This module is deliberately independent of Store projection. It performs only
SELECTs against the supplied connection and never treats the newest manifest as
the origin of an older response.
"""

from dataclasses import dataclass
import copy
from functools import lru_cache
import hashlib
import json
import sqlite3
from typing import Optional

from .planner import Planner


class _DuplicateJSONKey(ValueError):
    pass


@dataclass(frozen=True)
class ResponsePlannerBinding:
    """Immutable resolution result.

    ``status`` is one of ``legacy_unchecked``, ``known_current``,
    ``known_saved``, ``ambiguous``, ``unresolved``, ``catalog_corrupt``, or
    ``catalog_invalid``. A legacy response carries the supplied current planner
    for compatibility, but the status explicitly says its origin is unverified.
    """

    status: str
    planner: Optional[Planner]
    manifest_sha: Optional[str]


@dataclass(frozen=True)
class _Candidate:
    manifest_sha: str
    definition: str
    manifest: dict
    canonical_manifest: str


def _field(record, key, default=None, index=None):
    try:
        return record[key]
    except (KeyError, IndexError, TypeError):
        if index is not None:
            try:
                return record[index]
            except (IndexError, TypeError):
                pass
        return default


def _json_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(',', ':'),
        sort_keys=True,
    ).encode('utf-8')


def _definition(manifest, operation):
    """Return canonical selected-operation definition or ``None`` if invalid."""
    if not isinstance(manifest, dict):
        return None
    queries = manifest.get('queries')
    if not isinstance(queries, dict):
        return None
    entry = queries.get(operation)
    if not isinstance(entry, dict):
        return None
    params = entry.get('params')
    operation_doc = entry.get('operation')
    if not isinstance(params, dict) or not isinstance(operation_doc, dict):
        return None
    operation_kind = params.get('operationKind')
    argument_definitions = operation_doc.get('argumentDefinitions')
    selections = operation_doc.get('selections')
    if (not isinstance(operation_kind, str)
            or not isinstance(argument_definitions, list)
            or not isinstance(selections, list)):
        return None
    try:
        return _json_bytes({
            'operationKind': operation_kind,
            'argumentDefinitions': argument_definitions,
            'selections': selections,
        }).decode('utf-8')
    except (TypeError, ValueError, UnicodeError):
        return None


def _matches(manifest, operation, query_id, app_version):
    if not isinstance(manifest, dict):
        return False
    queries = manifest.get('queries')
    if not isinstance(queries, dict):
        return False
    entry = queries.get(operation)
    if not isinstance(entry, dict):
        return False
    params = entry.get('params')
    if not isinstance(params, dict) or params.get('id') != query_id:
        return False
    return app_version is None or manifest.get('version') == app_version


def _parse_manifest(text):
    def reject_constant(_value):
        raise ValueError('nonstandard JSON constant')

    def reject_duplicate_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise _DuplicateJSONKey('duplicate JSON object key')
            result[key] = value
        return result

    return json.loads(
        text,
        parse_constant=reject_constant,
        object_pairs_hook=reject_duplicate_keys,
    )


@lru_cache(maxsize=16)
def _cached_saved_manifest(stored_sha, raw):
    """Cache only immutable lookup results, keyed by SHA and the exact bytes."""
    sha_valid = hashlib.sha256(raw).hexdigest() == stored_sha
    try:
        text = raw.decode('utf-8')
        manifest = _parse_manifest(text)
    except _DuplicateJSONKey:
        return sha_valid, 'duplicate', None, None
    except (UnicodeError, ValueError, TypeError):
        return sha_valid, 'invalid', None, None
    try:
        canonical_manifest = _json_bytes(manifest).decode('utf-8')
    except (TypeError, ValueError, UnicodeError):
        return sha_valid, 'invalid', None, None
    return sha_valid, 'valid', manifest, canonical_manifest


def _current_candidate(current_planner, operation, query_id, app_version):
    manifest = getattr(current_planner, 'manifest', None)
    if not _matches(manifest, operation, query_id, app_version):
        return None, None
    definition = _definition(manifest, operation)
    if definition is None:
        return None, 'catalog_invalid'
    try:
        manifest_text = _json_bytes(manifest).decode('utf-8')
    except (TypeError, ValueError, UnicodeError):
        return None, 'catalog_invalid'
    return (definition, manifest_text), None


def _read_saved_candidates(connection, operation, query_id, app_version):
    """Return verified candidates and conservative catalog-integrity status."""
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='manifests'"
        ).fetchone()
    except sqlite3.Error:
        return [], 'catalog_invalid', []
    if table is None:
        return [], None, []

    try:
        rows = connection.execute(
            'SELECT sha256,json_text FROM manifests'
        ).fetchall()
    except sqlite3.Error:
        return [], 'catalog_invalid', []

    candidates = []
    exact_rows = []
    corrupt_match = False
    corrupt_unknown = False
    invalid_match = False
    invalid_unknown = False
    duplicate_unknown = False

    normalized_rows = []
    for row in rows:
        stored_sha = _field(row, 'sha256', index=0)
        text_value = _field(row, 'json_text', index=1)
        if isinstance(text_value, str):
            try:
                raw = text_value.encode('utf-8')
            except UnicodeError:
                raw = None
        elif isinstance(text_value, bytes):
            raw = text_value
        else:
            raw = None

        if raw is None:
            normalized_rows.append((str(stored_sha or ''), None, False, 'invalid', None, None))
            continue
        cache_sha = stored_sha if isinstance(stored_sha, str) else ''
        sha_valid, parse_status, manifest, canonical_manifest = _cached_saved_manifest(cache_sha, raw)
        normalized_rows.append((cache_sha, raw, sha_valid, parse_status, manifest, canonical_manifest))

    for stored_sha, raw, sha_valid, parse_status, manifest, canonical_manifest in sorted(normalized_rows, key=lambda row: row[0]):
        if parse_status != 'valid':
            if parse_status == 'duplicate':
                duplicate_unknown = True
            elif sha_valid:
                invalid_unknown = True
            else:
                corrupt_unknown = True
            continue

        if not _matches(manifest, operation, query_id, app_version):
            continue
        if not sha_valid:
            corrupt_match = True
            continue

        definition = _definition(manifest, operation)
        if definition is None:
            invalid_match = True
            continue
        if not isinstance(stored_sha, str) or not stored_sha:
            invalid_match = True
            continue
        candidate = _Candidate(stored_sha, definition, manifest, canonical_manifest)
        candidates.append(candidate)
        exact_rows.append((stored_sha, canonical_manifest))

    if duplicate_unknown:
        return [], 'catalog_invalid', exact_rows
    if corrupt_match or corrupt_unknown:
        return [], 'catalog_corrupt', exact_rows
    if invalid_match or invalid_unknown:
        return [], 'catalog_invalid', exact_rows
    return candidates, None, exact_rows


def resolve_response_planner(connection, response, current_planner):
    """Resolve the operation schema that corresponds to a stored response.

    The database connection is used for SELECT statements only. For responses
    without a query id, the supplied planner is returned with the explicit
    ``legacy_unchecked`` status. For identified responses, current and saved
    manifest candidates are matched by operation, exact query id, and—when
    present—exact app version. Differing selected-operation definitions are
    ambiguous; catalog timestamps are not used to choose an origin.
    """
    operation = _field(response, 'operation')
    query_id = _field(response, 'query_id')
    app_version = _field(response, 'app_version')
    if query_id is None:
        return ResponsePlannerBinding('legacy_unchecked', current_planner, None)
    if (not isinstance(operation, str) or not isinstance(query_id, str)
            or not query_id
            or (app_version is not None and not isinstance(app_version, str))):
        return ResponsePlannerBinding('unresolved', None, None)

    current, current_error = _current_candidate(
        current_planner, operation, query_id, app_version,
    )
    saved, saved_error, exact_rows = _read_saved_candidates(
        connection, operation, query_id, app_version,
    )

    errors = [error for error in (current_error, saved_error) if error]
    if 'catalog_corrupt' in errors:
        return ResponsePlannerBinding('catalog_corrupt', None, None)
    if 'catalog_invalid' in errors:
        return ResponsePlannerBinding('catalog_invalid', None, None)

    current_definition = current[0] if current else None
    definitions = {candidate.definition for candidate in saved}
    if current_definition is not None:
        definitions.add(current_definition)
    if len(definitions) > 1:
        return ResponsePlannerBinding('ambiguous', None, None)
    if not definitions:
        return ResponsePlannerBinding('unresolved', None, None)

    if current is not None:
        current_text = current[1]
        saved_sha = next(
            (sha for sha, text in exact_rows if text == current_text),
            None,
        )
        return ResponsePlannerBinding('known_current', current_planner, saved_sha)

    chosen = min(saved, key=lambda candidate: candidate.manifest_sha)
    try:
        planner = Planner(copy.deepcopy(chosen.manifest))
    except Exception:
        return ResponsePlannerBinding('catalog_invalid', None, None)
    return ResponsePlannerBinding('known_saved', planner, chosen.manifest_sha)
