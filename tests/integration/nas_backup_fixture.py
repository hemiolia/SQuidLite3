#!/usr/bin/env python3
"""Exercise the real plaintext backup cycle using only a private fake remote.

Run this inside a candidate image with ``--network none``. All generated
database files, WAL sidecars, snapshots, receipts, fake remote objects, and
logs stay beneath the caller-provided private ``--work-dir``. The fixture
never deletes files and refuses to invoke a real rclone binary.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import sqlite3
import stat
import subprocess
import sys
import urllib.parse
import uuid
from typing import Any


REMOTE = "fixture:backup"
TABLE_VALUES = "fixture_values"


class FixtureError(RuntimeError):
    pass


def _sha256_file(path: pathlib.Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            total += len(chunk)
    return total, digest.hexdigest()


def _md5_file(path: pathlib.Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _typed(value: Any) -> dict[str, str]:
    if value is None:
        return {"sqlite_type": "null", "value": ""}
    if isinstance(value, bytes):
        return {"sqlite_type": "blob", "value": value.hex()}
    if isinstance(value, str):
        return {"sqlite_type": "text", "value": value}
    if isinstance(value, int):
        return {"sqlite_type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"sqlite_type": "real", "value": value.hex()}
    raise FixtureError(f"unsupported SQLite value returned by fixture: {type(value).__name__}")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _capture_database(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = [
        list(row)
        for row in conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        )
    ]
    tables: list[dict[str, Any]] = []
    names = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    for name in names:
        quoted = _quote_identifier(name)
        columns = [
            list(row)
            for row in conn.execute(f"PRAGMA table_xinfo({quoted})")
        ]
        rows = [
            [_typed(value) for value in row]
            for row in conn.execute(f"SELECT rowid, * FROM {quoted} ORDER BY rowid")
        ]
        tables.append({"name": name, "columns": columns, "rows": rows})
    return {"schema_objects": objects, "tables": tables}


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _open_readonly(path: pathlib.Path) -> sqlite3.Connection:
    uri = path.resolve(strict=True).as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    return conn


def _create_source(db_path: pathlib.Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=10, isolation_level=None)
    mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    if str(mode).lower() != "wal":
        conn.close()
        raise FixtureError(f"could not enable WAL mode (SQLite reported {mode!r})")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "CREATE TABLE fixture_values ("
        "seq INTEGER PRIMARY KEY, label TEXT NOT NULL, value)"
    )
    conn.execute("CREATE INDEX fixture_values_label_idx ON fixture_values(label)")
    conn.execute("CREATE TABLE fixture_empty (seq INTEGER PRIMARY KEY, value)")
    rows = [
        (1, "null", None),
        (2, "max-integer", 9223372036854775807),
        (3, "min-integer", -9223372036854775808),
        (4, "real", 1.0 / 3.0),
        (5, "text-nul-cr-astral", "left\x00middle\r\n𐀀"),
        (6, "empty-text", ""),
        (7, "blob-nul", b"\x00\xff\x10payload\x00"),
        (8, "empty-blob", b""),
    ]
    conn.executemany(
        "INSERT INTO fixture_values(seq, label, value) VALUES (?, ?, ?)", rows
    )
    conn.commit()
    # Materialize a read while keeping the writer connection open. Closing the
    # last connection could checkpoint/remove the WAL before the candidate runs.
    conn.execute("SELECT count(*) FROM fixture_values").fetchone()
    return conn


def _verify_type_cases(conn: sqlite3.Connection) -> None:
    actual = {
        label: sqlite_type
        for label, sqlite_type in conn.execute(
            "SELECT label, typeof(value) FROM fixture_values ORDER BY seq"
        )
    }
    expected = {
        "null": "null",
        "max-integer": "integer",
        "min-integer": "integer",
        "real": "real",
        "text-nul-cr-astral": "text",
        "empty-text": "text",
        "blob-nul": "blob",
        "empty-blob": "blob",
    }
    if actual != expected:
        raise FixtureError(f"SQLite fixture did not retain the requested native types: {actual!r}")


def _require_private_work_dir(raw_path: pathlib.Path) -> pathlib.Path:
    if not raw_path.is_absolute():
        raise FixtureError("--work-dir must be an absolute path")
    if raw_path.is_symlink():
        raise FixtureError("--work-dir must not be a symlink")
    if not raw_path.exists():
        raw_path.mkdir(mode=0o700, parents=True)
    if not raw_path.is_dir():
        raise FixtureError("--work-dir must be a directory")
    mode = stat.S_IMODE(raw_path.stat().st_mode)
    if mode & 0o077:
        raise FixtureError(f"--work-dir must be private (mode 0700 or stricter), got {mode:04o}")
    return raw_path.resolve(strict=True)


def _new_run_dir(work_root: pathlib.Path) -> pathlib.Path:
    for _ in range(8):
        path = work_root / f"nas-backup-fixture-{uuid.uuid4().hex}"
        try:
            path.mkdir(mode=0o700)
            return path
        except FileExistsError:
            continue
    raise FixtureError("could not reserve a unique fixture directory")


FAKE_RCLONE = r'''#!/usr/bin/python3
import hashlib
import json
import os
import pathlib
import shutil
import sys

root = pathlib.Path(os.environ["IKARING_FIXTURE_REMOTE_ROOT"]).resolve()
log = pathlib.Path(os.environ["IKARING_FIXTURE_RCLONE_LOG"])

def fail(message):
    sys.stderr.write("fixture fake-rclone refused operation: " + message + "\n")
    raise SystemExit(2)

def remote_path(value):
    if not value.startswith("fixture:"):
        fail("only fixture: remote paths are permitted")
    tail = value[len("fixture:"):]
    parts = pathlib.PurePosixPath(tail).parts
    if not parts or pathlib.PurePosixPath(tail).is_absolute() or any(p in (".", "..") for p in parts):
        fail("unsafe fixture remote path")
    target = root.joinpath(*parts)
    try:
        target.parent.resolve().relative_to(root)
    except ValueError:
        fail("fixture remote path escaped its private root")
    return target

def positional(values):
    result = []
    skip = False
    value_flags = {"--retries", "--low-level-retries", "--drive-chunk-size"}
    for item in values:
        if skip:
            skip = False
            continue
        if item in value_flags:
            skip = True
            continue
        if item.startswith("-"):
            continue
        result.append(item)
    if skip:
        fail("option is missing its value")
    return result

def record(argv):
    log.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with log.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(argv, ensure_ascii=True) + "\n")

argv = sys.argv[1:]
record(argv)
if not argv:
    fail("missing command")
command, *rest = argv
args = positional(rest)

if command == "lsf":
    if len(args) != 1:
        fail("lsf expected one remote directory")
    directory = remote_path(args[0])
    if directory.exists():
        for item in sorted(directory.iterdir(), key=lambda entry: entry.name):
            if item.is_file():
                print(item.name)
    raise SystemExit(0)

if command == "copyto":
    if len(args) != 2:
        fail("copyto expected source and destination")
    if "--immutable" not in rest:
        fail("copyto must request immutable destination handling")
    source = pathlib.Path(args[0]).resolve(strict=True)
    work_root = pathlib.Path(os.environ["IKARING_FIXTURE_WORK_ROOT"]).resolve()
    try:
        source.relative_to(work_root)
    except ValueError:
        fail("copyto source is outside the private work directory")
    if not source.is_file():
        fail("copyto source is not a regular file")
    destination = remote_path(args[1])
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if destination.exists():
        fail("immutable copyto destination already exists")
    with source.open("rb") as src, destination.open("xb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    raise SystemExit(0)

if command == "size":
    if len(args) != 1:
        fail("size expected one remote path")
    path = remote_path(args[0])
    if not path.is_file():
        fail("size target is absent")
    print(json.dumps({"bytes": path.stat().st_size}, separators=(",", ":")))
    raise SystemExit(0)

if command == "md5sum":
    if len(args) != 1:
        fail("md5sum expected one remote path")
    path = remote_path(args[0])
    if not path.is_file():
        fail("md5sum target is absent")
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    print(digest.hexdigest() + "  " + path.name)
    raise SystemExit(0)

if command == "cat":
    if len(args) != 1:
        fail("cat expected one remote path")
    path = remote_path(args[0])
    if not path.is_file():
        fail("cat target is absent")
    with path.open("rb") as handle:
        shutil.copyfileobj(handle, sys.stdout.buffer, length=1024 * 1024)
    raise SystemExit(0)

if command == "moveto":
    if len(args) != 2:
        fail("moveto expected source and destination")
    if "--immutable" not in rest:
        fail("moveto must request immutable destination handling")
    source = remote_path(args[0])
    destination = remote_path(args[1])
    if not source.is_file() or destination.exists():
        fail("moveto source is absent or immutable destination exists")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.replace(source, destination)
    raise SystemExit(0)

if command == "deletefile":
    if len(args) != 1:
        fail("deletefile expected one remote path")
    path = remote_path(args[0])
    if path.is_file():
        path.unlink()
    raise SystemExit(0)

fail("unsupported command " + repr(command))
'''


def _install_fake_rclone(run_dir: pathlib.Path) -> tuple[pathlib.Path, dict[str, str]]:
    bin_dir = run_dir / "fake-bin"
    bin_dir.mkdir(mode=0o700)
    executable = bin_dir / "rclone"
    script = FAKE_RCLONE.replace("#!/usr/bin/python3", f"#!{sys.executable}", 1)
    executable.write_text(script, encoding="utf-8")
    executable.chmod(0o700)

    remote_root = run_dir / "fake-remote"
    remote_root.mkdir(mode=0o700)
    log_path = run_dir / "fake-rclone-argv.jsonl"
    config_path = run_dir / "rclone.conf"
    config_path.write_text("", encoding="utf-8")
    config_path.chmod(0o600)
    home = run_dir / "home"
    tmp = run_dir / "tmp"
    gpg_home = run_dir / "gnupg"
    for path in (home, tmp, gpg_home):
        path.mkdir(mode=0o700)

    env = {
        "PATH": str(bin_dir),
        "HOME": str(home),
        "TMPDIR": str(tmp),
        "GNUPGHOME": str(gpg_home),
        "RCLONE_CONFIG": str(config_path),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
        "TZ": "UTC",
        "IKARING_FIXTURE_REMOTE_ROOT": str(remote_root),
        "IKARING_FIXTURE_RCLONE_LOG": str(log_path),
        "IKARING_FIXTURE_WORK_ROOT": str(run_dir),
    }
    return remote_root, env


def _read_remote_log(log_path: pathlib.Path) -> list[list[str]]:
    if not log_path.is_file():
        raise FixtureError("fake rclone was not invoked")
    calls = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        item = json.loads(line)
        if not isinstance(item, list) or not item or not all(isinstance(arg, str) for arg in item):
            raise FixtureError("fake rclone argv log is malformed")
        calls.append(item)
    return calls


def _run_fixture(work_root: pathlib.Path, cycle_script_arg: pathlib.Path) -> dict[str, Any]:
    cycle_script = cycle_script_arg.resolve(strict=True)
    if cycle_script_arg.is_symlink() or not cycle_script.is_file() or cycle_script.name != "nas_backup_cycle.py":
        raise FixtureError("--cycle-script must be a regular nas_backup_cycle.py file")
    support_script = cycle_script.parent / "verified_backup_support.py"
    if support_script.is_symlink() or not support_script.is_file():
        raise FixtureError("verified_backup_support.py must be a regular sibling of --cycle-script")

    run_dir = _new_run_dir(work_root)
    database_dir = run_dir / "database"
    database_dir.mkdir(mode=0o700)
    db_path = database_dir / "archive.sqlite3"
    backup_dir = run_dir / "backup"
    backup_dir.mkdir(mode=0o700)

    writer = _create_source(db_path)
    try:
        _verify_type_cases(writer)
        expected_source = _capture_database(writer)
        expected_digest = _canonical_digest(expected_source)
        wal_path = pathlib.Path(str(db_path) + "-wal")
        if not wal_path.is_file() or wal_path.stat().st_size <= 32:
            raise FixtureError("fixture source has no committed SQLite WAL frames")
        source_file_hashes_before = {
            "database": _sha256_file(db_path),
            "wal": _sha256_file(wal_path),
        }

        main_only_path = run_dir / "main-file-without-wal.sqlite3"
        shutil.copyfile(db_path, main_only_path)
        main_only = _open_readonly(main_only_path)
        try:
            bare_names = {
                row[0]
                for row in main_only.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        finally:
            main_only.close()
        if TABLE_VALUES in bare_names:
            raise FixtureError(
                "fixture's committed table is already in the main file; WAL-only condition was not established"
            )

        remote_root, child_env = _install_fake_rclone(run_dir)
        command = [
            sys.executable,
            str(cycle_script),
            "--db",
            str(db_path),
            "--backup-dir",
            str(backup_dir),
            "--remote",
            REMOTE,
        ]
        proc = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=child_env,
            check=False,
        )
        if proc.returncode != 0:
            raise FixtureError(
                "candidate backup cycle failed "
                f"(exit={proc.returncode}); stdout={proc.stdout[-2000:]!r}; stderr={proc.stderr[-4000:]!r}"
            )
        try:
            result = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise FixtureError(f"backup cycle stdout was not its JSON receipt summary: {exc}") from exc
        if not isinstance(result, dict) or result.get("status") != "ok":
            raise FixtureError(f"backup cycle returned a non-success summary: {result!r}")

        raw_paths = sorted(
            path for path in backup_dir.iterdir()
            if path.is_file() and path.name.endswith(".sqlite3")
        )
        if len(raw_paths) != 1:
            raise FixtureError(f"expected one local plaintext snapshot, found {[p.name for p in raw_paths]!r}")
        raw_path = raw_paths[0]
        manifest_path = pathlib.Path(str(raw_path) + ".manifest.json")
        if not manifest_path.is_file():
            raise FixtureError("snapshot manifest is missing")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        snapshot_metadata = manifest.get("raw_snapshot")
        if not isinstance(snapshot_metadata, dict):
            raise FixtureError("snapshot manifest has no raw_snapshot object")
        raw_bytes, raw_sha = _sha256_file(raw_path)
        if (
            manifest.get("storage") != "plaintext"
            or manifest.get("encryption") is not None
            or snapshot_metadata.get("basename") != raw_path.name
            or snapshot_metadata.get("bytes") != raw_bytes
            or snapshot_metadata.get("sha256") != raw_sha
        ):
            raise FixtureError("snapshot bytes/SHA-256 do not match its manifest")
        manifest_verification = manifest.get("verification")
        if (
            snapshot_metadata.get("quick_check") != "ok"
            or not isinstance(manifest_verification, dict)
            or manifest_verification.get("quick_check") != "ok"
            or manifest_verification.get("sha256_match") is not True
            or manifest_verification.get("plaintext") is not True
        ):
            raise FixtureError("snapshot manifest quick_check is not ok")

        snapshot = _open_readonly(raw_path)
        try:
            quick_check = snapshot.execute("PRAGMA quick_check").fetchall()
            if quick_check != [("ok",)]:
                raise FixtureError(f"snapshot quick_check failed: {quick_check!r}")
            snapshot_state = _capture_database(snapshot)
        finally:
            snapshot.close()
        snapshot_digest = _canonical_digest(snapshot_state)
        if snapshot_state != expected_source:
            raise FixtureError(
                "pinned snapshot does not preserve the WAL-backed source's full fixture schema and typed rows"
            )

        receipt_argument = pathlib.Path(result.get("receipt", ""))
        if receipt_argument.is_symlink():
            raise FixtureError("receipt path must not be a symlink")
        try:
            receipt_path = receipt_argument.resolve(strict=True)
            receipt_path.relative_to((backup_dir / "cloud-receipts").resolve(strict=True))
        except (OSError, ValueError) as exc:
            raise FixtureError("verified cloud receipt is outside the private fixture receipt directory") from exc
        if not receipt_path.is_file():
            raise FixtureError("verified cloud receipt is missing from the private fixture backup directory")
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("status") != "ok":
            raise FixtureError("cloud receipt status is not ok")
        verification = receipt.get("verification")
        required_receipt_flags = (
            "remote_listing_checked",
            "upload_size_verified",
            "upload_md5_verified",
            "roundtrip_sha256_verified",
            "manifest_roundtrip_sha256_verified",
        )
        if not isinstance(verification, dict) or any(verification.get(flag) is not True for flag in required_receipt_flags):
            raise FixtureError("receipt does not attest all listing/size/MD5/full-SHA checks")
        raw_md5 = _md5_file(raw_path)
        manifest_bytes, manifest_sha = _sha256_file(manifest_path)
        manifest_md5 = _md5_file(manifest_path)
        plain_receipt = receipt.get("plaintext_snapshot")
        manifest_receipt = receipt.get("manifest")
        if not isinstance(plain_receipt, dict) or (
            plain_receipt.get("basename") != raw_path.name
            or plain_receipt.get("bytes") != raw_bytes
            or plain_receipt.get("sha256") != raw_sha
            or plain_receipt.get("md5") != raw_md5
        ):
            raise FixtureError("receipt snapshot SHA-256 does not match the verified snapshot")
        if not isinstance(manifest_receipt, dict) or (
            manifest_receipt.get("basename") != manifest_path.name
            or manifest_receipt.get("bytes") != manifest_bytes
            or manifest_receipt.get("sha256") != manifest_sha
            or manifest_receipt.get("md5") != manifest_md5
        ):
            raise FixtureError("receipt manifest SHA-256 does not match the local manifest")
        if plain_receipt.get("remote") != f"{REMOTE}/{raw_path.name}":
            raise FixtureError("receipt points the plaintext snapshot outside the fixture remote")
        if manifest_receipt.get("remote") != f"{REMOTE}/{manifest_path.name}":
            raise FixtureError("receipt points the manifest outside the fixture remote")

        remote_dir = remote_root / "backup"
        expected_remote_names = {raw_path.name, manifest_path.name}
        remote_items = list(remote_dir.iterdir())
        if any(path.is_symlink() or not path.is_file() for path in remote_items):
            raise FixtureError("fake remote contains a symlink or non-file artifact")
        actual_remote_names = {path.name for path in remote_items}
        if actual_remote_names != expected_remote_names:
            raise FixtureError(
                f"fake remote contains an unexpected inventory: {sorted(actual_remote_names)!r}"
            )
        for local_path in (raw_path, manifest_path):
            remote_path = remote_dir / local_path.name
            local_bytes, local_sha = _sha256_file(local_path)
            remote_bytes, remote_sha = _sha256_file(remote_path)
            if (remote_bytes, remote_sha, _md5_file(remote_path)) != (
                local_bytes,
                local_sha,
                _md5_file(local_path),
            ):
                raise FixtureError(f"fake remote final object differs from local bytes: {local_path.name}")

        calls = _read_remote_log(run_dir / "fake-rclone-argv.jsonl")
        operations = [call[0] for call in calls]
        expected_operations = ["lsf", "copyto", "size", "md5sum", "cat", "moveto",
                               "copyto", "size", "md5sum", "cat", "moveto"]
        if sorted(operations) != sorted(expected_operations):
            raise FixtureError(f"unexpected fake-rclone command set: {operations!r}")
        if operations.count("cat") != 2:
            raise FixtureError("backup cycle did not fully read back both snapshot and manifest")
        cat_targets = [call[1] for call in calls if call[0] == "cat" and len(call) >= 2]
        expected_upload_names = {f"{raw_path.name}.uploading-", f"{manifest_path.name}.uploading-"}
        if len(cat_targets) != 2 or any(
            not any(target.startswith(f"{REMOTE}/{name}") for name in expected_upload_names)
            for target in cat_targets
        ):
            raise FixtureError("full readback did not target both unique temporary upload objects")
        for call in calls:
            if not call[0] in {"lsf", "copyto", "size", "md5sum", "cat", "moveto", "deletefile"}:
                raise FixtureError(f"fake-rclone log contains a non-fixture command: {call[0]!r}")
            for value in call[1:]:
                if value.startswith("fixture:") and not value.startswith(REMOTE + "/") and value != REMOTE:
                    raise FixtureError(f"fake-rclone call escaped the fixture remote: {value!r}")

        source_after = _capture_database(writer)
        if source_after != expected_source:
            raise FixtureError("source rows/schema changed while the backup cycle ran")
        source_file_hashes_after = {
            "database": _sha256_file(db_path),
            "wal": _sha256_file(wal_path),
        }
        if source_file_hashes_after != source_file_hashes_before:
            raise FixtureError("source main database or WAL bytes changed during the read-only cycle")

        return {
            "status": "ok",
            "run_dir": str(run_dir),
            "source": {
                "path": str(db_path),
                "rows_and_schema_sha256": expected_digest,
                "wal_bytes": wal_path.stat().st_size,
                "main_and_wal_bytes_unchanged": True,
            },
            "snapshot": {
                "path": str(raw_path),
                "bytes": raw_bytes,
                "sha256": raw_sha,
                "rows_and_schema_sha256": snapshot_digest,
                "matches_wal_backed_source": True,
                "manifest_path": str(manifest_path),
                "manifest_sha256": _sha256_file(manifest_path)[1],
            },
            "fake_remote": {
                "root": str(remote_root),
                "object_count": len(actual_remote_names),
                "full_readback_cat_count": operations.count("cat"),
            },
            "receipt": str(receipt_path),
            "rclone_argv_log": str(run_dir / "fake-rclone-argv.jsonl"),
            "cleanup_performed": False,
        }
    finally:
        writer.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=pathlib.Path, required=True)
    parser.add_argument("--cycle-script", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    try:
        work_root = _require_private_work_dir(args.work_dir)
        result = _run_fixture(work_root, args.cycle_script)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=True), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
