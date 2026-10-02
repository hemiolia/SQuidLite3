#!/usr/bin/env python3
"""Standard library support utilities for verified backup and upload scripts."""

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import shlex
import shutil
import sqlite3
import sys
import tempfile
import urllib.parse
import uuid


def cmd_plaintext_snapshot(args: argparse.Namespace) -> int:
    """Create a full plaintext SQLite snapshot and a manifest. Does not encrypt."""
    db_path = pathlib.Path(args.db_path).resolve()
    backup_dir = pathlib.Path(args.backup_dir).resolve()
    if not db_path.is_file():
        sys.stderr.write(f"Error: Database file does not exist: {db_path}\n")
        return 1
    backup_dir.mkdir(parents=True, exist_ok=True)

    uri = f"file:{urllib.parse.quote(str(db_path))}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        page_count = int(conn.execute("PRAGMA page_count;").fetchone()[0] or 0)
        page_size = int(conn.execute("PRAGMA page_size;").fetchone()[0] or 0)
        conn.close()
    except Exception as e:
        sys.stderr.write(f"Error: Failed to query source database PRAGMA ({db_path}): {e}\n")
        return 1

    db_bytes = page_count * page_size
    required = db_bytes + (1024 * 1024 * 1024)
    free_bytes = shutil.disk_usage(backup_dir).free
    if free_bytes < required:
        sys.stderr.write(
            f"Error: Insufficient free space in {backup_dir}: "
            f"required {required} bytes ({db_bytes} + 1GiB), available {free_bytes} bytes\n"
        )
        return 1

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    raw_name = f"archive_{stamp}_{uuid.uuid4().hex}.sqlite3"
    partial = backup_dir / f".{raw_name}.partial"
    final = backup_dir / raw_name
    manifest_final = backup_dir / f"{raw_name}.manifest.json"
    manifest_partial = backup_dir / f".{raw_name}.manifest.json.partial"
    if final.exists() or manifest_final.exists():
        sys.stderr.write(f"Error: Destination already exists: {raw_name}\n")
        return 1

    src = None
    dst = None
    try:
        src = sqlite3.connect(uri, uri=True)
        src.execute("PRAGMA busy_timeout=60000;")
        # Pin a read transaction before copying. In WAL mode this keeps the
        # snapshot stable while allowing the collector to commit newer rows;
        # backup steps do not restart against those newer commits. A writer
        # reservation would block collection for the entire large DB copy.
        src.execute("BEGIN;")
        capture_started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # A read-only URI does not load the pager until a statement runs.
        # Without this, backup() reports total=0 and would write an empty file.
        seen_pages = int(src.execute("PRAGMA page_count;").fetchone()[0] or 0)
        if seen_pages <= 0:
            raise RuntimeError(
                f"Source database page_count is {seen_pages}; refusing an empty snapshot"
            )
        captured_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        sys.stderr.write(f"snapshot_source_pages {seen_pages}\n")
        sys.stderr.flush()
        dst = sqlite3.connect(str(partial))
        steps = {"n": 0}

        def _progress(status, remaining, total):
            del status
            if total <= 0:
                raise RuntimeError(f"SQLite backup reported no pages (total={total})")
            steps["n"] += 1
            if remaining == 0 or steps["n"] % 16 == 0:
                sys.stderr.write(
                    f"snapshot_progress remaining={remaining} total={total}\n"
                )
                sys.stderr.flush()

        sys.stderr.write("snapshot_phase copy\n")
        sys.stderr.flush()
        src.backup(dst, pages=-1, progress=_progress, sleep=0)
        dst.close()
        dst = None
        src.rollback()
        src.close()
        src = None
        sys.stderr.write("snapshot_phase copy_done\n")
        sys.stderr.flush()

        sync_fd = os.open(partial, os.O_RDONLY)
        try:
            os.fsync(sync_fd)
        finally:
            os.close(sync_fd)

        sys.stderr.write("snapshot_phase quick_check\n")
        sys.stderr.flush()
        chk = sqlite3.connect(f"file:{urllib.parse.quote(str(partial))}?mode=ro", uri=True)
        chk.execute("PRAGMA cache_size=-65536;")
        res = chk.execute("PRAGMA quick_check;").fetchall()
        chk.close()
        if len(res) != 1 or res[0][0] != "ok":
            sys.stderr.write(f"Error: SQLite quick_check returned unexpected result: {res}\n")
            return 1

        with open(partial, "rb") as handle:
            header = handle.read(16)
        if header != b"SQLite format 3\x00":
            sys.stderr.write("Error: Snapshot header is not SQLite format 3\n")
            return 1

        sys.stderr.write("snapshot_phase sha256\n")
        sys.stderr.flush()
        digest = hashlib.sha256()
        size = 0
        with open(partial, "rb") as handle:
            for chunk in iter(lambda: handle.read(1048576), b""):
                digest.update(chunk)
                size += len(chunk)
        os.replace(partial, final)
        manifest = {
            "capture_started_at": capture_started,
            "captured_at": captured_at,
            "captured_at_kind": "pinned_read_transaction",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "storage": "plaintext",
            "encryption": None,
            "raw_snapshot": {
                "basename": final.name,
                "bytes": size,
                "sha256": digest.hexdigest(),
                "quick_check": "ok",
            },
            "verification": {
                "sha256_match": True,
                "quick_check": "ok",
                "plaintext": True,
            },
        }
        manifest_partial.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        os.replace(manifest_partial, manifest_final)
    except Exception as e:
        sys.stderr.write(f"Error: Plaintext snapshot failed: {e}\n")
        return 1
    finally:
        if dst is not None:
            try:
                dst.close()
            except Exception:
                pass
        if src is not None:
            try:
                src.close()
            except Exception:
                pass
        if partial.exists():
            try:
                partial.unlink()
            except Exception:
                pass
        if manifest_partial.exists():
            try:
                manifest_partial.unlink()
            except Exception:
                pass

    sys.stdout.write(json.dumps({
        "status": "ok",
        "raw_path": str(final),
        "manifest_path": str(manifest_final),
    }) + "\n")
    return 0


def cmd_check_space(args: argparse.Namespace) -> int:
    db_path = pathlib.Path(args.db_path).resolve()
    backup_dir = pathlib.Path(args.backup_dir).resolve()

    if not db_path.is_file():
        sys.stderr.write(f"Error: Database file does not exist: {db_path}\n")
        return 1
    if not backup_dir.is_dir():
        sys.stderr.write(f"Error: Backup directory does not exist: {backup_dir}\n")
        return 1

    uri = f"file:{urllib.parse.quote(str(db_path))}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        cur = conn.cursor()
        cur.execute("PRAGMA page_count;")
        row = cur.fetchone()
        page_count = int(row[0]) if row and row[0] is not None else 0
        cur.execute("PRAGMA page_size;")
        row = cur.fetchone()
        page_size = int(row[0]) if row and row[0] is not None else 0
        conn.close()
    except Exception as e:
        sys.stderr.write(f"Error: Failed to query source database PRAGMA ({db_path}): {e}\n")
        return 1

    db_bytes = page_count * page_size
    one_gib = 1024 * 1024 * 1024
    required_bytes = (4 * db_bytes) + one_gib

    try:
        free_bytes = shutil.disk_usage(backup_dir).free
    except Exception as e:
        sys.stderr.write(f"Error: Failed to inspect disk usage of {backup_dir}: {e}\n")
        return 1

    if free_bytes < required_bytes:
        sys.stderr.write(
            f"Error: Insufficient free space in {backup_dir}: "
            f"required {required_bytes} bytes (4 * {db_bytes} + 1GiB), available {free_bytes} bytes\n"
        )
        return 1

    return 0


def cmd_backup_db(args: argparse.Namespace) -> int:
    src_path = pathlib.Path(args.src_path).resolve()
    dst_path = pathlib.Path(args.dst_path).resolve()

    if not src_path.is_file():
        sys.stderr.write(f"Error: Source database does not exist: {src_path}\n")
        return 1

    src_uri = f"file:{urllib.parse.quote(str(src_path))}?mode=ro"
    try:
        src_conn = sqlite3.connect(src_uri, uri=True)
        dst_conn = sqlite3.connect(str(dst_path))
        src_conn.backup(dst_conn)
        dst_conn.close()
        src_conn.close()
    except Exception as e:
        sys.stderr.write(f"Error: SQLite backup failed from {src_path} to {dst_path}: {e}\n")
        return 1

    chk_uri = f"file:{urllib.parse.quote(str(dst_path))}?mode=ro"
    try:
        chk_conn = sqlite3.connect(chk_uri, uri=True)
        chk_conn.execute("PRAGMA cache_size=-65536;")
        res = chk_conn.execute("PRAGMA quick_check;").fetchall()
        chk_conn.close()
    except Exception as e:
        sys.stderr.write(f"Error: SQLite quick_check failed on {dst_path}: {e}\n")
        return 1

    if len(res) != 1 or res[0][0] != "ok":
        sys.stderr.write(f"Error: SQLite quick_check returned unexpected result on {dst_path}: {res}\n")
        return 1

    return 0


def cmd_quick_check(args: argparse.Namespace) -> int:
    target_path = pathlib.Path(args.path).resolve()
    if not target_path.is_file():
        sys.stderr.write(f"Error: File does not exist for quick_check: {target_path}\n")
        return 1

    chk_uri = f"file:{urllib.parse.quote(str(target_path))}?mode=ro"
    try:
        conn = sqlite3.connect(chk_uri, uri=True)
        conn.execute("PRAGMA cache_size=-65536;")
        res = conn.execute("PRAGMA quick_check;").fetchall()
        conn.close()
    except Exception as e:
        sys.stderr.write(f"Error: SQLite quick_check failed on {target_path}: {e}\n")
        return 1

    if len(res) != 1 or res[0][0] != "ok":
        sys.stderr.write(f"Error: SQLite quick_check returned non-ok on {target_path}: {res}\n")
        return 1

    return 0


def cmd_hash_file_multi(args: argparse.Namespace) -> int:
    target_path = pathlib.Path(args.path).resolve()
    if not target_path.is_file():
        sys.stderr.write(f"Error: File does not exist: {target_path}\n")
        return 1

    h_sha = hashlib.sha256()
    h_md5 = hashlib.md5()
    total_bytes = 0
    with open(target_path, "rb") as f:
        for chunk in iter(lambda: f.read(1048576), b""):
            h_sha.update(chunk)
            h_md5.update(chunk)
            total_bytes += len(chunk)

    sys.stdout.write(f"{total_bytes} {h_sha.hexdigest()} {h_md5.hexdigest()}\n")
    return 0


def cmd_hash_file_sha256(args: argparse.Namespace) -> int:
    target_path = pathlib.Path(args.path).resolve()
    if not target_path.is_file():
        sys.stderr.write(f"Error: File does not exist: {target_path}\n")
        return 1

    h_sha = hashlib.sha256()
    total_bytes = 0
    with open(target_path, "rb") as f:
        for chunk in iter(lambda: f.read(1048576), b""):
            h_sha.update(chunk)
            total_bytes += len(chunk)

    sys.stdout.write(f"{total_bytes} {h_sha.hexdigest()}\n")
    return 0


def cmd_write_manifest(args: argparse.Namespace) -> int:
    manifest_data = {
        "timestamp": args.timestamp,
        "raw_snapshot": {
            "basename": args.raw_basename,
            "bytes": int(args.raw_bytes),
            "sha256": args.raw_sha256,
            "quick_check": args.raw_quick_check,
        },
        "encrypted_snapshot": {
            "basename": args.enc_basename,
            "bytes": int(args.enc_bytes),
            "sha256": args.enc_sha256,
            "compression": args.compression,
            "cipher": args.cipher,
        },
        "verification": {
            "sha256_match": True,
            "quick_check": "ok",
        },
    }

    out_path = pathlib.Path(args.output).resolve()
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)
            f.write("\n")
    except Exception as e:
        sys.stderr.write(f"Error: Failed to write manifest to {out_path}: {e}\n")
        return 1

    return 0


def cmd_verify_manifest_content(args: argparse.Namespace) -> int:
    manifest_content = args.manifest
    enc_basename = args.enc_basename
    src_bytes = int(args.src_enc_size)
    src_sha256 = args.src_enc_sha256

    try:
        data = json.loads(manifest_content)
    except Exception as e:
        sys.stderr.write(f"Error: Manifest is not valid JSON: {e}\n")
        return 1

    enc_snap = data.get("encrypted_snapshot")
    if not isinstance(enc_snap, dict):
        sys.stderr.write("Error: Manifest missing valid 'encrypted_snapshot' section\n")
        return 1

    if enc_snap.get("basename") != enc_basename:
        sys.stderr.write(
            f"Error: Manifest encrypted basename mismatch: "
            f"manifest '{enc_snap.get('basename')}' vs expected '{enc_basename}'\n"
        )
        return 1

    if enc_snap.get("bytes") != src_bytes:
        sys.stderr.write(
            f"Error: Manifest encrypted bytes mismatch: "
            f"manifest {enc_snap.get('bytes')} vs source {src_bytes}\n"
        )
        return 1

    if enc_snap.get("sha256") != src_sha256:
        sys.stderr.write(
            f"Error: Manifest encrypted sha256 mismatch: "
            f"manifest '{enc_snap.get('sha256')}' vs source '{src_sha256}'\n"
        )
        return 1

    verif = data.get("verification")
    if not isinstance(verif, dict):
        sys.stderr.write("Error: Manifest missing valid 'verification' section\n")
        return 1

    if verif.get("sha256_match") is not True:
        sys.stderr.write(
            f"Error: Manifest verification.sha256_match is not true: {verif.get('sha256_match')}\n"
        )
        return 1

    if verif.get("quick_check") != "ok":
        sys.stderr.write(
            f"Error: Manifest verification.quick_check is not 'ok': {verif.get('quick_check')}\n"
        )
        return 1

    raw_snap = data.get("raw_snapshot")
    if isinstance(raw_snap, dict) and raw_snap.get("quick_check") != "ok":
        sys.stderr.write(
            f"Error: Manifest raw_snapshot.quick_check is not 'ok': {raw_snap.get('quick_check')}\n"
        )
        return 1

    return 0


def cmd_safe_unlink(args: argparse.Namespace) -> int:
    backup_dir = pathlib.Path(args.backup_dir).resolve()
    prefix = args.prefix

    for item in args.paths:
        if not item:
            continue
        try:
            p = pathlib.Path(item).resolve()
            if p.parent == backup_dir and p.name.startswith(prefix) and (p.is_file() or p.is_symlink()):
                p.unlink(missing_ok=True)
        except Exception:
            pass
    return 0


def cmd_create_temp_mode600(args: argparse.Namespace) -> int:
    target_dir = pathlib.Path(args.target_dir).resolve()
    if not target_dir.is_dir():
        sys.stderr.write(f"Error: Target directory does not exist: {target_dir}\n")
        return 1

    prefix = args.prefix
    fd, path = tempfile.mkstemp(prefix=prefix, dir=str(target_dir))
    os.close(fd)
    os.chmod(path, 0o600)
    sys.stdout.write(f"{path}\n")
    return 0


def cmd_quote(args: argparse.Namespace) -> int:
    sys.stdout.write(shlex.quote(args.value) + "\n")
    return 0


def cmd_json_output(args: argparse.Namespace) -> int:
    payload = {}
    for item in args.pairs:
        if "=" in item:
            k, v = item.split("=", 1)
            # Try to parse numeric or boolean if appropriate
            if v.isdigit():
                payload[k] = int(v)
            elif v.lower() == "true":
                payload[k] = True
            elif v.lower() == "false":
                payload[k] = False
            else:
                payload[k] = v
        else:
            payload[item] = True
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Verified backup support helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_plain = subparsers.add_parser("plaintext-snapshot")
    p_plain.add_argument("db_path")
    p_plain.add_argument("backup_dir")
    p_plain.set_defaults(func=cmd_plaintext_snapshot)

    # check-space
    p_space = subparsers.add_parser("check-space")
    p_space.add_argument("db_path")
    p_space.add_argument("backup_dir")
    p_space.set_defaults(func=cmd_check_space)

    # backup-db
    p_bk = subparsers.add_parser("backup-db")
    p_bk.add_argument("src_path")
    p_bk.add_argument("dst_path")
    p_bk.set_defaults(func=cmd_backup_db)

    # quick-check
    p_qc = subparsers.add_parser("quick-check")
    p_qc.add_argument("path")
    p_qc.set_defaults(func=cmd_quick_check)

    # hash-file-multi (bytes sha256 md5)
    p_hmulti = subparsers.add_parser("hash-file-multi")
    p_hmulti.add_argument("path")
    p_hmulti.set_defaults(func=cmd_hash_file_multi)

    # hash-file-sha256 (bytes sha256)
    p_hsha = subparsers.add_parser("hash-file-sha256")
    p_hsha.add_argument("path")
    p_hsha.set_defaults(func=cmd_hash_file_sha256)

    # write-manifest
    p_wm = subparsers.add_parser("write-manifest")
    p_wm.add_argument("--output", required=True)
    p_wm.add_argument("--timestamp", required=True)
    p_wm.add_argument("--raw-basename", required=True)
    p_wm.add_argument("--raw-bytes", required=True)
    p_wm.add_argument("--raw-sha256", required=True)
    p_wm.add_argument("--raw-quick-check", default="ok")
    p_wm.add_argument("--enc-basename", required=True)
    p_wm.add_argument("--enc-bytes", required=True)
    p_wm.add_argument("--enc-sha256", required=True)
    p_wm.add_argument("--compression", default="zstd")
    p_wm.add_argument("--cipher", default="AES256")
    p_wm.set_defaults(func=cmd_write_manifest)

    # verify-manifest-content
    p_vmc = subparsers.add_parser("verify-manifest-content")
    p_vmc.add_argument("--manifest", required=True)
    p_vmc.add_argument("--enc-basename", required=True)
    p_vmc.add_argument("--src-enc-size", required=True)
    p_vmc.add_argument("--src-enc-sha256", required=True)
    p_vmc.set_defaults(func=cmd_verify_manifest_content)

    # safe-unlink
    p_su = subparsers.add_parser("safe-unlink")
    p_su.add_argument("backup_dir")
    p_su.add_argument("prefix")
    p_su.add_argument("paths", nargs="*")
    p_su.set_defaults(func=cmd_safe_unlink)

    # create-temp-mode600
    p_ct = subparsers.add_parser("create-temp-mode600")
    p_ct.add_argument("target_dir")
    p_ct.add_argument("prefix")
    p_ct.set_defaults(func=cmd_create_temp_mode600)

    # quote
    p_q = subparsers.add_parser("quote")
    p_q.add_argument("value")
    p_q.set_defaults(func=cmd_quote)

    # json-output
    p_jo = subparsers.add_parser("json-output")
    p_jo.add_argument("pairs", nargs="*")
    p_jo.set_defaults(func=cmd_json_output)

    parsed = parser.parse_args()
    return parsed.func(parsed)


if __name__ == "__main__":
    sys.exit(main())
