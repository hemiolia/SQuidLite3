#!/usr/bin/env python3
"""SQuidLite3 の部品を作り、Google Drive（rclone）へ送って SHA-256 で照合する常駐 worker（設計 0.3）。

  parts_worker.py --source DB --out DIR --state PATH --work-dir DIR --remote REMOTE:db \\
                  --rclone BIN --rclone-config CONF [--interval 60] [--once]

一周期の流れ（設計書「更新の手順」）:
  1. build_parts で部品と目録 catalog.sqlite3 を作る（正本は読み取り専用）。
  2. 送るべき部品 = 状態 DB の published と part_files の SHA-256 が違うもの全部（前回の失敗分を含む）。
  3. `rclone copy` で送り、`rclone hashsum sha256` で Drive 側の SHA-256 を取り、手元と一致したものだけ published を進める。
  4. 全部品が published と一致したときだけ、README_FOR_AI.md と catalog.sqlite3（公開の確定点）を同じ方法で送って照合する。
     目録は送る直前に status.published_at を書き、書いた後の SHA-256 で照合する。
一周期ごとに JSON を一行、標準出力へ出す。例外は握りつぶさずログ（標準エラー）に出して次の周期へ進む。
二重起動は状態 DB と同じ場所のロックファイル（fcntl.flock、非ブロック）で防ぐ。
rclone は run_rclone 一つから呼ぶ（シェルを使わない）。
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/python"))

from ikarchive.parts import (  # noqa: E402
    CATALOG_NAME,
    CHANGE_TABLE,
    PartsQuestion,
    _sha256_file,
    build_parts,
)
from ikarchive.parts_guide import render_readme  # noqa: E402

README_NAME = "README_FOR_AI.md"
_HASH_LINE = re.compile(r"^([0-9a-fA-F]{64})\s+(.+?)\s*$")


def run_rclone(rclone: str, config: str, args: list[str]) -> str:
    """rclone を一回呼ぶ（このスクリプトで rclone を実行する唯一の場所。シェルを使わない。テストではモックする）。

    終了コードが 0 でなければ RuntimeError。標準出力を返す。
    """
    command = [rclone, "--config", config, *args]
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"rclone {args[0]} が終了コード {proc.returncode}: {proc.stderr.strip()[-2000:]}")
    return proc.stdout


def _log(message: str) -> None:
    sys.stderr.write(message.rstrip("\n") + "\n")
    sys.stderr.flush()


def parse_hashsum(output: str) -> dict[str, str]:
    """`rclone hashsum sha256` の出力（`<sha256>  <相対パス>`）を {相対パス: sha256} にする。"""
    found: dict[str, str] = {}
    for line in output.splitlines():
        match = _HASH_LINE.match(line)
        if match:
            found[match.group(2)] = match.group(1).lower()
    return found


def send_and_verify(opts: argparse.Namespace, wanted: dict[str, str]) -> tuple[set[str], set[str]]:
    """wanted（{相対パス: 手元の SHA-256}）を Drive へ送り、Drive の SHA-256 と一致したものを返す。

    戻り値は (照合できたパス, 照合できなかったパス)。copy が失敗しても hashsum で何が届いたかを確かめる。
    """
    if not wanted:
        return set(), set()
    work_dir = Path(opts.work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    fd, list_name = tempfile.mkstemp(prefix="files-from-", suffix=".txt", dir=work_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for path in sorted(wanted):
                fh.write(path + "\n")
        try:
            run_rclone(opts.rclone, opts.rclone_config, [
                "copy", str(opts.out), opts.remote, "--files-from", list_name,
                "--transfers", "4", "--checkers", "8", "--no-traverse",
            ])
        except Exception:
            _log("rclone copy 失敗:\n" + traceback.format_exc())
        remote_sha: dict[str, str] = {}
        try:
            output = run_rclone(opts.rclone, opts.rclone_config, [
                "hashsum", "sha256", opts.remote, "--files-from", list_name,
            ])
            remote_sha = parse_hashsum(output)
        except Exception:
            _log("rclone hashsum 失敗:\n" + traceback.format_exc())
    finally:
        try:
            os.unlink(list_name)
        except FileNotFoundError:
            pass
    verified = {p for p, digest in wanted.items() if remote_sha.get(p) == digest}
    return verified, set(wanted) - verified


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _lag_seconds(source: str, through: Optional[int]) -> Optional[float]:
    """through の変更追跡の changed_at から現在までの秒数。"""
    if through is None:
        return None
    path = Path(source).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(path, uri=True)
    try:
        row = conn.execute(f'SELECT changed_at FROM "{CHANGE_TABLE}" WHERE event_id=?', (through,)).fetchone()
    finally:
        conn.close()
    if not row or not isinstance(row[0], str):
        return None
    try:
        when = datetime.fromisoformat(row[0])
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - when).total_seconds(), 3)


def _write_readme(out: Path) -> Path:
    """目録から README_FOR_AI.md を作る（一時ファイルから置き換える）。"""
    catalog = sqlite3.connect(Path(out / CATALOG_NAME).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        text = render_readme(catalog)
    finally:
        catalog.close()
    target = out / README_NAME
    tmp = out / (README_NAME + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, target)
    return target


def _stamp_catalog(out: Path, published_at: str) -> None:
    """目録の status.published_at を書く（送る直前。書いた後の SHA-256 で照合する）。"""
    conn = sqlite3.connect(out / CATALOG_NAME, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("UPDATE status SET published_at=?", (published_at,))
    finally:
        conn.close()


def run_cycle(opts: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    out = Path(opts.out)
    result = build_parts(opts.source, out, opts.state, opts.work_dir)
    through = result["through_event_id"]

    state = sqlite3.connect(opts.state, isolation_level=None)
    try:
        # 送るべき部品: published と SHA-256 が違うもの全部（前回の失敗分を含む）。目録と README は含めない
        pending = {
            path: digest
            for path, digest, _bytes in state.execute(
                "SELECT f.path,f.sha256,f.bytes FROM part_files f LEFT JOIN published p ON p.path=f.path "
                "WHERE p.sha256 IS NOT f.sha256 ORDER BY f.path"
            )
        }
        sizes = {p: b for p, b in state.execute("SELECT path,bytes FROM part_files")}
        verified, failed = send_and_verify(opts, pending)
        if verified:
            now = _now()
            state.execute("BEGIN IMMEDIATE")
            for path in sorted(verified):
                state.execute(
                    "INSERT OR REPLACE INTO published(path,sha256,bytes,published_at) VALUES(?,?,?,?)",
                    (path, pending[path], sizes.get(path), now),
                )
            state.execute("INSERT OR REPLACE INTO meta VALUES('catalog_pending','1')")
            state.execute("COMMIT")
        for path in sorted(failed):
            _log(f"照合できなかった部品（次の周期に再送）: {path}")

        catalog_published = False
        unpublished = state.execute(
            "SELECT count(*) FROM part_files f LEFT JOIN published p ON p.path=f.path WHERE p.sha256 IS NOT f.sha256"
        ).fetchone()[0]
        if unpublished == 0:
            marker = state.execute("SELECT value FROM meta WHERE key='catalog_pending'").fetchone()
            published_through = state.execute(
                "SELECT value FROM meta WHERE key='catalog_published_through'"
            ).fetchone()
            never = state.execute("SELECT 1 FROM published WHERE path=?", (CATALOG_NAME,)).fetchone() is None
            need = (
                bool(verified)
                or (marker is not None and marker[0] == "1")
                or never
                or (published_through is None or published_through[0] != str(through))
            )
            if need:
                now = _now()
                _stamp_catalog(out, now)
                readme = _write_readme(out)
                wanted = {
                    README_NAME: _sha256_file(readme)[0],
                    CATALOG_NAME: _sha256_file(out / CATALOG_NAME)[0],
                }
                # README を先に、目録（公開の確定点）を最後に送る
                ok_readme, _ = send_and_verify(opts, {README_NAME: wanted[README_NAME]})
                ok_catalog: set[str] = set()
                if ok_readme:
                    ok_catalog, _ = send_and_verify(opts, {CATALOG_NAME: wanted[CATALOG_NAME]})
                if ok_readme and ok_catalog:
                    catalog_published = True
                    state.execute("BEGIN IMMEDIATE")
                    for name in (README_NAME, CATALOG_NAME):
                        size = (out / name).stat().st_size
                        state.execute(
                            "INSERT OR REPLACE INTO published(path,sha256,bytes,published_at) VALUES(?,?,?,?)",
                            (name, wanted[name], size, now),
                        )
                    state.execute("INSERT OR REPLACE INTO meta VALUES('catalog_published_through',?)", (str(through),))
                    state.execute("DELETE FROM meta WHERE key='catalog_pending'")
                    state.execute("COMMIT")
                else:
                    _log("README または目録を照合できなかった（次の周期に再送）")
                    state.execute("INSERT OR REPLACE INTO meta VALUES('catalog_pending','1')")
    finally:
        state.close()

    return {
        "through_event_id": through,
        "rebuilt": len(result["rebuilt_parts"]),
        "uploaded": len(pending),
        "verified": len(verified),
        "failed": len(failed),
        "catalog_published": catalog_published,
        "lag_seconds": _lag_seconds(opts.source, through),
        "duration_seconds": round(time.monotonic() - started, 3),
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, help="正本の SQLite（読み取り専用で開く）")
    parser.add_argument("--out", required=True, help="部品の出力ディレクトリ（Drive の db/ と同じ階層）")
    parser.add_argument("--state", required=True, help="状態 DB のパス")
    parser.add_argument("--work-dir", required=True, help="作業ディレクトリ")
    parser.add_argument("--remote", required=True, help="rclone の送り先（例: gdrive:SQuidLite3/db）")
    parser.add_argument("--rclone", required=True, help="rclone の実行ファイル")
    parser.add_argument("--rclone-config", required=True, help="rclone.conf")
    parser.add_argument("--interval", type=float, default=60.0, help="周期の間隔（秒）")
    parser.add_argument("--once", action="store_true", help="一周期だけ行って終了する")
    opts = parser.parse_args(argv)

    state_path = Path(opts.state)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = state_path.with_name(state_path.name + ".lock")
    lock_file = open(lock_path, "a")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print(json.dumps({"state": "another_worker_running"}, ensure_ascii=False), flush=True)
        lock_file.close()
        return 0

    try:
        while True:
            failed_cycle = False
            try:
                summary = run_cycle(opts)
                failed_cycle = summary["failed"] > 0
                print(json.dumps(summary, ensure_ascii=False), flush=True)
            except PartsQuestion as exc:
                _log(str(exc))
                failed_cycle = True
            except Exception:
                _log("周期が例外で失敗:\n" + traceback.format_exc())
                failed_cycle = True
            if opts.once:
                return 1 if failed_cycle else 0
            time.sleep(opts.interval)
    finally:
        lock_file.close()


if __name__ == "__main__":
    raise SystemExit(main())
