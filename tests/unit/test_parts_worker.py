"""scripts/parts_worker.py の試験。人工 DB と偽の rclone（run_rclone のモック）だけを使い、実 rclone・ネットワークには触れない。"""

import argparse
import contextlib
import fcntl
import hashlib
import io
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src/python"))
sys.path.insert(0, str(HERE.parents[1] / "scripts"))

from test_parts import MATCH_PART, Source  # noqa: E402

import parts_worker  # noqa: E402


class FakeRclone:
    """run_rclone の代わり。remote_dir を Drive に見立てる。"""

    def __init__(self, remote_dir: Path):
        self.remote = remote_dir
        self.calls: list[list[str]] = []
        self.copied_order: list[str] = []
        self.corrupt: set[str] = set()  # 届いたが中身が違うことにする（SHA が一致しない）
        self.drop: set[str] = set()  # 届かないことにする
        self.hashsum_error = False

    def __call__(self, rclone, config, args):
        self.calls.append([rclone, config, *args])
        files = self._files_from(args)
        if args[0] == "copy":
            src = Path(args[1])
            for rel in files:
                self.copied_order.append(rel)
                if rel in self.drop:
                    continue
                target = self.remote / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                data = (src / rel).read_bytes()
                target.write_bytes(data + b"x" if rel in self.corrupt else data)
            return ""
        if args[0] == "hashsum":
            if self.hashsum_error:
                raise RuntimeError("hashsum failed")
            lines = []
            for rel in files:
                target = self.remote / rel
                if target.is_file():
                    lines.append(f"{hashlib.sha256(target.read_bytes()).hexdigest()}  {rel}")
            return "\n".join(lines) + "\n"
        raise AssertionError(args)

    @staticmethod
    def _files_from(args):
        listing = Path(args[args.index("--files-from") + 1])
        return [line for line in listing.read_text(encoding="utf-8").splitlines() if line]


class WorkerTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.src = Source(self.root / "database" / "a.sqlite3")
        self.src.seed()
        self.remote = self.root / "drive"
        self.fake = FakeRclone(self.remote)
        patcher = mock.patch.object(parts_worker, "run_rclone", self.fake)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.opts = argparse.Namespace(
            source=str(self.src.path), out=str(self.root / "parts"), state=str(self.root / "state.sqlite3"),
            work_dir=str(self.root / "work"), remote="gdrive:SQuidLite3/db", rclone="/usr/bin/rclone-fake",
            rclone_config="/nonexistent/rclone.conf", interval=0.0, once=True,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def published(self) -> dict:
        conn = sqlite3.connect(self.opts.state)
        try:
            return {p: (s, b) for p, s, b, _at in conn.execute("SELECT path,sha256,bytes,published_at FROM published")}
        finally:
            conn.close()

    def part_files(self) -> dict:
        conn = sqlite3.connect(self.opts.state)
        try:
            return {p: s for p, s in conn.execute("SELECT path,sha256 FROM part_files")}
        finally:
            conn.close()

    def remote_catalog_published_at(self):
        conn = sqlite3.connect(f"{(self.remote / 'catalog.sqlite3').resolve().as_uri()}?mode=ro", uri=True)
        try:
            return conn.execute("SELECT published_at FROM status").fetchone()[0]
        finally:
            conn.close()


class CycleTests(WorkerTestBase):
    def test_success_publishes_parts_then_readme_then_catalog(self):
        summary = parts_worker.run_cycle(self.opts)
        parts = self.part_files()
        self.assertGreater(len(parts), 5)
        self.assertEqual(summary["uploaded"], len(parts))
        self.assertEqual(summary["verified"], len(parts))
        self.assertEqual(summary["failed"], 0)
        self.assertTrue(summary["catalog_published"])
        self.assertIsNotNone(summary["through_event_id"])
        self.assertIsNotNone(summary["lag_seconds"])
        self.assertGreaterEqual(summary["duration_seconds"], 0)
        # published は part_files の SHA-256 と一致し、目録・README も入る
        published = self.published()
        for path, digest in parts.items():
            self.assertEqual(published[path][0], digest)
            self.assertEqual(hashlib.sha256((self.remote / path).read_bytes()).hexdigest(), digest)
        self.assertIn("catalog.sqlite3", published)
        self.assertIn("README_FOR_AI.md", published)
        # 目録は最後。部品の送信の後に README、最後に目録
        order = self.fake.copied_order
        self.assertEqual(order[-2:], ["README_FOR_AI.md", "catalog.sqlite3"])
        self.assertNotIn("catalog.sqlite3", order[:-1])
        # 目録の送る直前に書いた published_at が、送った目録に入っている
        self.assertIsNotNone(self.remote_catalog_published_at())
        self.assertEqual(
            hashlib.sha256((self.remote / "catalog.sqlite3").read_bytes()).hexdigest(),
            published["catalog.sqlite3"][0],
        )
        readme = (self.remote / "README_FOR_AI.md").read_text(encoding="utf-8")
        self.assertIn("published_at", readme)
        # rclone の引数はリストで、設計どおりの旗を持つ
        copy_call = next(c for c in self.fake.calls if c[2] == "copy")
        self.assertEqual(copy_call[:2], ["/usr/bin/rclone-fake", "/nonexistent/rclone.conf"])
        self.assertEqual(copy_call[3:5], [str(self.root / "parts"), "gdrive:SQuidLite3/db"])
        for flag, value in (("--transfers", "4"), ("--checkers", "8")):
            self.assertEqual(copy_call[copy_call.index(flag) + 1], value)
        self.assertIn("--no-traverse", copy_call)
        self.assertIn("--files-from", copy_call)
        hash_call = next(c for c in self.fake.calls if c[2] == "hashsum")
        self.assertEqual(hash_call[2:5], ["hashsum", "sha256", "gdrive:SQuidLite3/db"])

    def test_idle_cycle_sends_nothing_and_change_sends_only_rebuilt(self):
        parts_worker.run_cycle(self.opts)
        self.fake.calls.clear()
        idle = parts_worker.run_cycle(self.opts)
        self.assertEqual((idle["rebuilt"], idle["uploaded"], idle["verified"], idle["failed"]), (0, 0, 0, 0))
        self.assertFalse(idle["catalog_published"])
        self.assertEqual(self.fake.calls, [])  # 何も変わらなければ rclone を呼ばない
        conn = self.src.connect()
        with conn:
            self.src.next_id = 100
            self.src.match(conn, "m6")
        conn.close()
        summary = parts_worker.run_cycle(self.opts)
        self.assertFalse(summary["failed"])
        self.assertGreaterEqual(summary["uploaded"], 1)
        self.assertLess(summary["uploaded"], len(self.part_files()))
        self.assertEqual(summary["uploaded"], summary["rebuilt"])
        self.assertTrue(summary["catalog_published"])
        remote_match = sqlite3.connect(f"{(self.remote / MATCH_PART).resolve().as_uri()}?mode=ro", uri=True)
        try:
            self.assertEqual(remote_match.execute("SELECT count(*) FROM matches WHERE match_key='m6'").fetchone()[0], 1)
        finally:
            remote_match.close()

    def test_partial_verification_failure_withholds_catalog_and_resends(self):
        # 一つの部品が壊れて届く。目録は送らない
        victim = "system/runs.sqlite3"
        self.fake.corrupt = {victim}
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["verified"], summary["uploaded"] - 1)
        self.assertFalse(summary["catalog_published"])
        self.assertNotIn(victim, self.published())
        self.assertNotIn("catalog.sqlite3", self.published())
        self.assertFalse((self.remote / "catalog.sqlite3").exists())
        self.assertNotIn("catalog.sqlite3", self.fake.copied_order)
        # 次の周期: 壊れていない部品は再送せず、失敗した一つだけを再送し、目録が出る
        self.fake.corrupt = set()
        self.fake.copied_order.clear()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["rebuilt"], summary["uploaded"], summary["verified"], summary["failed"]), (0, 1, 1, 0))
        self.assertTrue(summary["catalog_published"])
        self.assertEqual(self.fake.copied_order, [victim, "README_FOR_AI.md", "catalog.sqlite3"])
        self.assertEqual(self.published()[victim][0], self.part_files()[victim])
        self.assertIsNotNone(self.remote_catalog_published_at())

    def test_missing_on_remote_and_hashsum_error_are_failures(self):
        self.fake.drop = {"system/control.sqlite3"}
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual(summary["failed"], 1)
        self.assertFalse(summary["catalog_published"])
        self.fake.drop = set()
        self.fake.hashsum_error = True
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual(summary["verified"], 0)
        self.assertEqual(summary["failed"], 1)
        self.assertFalse(summary["catalog_published"])
        self.fake.hashsum_error = False
        summary = parts_worker.run_cycle(self.opts)
        self.assertTrue(summary["catalog_published"])

    def test_catalog_failure_is_retried_next_cycle(self):
        self.fake.corrupt = {"catalog.sqlite3"}
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual(summary["failed"], 0)
        self.assertFalse(summary["catalog_published"])
        self.assertNotIn("catalog.sqlite3", self.published())
        self.fake.corrupt = set()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual(summary["uploaded"], 0)
        self.assertTrue(summary["catalog_published"])  # 部品が変わっていなくても目録は再送される


class MainTests(WorkerTestBase):
    def argv(self):
        o = self.opts
        return ["--source", o.source, "--out", o.out, "--state", o.state, "--work-dir", o.work_dir,
                "--remote", o.remote, "--rclone", o.rclone, "--rclone-config", o.rclone_config, "--once"]

    def run_main(self, argv):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
            code = parts_worker.main(argv)
        return code, buf.getvalue()

    def test_once_prints_one_json_line(self):
        code, text = self.run_main(self.argv())
        self.assertEqual(code, 0)
        lines = text.strip().splitlines()
        self.assertEqual(len(lines), 1)
        data = json.loads(lines[0])
        self.assertEqual(
            set(data),
            {"through_event_id", "rebuilt", "uploaded", "verified", "failed", "catalog_published",
             "lag_seconds", "duration_seconds"},
        )
        self.assertTrue(data["catalog_published"])

    def test_second_worker_exits_zero_without_working(self):
        lock_path = Path(self.opts.state).with_name(Path(self.opts.state).name + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, "a") as held:
            fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            code, text = self.run_main(self.argv())
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(text), {"state": "another_worker_running"})
        self.assertEqual(self.fake.calls, [])
        self.assertFalse(Path(self.opts.out).exists())  # 作業も始めない
        # ロックが解けたら動く
        code, text = self.run_main(self.argv())
        self.assertEqual(code, 0)
        self.assertIn("through_event_id", json.loads(text))

    def test_exception_is_logged_and_once_exits_one(self):
        argv = self.argv()
        argv[argv.index("--source") + 1] = str(self.root / "no-such.sqlite3")
        code, text = self.run_main(argv)
        self.assertEqual(code, 1)
        self.assertEqual(text, "")

    def test_verification_failure_makes_once_exit_one(self):
        self.fake.corrupt = {"system/runs.sqlite3"}
        code, text = self.run_main(self.argv())
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(text)["failed"], 1)


class HashsumParseTests(unittest.TestCase):
    def test_parse(self):
        h = "a" * 64
        self.assertEqual(
            parts_worker.parse_hashsum(f"{h}  matches/a b/x.sqlite3\nnot a line\n{h.upper()}  y.sqlite3\n"),
            {"matches/a b/x.sqlite3": h, "y.sqlite3": h},
        )


if __name__ == "__main__":
    unittest.main()
