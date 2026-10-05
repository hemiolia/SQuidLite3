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

import ikarchive.parts as parts_module  # noqa: E402
from ikarchive.parts import PartsQuestion, audit  # noqa: E402
from test_parts import Source, assert_audit_clean, match_path  # noqa: E402

import parts_worker  # noqa: E402

M6_PART = match_path("m6")  # 版 3: 試合は 1 試合 1 ファイル


class FakeRclone:
    """run_rclone の代わり。remote_dir を Drive に見立てる。"""

    def __init__(self, remote_dir: Path):
        self.remote = remote_dir
        self.calls: list[list[str]] = []
        self.copied_order: list[str] = []
        self.corrupt: set[str] = set()  # 届いたが中身が違うことにする（SHA が一致しない）
        self.drop: set[str] = set()  # 届かないことにする
        self.hashsum_error = False
        self.delete_fail: set[str] = set()  # deletefile が（一般の例外で）失敗することにする。Drive の側は変わらない
        self.delete_code: dict[str, int] = {}  # deletefile が、その rclone の終了コードで失敗することにする
        self.events: list[tuple[str, str]] = []  # 時系列: ("copy", 相対パス) と ("delete", 相対パス)

    DELETE_PREFIX = "gdrive:SQuidLite3/db/"

    def __call__(self, rclone, config, args):
        self.calls.append([rclone, config, *args])
        if args[0] == "deletefile":
            return self._deletefile(args)
        files = self._files_from(args)
        if args[0] == "copy":
            src = Path(args[1])
            for rel in files:
                self.copied_order.append(rel)
                self.events.append(("copy", rel))
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

    def _deletefile(self, args):
        """`rclone deletefile <remote>/<path>`。引数は 1 つだけ。Drive に無ければ rclone と同じく終了コード 4。"""
        assert len(args) == 2 and args[1].startswith(self.DELETE_PREFIX), args
        rel = args[1][len(self.DELETE_PREFIX):]
        if rel in self.delete_fail:
            raise RuntimeError("deletefile failed")
        if rel in self.delete_code:
            raise parts_worker.RcloneError("deletefile failed", self.delete_code[rel])
        target = self.remote / rel
        if not target.is_file():
            raise parts_worker.RcloneError("object not found", 4)
        target.unlink()
        self.events.append(("delete", rel))
        return ""

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

    def argv(self):
        o = self.opts
        return ["--source", o.source, "--out", o.out, "--state", o.state, "--work-dir", o.work_dir,
                "--remote", o.remote, "--rclone", o.rclone, "--rclone-config", o.rclone_config, "--once"]

    def run_main(self, argv):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
            code = parts_worker.main(argv)
        return code, buf.getvalue()

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
        remote_match = sqlite3.connect(f"{(self.remote / M6_PART).resolve().as_uri()}?mode=ro", uri=True)
        try:
            self.assertEqual(remote_match.execute("SELECT count(*) FROM matches WHERE match_key='m6'").fetchone()[0], 1)
        finally:
            remote_match.close()
        assert_audit_clean(self, audit(self.src.path, self.root / "parts"))

    def test_refetch_cycle_sends_only_the_fetch_day_and_system_parts(self):
        # 同じ応答の取り直し（response_fetches の行が一つ増えるだけ）は、fetches/<日> と system 系しか作り直さず送らない
        parts_worker.run_cycle(self.opts)
        conn = self.src.connect()
        with conn:
            self.src.refetch(conn, self.src.detail_id["m1"], "2026-10-07T01:00:00+00:00")
        conn.close()
        self.fake.calls.clear()
        self.fake.copied_order.clear()
        summary = parts_worker.run_cycle(self.opts)
        sent = [p for p in self.fake.copied_order if p not in ("README_FOR_AI.md", "catalog.sqlite3")]
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(summary["uploaded"], len(sent))
        self.assertTrue(summary["catalog_published"])
        self.assertIn("fetches/2026-10/2026-10-07.sqlite3", sent)
        self.assertEqual({p for p in sent if not p.startswith(("fetches/", "system/"))}, set())
        assert_audit_clean(self, audit(self.src.path, self.root / "parts"))

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


class RuleVersionTests(WorkerTestBase):
    def test_other_rule_version_stops_the_cycle_before_sending_anything(self):
        parts_worker.run_cycle(self.opts)
        before = self.part_files()
        self.fake.calls.clear()
        with mock.patch.object(parts_module, "HOME_RULE_VERSION", parts_module.HOME_RULE_VERSION + 1):
            with self.assertRaises(PartsQuestion) as ctx:
                parts_worker.run_cycle(self.opts)
        self.assertTrue(str(ctx.exception).startswith("QUESTION:"))
        self.assertEqual(self.fake.calls, [])  # 古い版の出力先に何も足さず、何も送らない
        self.assertEqual(self.part_files(), before)


class MainTests(WorkerTestBase):
    def test_once_prints_one_json_line(self):
        code, text = self.run_main(self.argv())
        self.assertEqual(code, 0)
        lines = text.strip().splitlines()
        self.assertEqual(len(lines), 1)
        data = json.loads(lines[0])
        self.assertEqual(
            set(data),
            {"through_event_id", "rebuilt", "uploaded", "verified", "failed", "deleted", "delete_failed",
             "catalog_published", "lag_seconds", "duration_seconds"},
        )
        self.assertTrue(data["catalog_published"])
        self.assertEqual((data["deleted"], data["delete_failed"]), ([], 0))

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

    def test_other_rule_version_makes_once_exit_one_and_logs_question(self):
        self.run_main(self.argv())
        buf, err = io.StringIO(), io.StringIO()
        with mock.patch.object(parts_module, "HOME_RULE_VERSION", parts_module.HOME_RULE_VERSION + 1):
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
                code = parts_worker.main(self.argv())
        self.assertEqual(code, 1)
        self.assertEqual(buf.getvalue(), "")
        self.assertTrue(err.getvalue().startswith("QUESTION:"), err.getvalue())

    def test_verification_failure_makes_once_exit_one(self):
        self.fake.corrupt = {"system/runs.sqlite3"}
        code, text = self.run_main(self.argv())
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(text)["failed"], 1)


PENDING = "matches/unclassified/no-rule/unknown-date/pend.sqlite3"  # 詳細が未取得の試合の部品


class DeletionTests(WorkerTestBase):
    """行が一つも無くなった部品を、Drive からも `rclone deletefile <remote>/<path>` で消す。"""

    def start_with_pending_match(self):
        """詳細が未取得の試合の部品を Drive に送った状態から始める。"""
        conn = self.src.connect()
        with conn:
            self.src.pending_match(conn, "pend")
        conn.close()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["failed"], summary["deleted"], summary["delete_failed"]), (0, [], 0))
        self.assertTrue((self.remote / PENDING).is_file())
        self.assertIn(PENDING, self.published())
        self.assertEqual(self.deletefile_calls(), [])
        self.fake.calls.clear()
        self.fake.copied_order.clear()
        self.fake.events.clear()

    def attach_detail(self):
        """詳細が届いて、試合が新しい場所の部品へ移る（元の部品は行が無くなる）。移り先の住所を返す。"""
        conn = self.src.connect()
        with conn:
            self.src.next_id = 300
            self.src.attach_detail(conn, "pend")
        conn.close()
        return match_path("pend")

    def deletefile_calls(self):
        return [c for c in self.fake.calls if c[2] == "deletefile"]

    def remote_catalog_files(self) -> set:
        conn = sqlite3.connect(f"{(self.remote / 'catalog.sqlite3').resolve().as_uri()}?mode=ro", uri=True)
        try:
            return {r[0] for r in conn.execute("SELECT path FROM files")}
        finally:
            conn.close()

    def test_part_that_lost_all_rows_is_deleted_from_drive_after_the_catalog(self):
        self.start_with_pending_match()
        arrived = self.attach_detail()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual(summary["deleted"], [PENDING])
        self.assertEqual((summary["failed"], summary["delete_failed"]), (0, 0))
        self.assertTrue(summary["catalog_published"])
        # 手元・状態 DB・Drive のどこにも残らない
        self.assertFalse((self.root / "parts" / PENDING).exists())
        self.assertNotIn(PENDING, self.part_files())
        self.assertNotIn(PENDING, self.published())
        self.assertFalse((self.remote / PENDING).exists())
        # 移り先は Drive にあり、照合されている
        self.assertIn(arrived, self.published())
        self.assertEqual(hashlib.sha256((self.remote / arrived).read_bytes()).hexdigest(), self.part_files()[arrived])
        # rclone は run_rclone 経由で、リストの引数（シェルを通さない）として <remote>/<path> を一つ渡される
        self.assertEqual(self.deletefile_calls(),
                         [["/usr/bin/rclone-fake", "/nonexistent/rclone.conf", "deletefile",
                           f"gdrive:SQuidLite3/db/{PENDING}"]])
        # 順序: 移り先の部品を送って照合し、新しい目録（公開の確定点）を送って照合してから、古い部品を消す。
        # Drive 上の目録が Drive に無い部品を指す時間を作らない。
        events = self.fake.events
        self.assertLess(events.index(("copy", arrived)), events.index(("copy", "catalog.sqlite3")))
        self.assertLess(events.index(("copy", "catalog.sqlite3")), events.index(("delete", PENDING)))
        self.assertEqual(events[-1], ("delete", PENDING))
        self.assertEqual(self.fake.copied_order[-2:], ["README_FOR_AI.md", "catalog.sqlite3"])
        # Drive の目録は、消した部品を載せず、移り先を載せる
        listed = self.remote_catalog_files()
        self.assertNotIn(PENDING, listed)
        self.assertIn(arrived, listed)
        assert_audit_clean(self, audit(self.src.path, self.root / "parts"))

    def test_failed_deletion_stays_published_and_is_retried_next_cycle(self):
        self.start_with_pending_match()
        self.attach_detail()
        self.fake.delete_fail = {PENDING}
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["deleted"], summary["delete_failed"], summary["failed"]), ([], 1, 0))
        self.assertIn(PENDING, self.published())  # published に残る
        self.assertTrue((self.remote / PENDING).is_file())  # Drive にも残る
        self.assertNotIn(PENDING, self.part_files())  # もう部品ではない
        self.assertTrue(summary["catalog_published"])  # 消せなくても目録は送る（目録は消した部品を載せない）
        self.assertNotIn(PENDING, self.remote_catalog_files())
        self.assertEqual(len(self.deletefile_calls()), 1)
        # 次の周期: 再試行して消せる。部品は送らず、目録は変わらないので送り直さない
        self.fake.delete_fail = set()
        self.fake.calls.clear()
        self.fake.copied_order.clear()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["deleted"], summary["delete_failed"]), ([PENDING], 0))
        self.assertEqual((summary["rebuilt"], summary["uploaded"]), (0, 0))
        self.assertFalse(summary["catalog_published"])
        self.assertNotIn(PENDING, self.published())
        self.assertFalse((self.remote / PENDING).exists())
        self.assertEqual(self.fake.copied_order, [])
        # その次の周期: 消すものは無い。rclone は呼ばれない
        self.fake.calls.clear()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["deleted"], summary["delete_failed"]), ([], 0))
        self.assertEqual(self.fake.calls, [])

    def test_file_already_absent_on_drive_counts_as_deleted(self):
        self.start_with_pending_match()
        self.attach_detail()
        (self.remote / PENDING).unlink()  # Drive の側では既に無い（rclone は終了コード 4 で失敗する）
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["deleted"], summary["delete_failed"]), ([PENDING], 0))
        self.assertNotIn(PENDING, self.published())
        self.assertEqual(len(self.deletefile_calls()), 1)  # 呼んだ上で、既に無かったと分かった

    def test_other_rclone_errors_are_not_deletions(self):
        self.start_with_pending_match()
        self.attach_detail()
        self.fake.delete_code = {PENDING: 7}  # 致命的エラー（終了コード 7）は、消せたことにしない
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["deleted"], summary["delete_failed"]), ([], 1))
        self.assertIn(PENDING, self.published())
        self.assertTrue((self.remote / PENDING).is_file())

    def test_deletion_waits_until_every_part_is_verified(self):
        self.start_with_pending_match()
        arrived = self.attach_detail()
        self.fake.corrupt = {arrived}  # 移り先の部品が壊れて届く
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual((summary["deleted"], summary["delete_failed"]), ([], 0))
        self.assertFalse(summary["catalog_published"])
        self.assertEqual(self.deletefile_calls(), [])  # 移った行の送り先がそろうまで、元の部品を消さない
        self.assertTrue((self.remote / PENDING).is_file())
        self.assertIn(PENDING, self.published())
        # 次の周期: 部品がそろい、目録を送って照合してから消す
        self.fake.corrupt = set()
        self.fake.events.clear()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["failed"], summary["deleted"]), (0, [PENDING]))
        self.assertTrue(summary["catalog_published"])
        self.assertEqual(self.fake.events[-1], ("delete", PENDING))
        self.assertLess(self.fake.events.index(("copy", "catalog.sqlite3")), self.fake.events.index(("delete", PENDING)))

    def test_part_recreated_before_the_retry_is_sent_again_not_deleted(self):
        part = "unplaced/match_tags.sqlite3"  # seed の孤児タグだけが入っている
        parts_worker.run_cycle(self.opts)
        self.assertIn(part, self.published())
        self.src.execute("DELETE FROM match_tags WHERE match_key='nomatch'")
        self.fake.delete_fail = {part}
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["deleted"], summary["delete_failed"]), ([], 1))
        self.assertIn(part, self.published())
        # 消せないうちに、同じ住所へ行が戻って部品が作り直された: 消さずに送り直す
        self.src.execute("INSERT INTO match_tags VALUES('acc','another','再び孤児',NULL,'t','t')")
        self.fake.delete_fail = set()
        self.fake.calls.clear()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["deleted"], summary["delete_failed"], summary["failed"]), ([], 0, 0))
        self.assertEqual(self.deletefile_calls(), [])
        self.assertEqual(self.published()[part][0], self.part_files()[part])
        self.assertEqual(hashlib.sha256((self.remote / part).read_bytes()).hexdigest(), self.part_files()[part])
        assert_audit_clean(self, audit(self.src.path, self.root / "parts"))

    def test_nothing_but_removed_parts_is_ever_deleted(self):
        # 負の対照: 通常の周期（初回・何も変わらない・試合の追加）では deletefile を呼ばない
        parts_worker.run_cycle(self.opts)
        parts_worker.run_cycle(self.opts)
        conn = self.src.connect()
        with conn:
            self.src.next_id = 100
            self.src.match(conn, "m6")
        conn.close()
        parts_worker.run_cycle(self.opts)
        self.assertEqual(self.deletefile_calls(), [])
        published = self.published()
        self.assertIn("README_FOR_AI.md", published)
        self.assertIn("catalog.sqlite3", published)
        # published に、消してはならない記録を仕込む。目録・README のほか、部品でない記録、不正な住所、
        # 手元にファイルがある住所は、part_files に無くても消さない
        out = self.root / "parts"
        (out / "present.sqlite3").write_bytes(b"still here")
        planted = ["present.sqlite3", "notes/readme.txt", "../escape.sqlite3", "matches//double.sqlite3",
                   "back\\slash.sqlite3"]
        genuine = "matches/gone/orphan.sqlite3"  # published にあり、part_files に無く、手元にも無い
        conn = sqlite3.connect(self.opts.state)
        for path in planted + [genuine]:
            conn.execute("INSERT INTO published(path,sha256,bytes,published_at) VALUES(?,?,?,?)", (path, "x", 1, "t"))
        conn.commit()
        conn.close()
        self.fake.calls.clear()
        summary = parts_worker.run_cycle(self.opts)
        self.assertEqual(summary["deleted"], [genuine])
        self.assertEqual(self.deletefile_calls(),
                         [["/usr/bin/rclone-fake", "/nonexistent/rclone.conf", "deletefile",
                           f"gdrive:SQuidLite3/db/{genuine}"]])
        after = self.published()
        for path in planted + ["README_FOR_AI.md", "catalog.sqlite3"]:
            self.assertIn(path, after, path)
        self.assertNotIn(genuine, after)
        self.assertEqual((out / "present.sqlite3").read_bytes(), b"still here")

    def test_catalog_is_sent_again_when_a_part_left_the_catalog_even_if_nothing_else_changed(self):
        # 通常は変更追跡の through が進むので目録は送り直されるが、部品が目録から外れた周期は、それだけでも送り直す
        parts_worker.run_cycle(self.opts)
        real_build = parts_worker.build_parts

        def build_that_removed_a_part(*args, **kwargs):
            result = real_build(*args, **kwargs)
            result["removed_parts"] = ["unplaced/some-removed-part.sqlite3"]
            return result

        idle = parts_worker.run_cycle(self.opts)  # 対照: 何も変わらず、部品も外れない周期は目録を送らない
        self.assertFalse(idle["catalog_published"])
        with mock.patch.object(parts_worker, "build_parts", build_that_removed_a_part):
            summary = parts_worker.run_cycle(self.opts)
        self.assertEqual((summary["rebuilt"], summary["uploaded"]), (0, 0))
        self.assertTrue(summary["catalog_published"])

    def test_deletion_failure_makes_once_exit_one_and_removal_shows_in_the_json_line(self):
        self.start_with_pending_match()
        self.attach_detail()
        self.fake.delete_fail = {PENDING}
        code, text = self.run_main(self.argv())
        self.assertEqual(code, 1)
        data = json.loads(text)
        self.assertEqual((data["deleted"], data["delete_failed"], data["failed"]), ([], 1, 0))
        self.fake.delete_fail = set()
        code, text = self.run_main(self.argv())
        self.assertEqual(code, 0)
        data = json.loads(text)
        self.assertEqual((data["deleted"], data["delete_failed"]), ([PENDING], 0))


class HashsumParseTests(unittest.TestCase):
    def test_parse(self):
        h = "a" * 64
        self.assertEqual(
            parts_worker.parse_hashsum(f"{h}  matches/a b/x.sqlite3\nnot a line\n{h.upper()}  y.sqlite3\n"),
            {"matches/a b/x.sqlite3": h, "y.sqlite3": h},
        )


class RemotePathTests(unittest.TestCase):
    def test_remote_path(self):
        self.assertEqual(parts_worker.remote_path("gdrive:SQuidLite3/db", "a/b.sqlite3"), "gdrive:SQuidLite3/db/a/b.sqlite3")
        self.assertEqual(parts_worker.remote_path("gdrive:SQuidLite3/db/", "a/b.sqlite3"), "gdrive:SQuidLite3/db/a/b.sqlite3")
        self.assertEqual(parts_worker.remote_path("gdrive:", "a/b.sqlite3"), "gdrive:a/b.sqlite3")

    def test_part_addresses(self):
        ok = parts_worker._is_part_address
        self.assertTrue(ok("matches/xmatch/AREA/2026-10/2026-10-05/m1.sqlite3"))
        self.assertTrue(ok("unplaced/match_tags.sqlite3"))
        for bad in ("catalog.sqlite3", "README_FOR_AI.md", "notes/readme.txt", "../x.sqlite3", "/abs.sqlite3",
                    "a//b.sqlite3", "a/./b.sqlite3", "a\\b.sqlite3", "a/b.sqlite3\x00"):
            self.assertFalse(ok(bad), bad)


if __name__ == "__main__":
    unittest.main()
