import contextlib
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import nas_backup_scheduler as scheduler


JST = ZoneInfo('Asia/Tokyo')


class BackupSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp.name) / 'state'
        self.state_dir.mkdir(mode=0o700)
        self.current = [datetime(2026, 9, 27, 4, 0, tzinfo=JST)]
        self.calls = []
        self.outcomes = []

    def tearDown(self):
        self.temp.cleanup()

    def make_scheduler(self):
        def run(command, stop):
            self.calls.append(command)
            return self.outcomes.pop(0)

        return scheduler.Scheduler(
            self.state_dir, cycle_args=['--db', '/database/archive.sqlite3'],
            clock=lambda: self.current[0], runner=run,
        )

    def step(self, instance):
        with contextlib.redirect_stdout(io.StringIO()):
            return instance.step()

    def test_due_day_changes_at_four_not_midnight(self):
        for hour, minute, expected in ((0, 0, '2026-09-26'),
                                       (3, 59, '2026-09-26'),
                                       (4, 0, '2026-09-27')):
            instant = datetime(2026, 9, 27, hour, minute, tzinfo=JST)
            self.assertEqual(scheduler.latest_due_day(instant, 4, JST).isoformat(), expected)

    def test_tokyo_default_works_without_system_timezone_database(self):
        from unittest.mock import patch
        with patch.object(scheduler, 'ZoneInfo', side_effect=scheduler.ZoneInfoNotFoundError('missing')):
            zone = scheduler.resolve_timezone('Asia/Tokyo')
            self.assertEqual(datetime(2026, 9, 27, tzinfo=zone).utcoffset(), timedelta(hours=9))
            with self.assertRaises(scheduler.ZoneInfoNotFoundError):
                scheduler.resolve_timezone('Europe/Paris')

    def test_prior_success_is_not_repeated_until_next_due_day(self):
        scheduler.write_state(self.state_dir, {'last_success_due_day': '2026-09-26'})
        self.outcomes = [0, 0]
        instance = self.make_scheduler()
        self.step(instance)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][-2:], ['--db', '/database/archive.sqlite3'])
        self.assertEqual(scheduler.read_state(self.state_dir)['last_success_due_day'], '2026-09-27')
        self.assertGreater(self.step(instance), 0)
        self.assertEqual(len(self.calls), 1)
        self.current[0] += timedelta(days=1)
        self.step(instance)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(scheduler.read_state(self.state_dir)['last_success_due_day'], '2026-09-28')

    def test_failed_cycle_preserves_success_and_retries_after_fifteen_minutes(self):
        scheduler.write_state(self.state_dir, {'last_success_due_day': '2026-09-26'})
        self.outcomes = [9, 0]
        instance = self.make_scheduler()
        self.step(instance)
        failed = scheduler.read_state(self.state_dir)
        self.assertEqual(failed['last_success_due_day'], '2026-09-26')
        self.assertEqual(failed['last_failure_due_day'], '2026-09-27')
        self.assertEqual(failed['last_failure_code'], 9)
        self.assertEqual(self.step(instance), 900)
        self.current[0] += timedelta(minutes=14)
        self.assertEqual(self.step(instance), 60)
        self.assertEqual(len(self.calls), 1)
        self.current[0] += timedelta(minutes=1)
        self.step(instance)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(scheduler.read_state(self.state_dir)['last_success_due_day'], '2026-09-27')

    def test_failure_before_four_does_not_delay_new_due_day(self):
        self.current[0] = datetime(2026, 9, 27, 3, 59, tzinfo=JST)
        self.outcomes = [7, 0]
        instance = self.make_scheduler()
        self.step(instance)
        self.assertEqual(scheduler.read_state(self.state_dir)['last_failure_due_day'], '2026-09-26')
        self.current[0] += timedelta(minutes=1)
        self.step(instance)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(scheduler.read_state(self.state_dir)['last_success_due_day'], '2026-09-27')

    def test_clock_rollback_does_not_extend_failed_retry_unboundedly(self):
        self.current[0] = datetime(2026, 9, 27, 5, 0, tzinfo=JST)
        self.outcomes = [7, 0]
        instance = self.make_scheduler()
        self.step(instance)
        self.current[0] -= timedelta(hours=1)
        self.step(instance)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(scheduler.read_state(self.state_dir)['last_success_due_day'], '2026-09-27')

    def test_state_write_is_atomic_and_lock_is_nonblocking(self):
        scheduler.write_state(self.state_dir, {'last_success_due_day': '2026-09-26'})
        original = (self.state_dir / scheduler.STATE_NAME).read_bytes()
        from unittest.mock import patch
        with patch.object(scheduler.os, 'replace', side_effect=OSError('simulated rename failure')):
            with self.assertRaises(OSError):
                scheduler.write_state(self.state_dir, {'last_success_due_day': '2026-09-27'})
        self.assertEqual((self.state_dir / scheduler.STATE_NAME).read_bytes(), original)
        self.assertEqual(list(self.state_dir.glob('.scheduler-state-*')), [])
        first = scheduler.acquire_lock(self.state_dir)
        try:
            with self.assertRaises(BlockingIOError):
                scheduler.acquire_lock(self.state_dir)
        finally:
            os.close(first)

    def test_stop_terminates_running_child(self):
        stop = threading.Event()
        timer = threading.Timer(0.2, stop.set)
        timer.start()
        start = time.monotonic()
        try:
            code = scheduler.run_child([sys.executable, '-c', 'import time; time.sleep(60)'], stop)
        finally:
            timer.join()
        self.assertNotEqual(code, 0)
        self.assertLess(time.monotonic() - start, 5)


if __name__ == '__main__':
    unittest.main()
