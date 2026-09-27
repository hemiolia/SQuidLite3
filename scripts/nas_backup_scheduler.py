#!/usr/bin/env python3
"""Run the verified NAS backup once for each due local calendar day."""

import argparse
import fcntl
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import threading
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


RETRY_DELAY = timedelta(minutes=15)
STATE_NAME = 'scheduler-state.json'
LOCK_NAME = '.scheduler.lock'


def latest_due_day(now, hour, zone):
    local = now.astimezone(zone)
    return local.date() if local.hour >= hour else local.date() - timedelta(days=1)


def next_due_at(day, hour, zone):
    return datetime.combine(day + timedelta(days=1), time(hour), tzinfo=zone)


def resolve_timezone(name):
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        # The slim backup image may lack the system timezone database. Japan's
        # current civil time is fixed at UTC+09:00.
        if name == 'Asia/Tokyo':
            return timezone(timedelta(hours=9), name)
        raise


def _due_date(value):
    if not isinstance(value, str):
        raise ValueError('last_success_due_day must be an ISO calendar date')
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError('last_success_due_day must be an ISO calendar date')
    return parsed


def read_state(state_dir):
    path = state_dir / STATE_NAME
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('scheduler state must be a JSON object')
    if 'last_success_due_day' in data:
        _due_date(data['last_success_due_day'])
    if 'last_failure_due_day' in data:
        _due_date(data['last_failure_due_day'])
    if 'last_failure_at' in data:
        if not isinstance(data['last_failure_at'], str):
            raise ValueError('last_failure_at must include a timezone')
        stamp = datetime.fromisoformat(data['last_failure_at'])
        if stamp.tzinfo is None:
            raise ValueError('last_failure_at must include a timezone')
    return data


def write_state(state_dir, state):
    """Publish a complete state file and sync both file contents and rename."""
    payload = (json.dumps(state, ensure_ascii=False, sort_keys=True) + '\n').encode('utf-8')
    fd, temp_name = tempfile.mkstemp(prefix='.scheduler-state-', dir=state_dir)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'wb') as out:
            out.write(payload)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp_name, state_dir / STATE_NAME)
        dir_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def acquire_lock(state_dir):
    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(state_dir / LOCK_NAME, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        os.close(fd)
        raise
    return fd


def run_child(command, stop):
    """Keep the backup and its descendants in one signalable process group."""
    if stop.is_set():
        return 143
    try:
        child = subprocess.Popen(command, start_new_session=True)
    except OSError as exc:
        print(f'backup child could not start: {exc}', file=sys.stderr, flush=True)
        return 127
    while True:
        if stop.is_set():
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                code = child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                code = child.wait()
            # The cycle may have exited before a descendant did. Do not leave
            # that process group running after the scheduler stops.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return code
        try:
            return child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass


class Scheduler:
    def __init__(self, state_dir, hour=4, timezone_name='Asia/Tokyo', cycle_args=(),
                 clock=None, runner=run_child, stop=None):
        if not 0 <= hour <= 23:
            raise ValueError('--hour must be between 0 and 23')
        self.state_dir = pathlib.Path(state_dir)
        self.hour = hour
        self.zone = resolve_timezone(timezone_name)
        self.command = [sys.executable, str(pathlib.Path(__file__).with_name('nas_backup_cycle.py')),
                        *cycle_args]
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.runner = runner
        self.stop = stop or threading.Event()
        self.state = read_state(self.state_dir)

    def step(self):
        """Run a due backup if needed; return seconds until another check."""
        now = self.clock()
        due = latest_due_day(now, self.hour, self.zone)
        success = _due_date(self.state['last_success_due_day']) if 'last_success_due_day' in self.state else None
        if success is not None and success >= due:
            return max(0, (next_due_at(success, self.hour, self.zone) - now).total_seconds())

        failure_day = self.state.get('last_failure_due_day')
        if failure_day == due.isoformat() and 'last_failure_at' in self.state:
            failed_at = datetime.fromisoformat(self.state['last_failure_at'])
            retry_at = failed_at + RETRY_DELAY
            if failed_at <= now < retry_at:
                next_day = next_due_at(due, self.hour, self.zone)
                return max(0, min((retry_at - now).total_seconds(), (next_day - now).total_seconds()))

        if self.stop.is_set():
            return 0
        print(json.dumps({'event': 'backup_started', 'due_day': due.isoformat()}), flush=True)
        code = self.runner(self.command, self.stop)
        if self.stop.is_set():
            return 0

        finished = self.clock().astimezone(timezone.utc)
        if code == 0:
            self.state['last_success_due_day'] = due.isoformat()
            self.state['last_success_at'] = finished.isoformat()
            event = 'backup_succeeded'
        else:
            self.state['last_failure_due_day'] = due.isoformat()
            self.state['last_failure_at'] = finished.isoformat()
            self.state['last_failure_code'] = code
            event = 'backup_failed'
        write_state(self.state_dir, self.state)
        print(json.dumps({'event': event, 'due_day': due.isoformat(), 'exit_code': code}), flush=True)
        return 0

    def run_forever(self):
        while not self.stop.is_set():
            delay = self.step()
            if not self.stop.is_set():
                self.stop.wait(min(max(delay, 0), 60))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=pathlib.Path, required=True)
    parser.add_argument('--hour', type=int, default=4)
    parser.add_argument('--timezone', default='Asia/Tokyo')
    args, cycle_args = parser.parse_known_args(argv)
    if cycle_args[:1] == ['--']:
        cycle_args = cycle_args[1:]
    os.umask(0o077)
    try:
        lock_fd = acquire_lock(args.state_dir)
    except BlockingIOError:
        print('another backup scheduler holds the state lock', file=sys.stderr)
        return 1
    try:
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        scheduler = Scheduler(args.state_dir, args.hour, args.timezone, cycle_args, stop=stop)
        scheduler.run_forever()
        return 0
    finally:
        os.close(lock_fd)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, json.JSONDecodeError, ZoneInfoNotFoundError) as exc:
        print(f'backup scheduler failed: {exc}', file=sys.stderr)
        sys.exit(1)
