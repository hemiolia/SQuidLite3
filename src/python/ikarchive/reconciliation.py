"""全量基準世代と現在の静止点の差を、全表・型を保持して列挙する。

変更記録導入前の基準世代には追跡できない時間帯がある。この初回照合を
省略してイベントの新しい分だけを配ると、その時間帯の変更が欠落する。
"""
from contextlib import AbstractContextManager
from datetime import datetime, timezone
import sqlite3

from .change_feed import read_change_batch


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _xinfo(connection, table):
    keys = ('cid', 'name', 'type', 'notnull', 'dflt_value', 'pk', 'hidden')
    return [dict(zip(keys, tuple(row))) for row in connection.execute(
        'PRAGMA table_xinfo(' + _quote(table) + ')')]


def _tables(connection):
    return {row[0]: row[1] for row in connection.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")}


def _rowid_alias(connection, table, columns):
    for row in connection.execute('PRAGMA table_list'):
        if row[0] == 'main' and row[1] == table:
            if row[2] != 'table' or row[4]:
                return None
            break
    used = {column.casefold() for column in columns}
    return next((alias for alias in ('_rowid_', 'rowid', 'oid')
                 if alias.casefold() not in used), None)


def _rows(connection, table, columns, alias):
    fields = ','.join(_quote(column) for column in columns)
    if alias:
        cursor = connection.execute('SELECT ' + _quote(alias) + ',' + fields
                                    + ' FROM ' + _quote(table) + ' ORDER BY ' + _quote(alias))
        for row in cursor:
            yield row[0], tuple(row[1:])
    else:
        # A key-only table is replaced in full, so its order has no semantic
        # meaning. No guessed key or collation is used for a merge.
        for row in connection.execute('SELECT ' + fields + ' FROM ' + _quote(table)):
            yield None, tuple(row)


def _equal(left, right):
    return (len(left) == len(right) and all(
        type(a) is type(b) and (a.hex() == b.hex() if isinstance(a, float) else a == b)
        for a, b in zip(left, right)))


class Reconciliation(AbstractContextManager):
    def __init__(self, current, baseline):
        if not isinstance(current, sqlite3.Connection) or not isinstance(baseline, sqlite3.Connection):
            raise TypeError('both inputs must be SQLite connections')
        if current is baseline:
            raise ValueError('baseline and current must be independent connections')
        if current.text_factory is not str or baseline.text_factory is not str:
            raise ValueError('standard str text_factory is required')
        self.current, self.baseline = current, baseline
        self._active = False
        self._owns_baseline = False
        self._batch = None
        self.metadata = {}

    def __enter__(self):
        if self._active:
            raise RuntimeError('reconciliation context is already active')
        try:
            self._batch = read_change_batch(self.current, 0)
            self._batch.__enter__()
            if not self.baseline.in_transaction:
                self.baseline.execute('BEGIN')
                self._owns_baseline = True
            self._before = _tables(self.baseline)  # Pins the baseline too.
            self._after = _tables(self.current)
            self._before_xinfo = {table: _xinfo(self.baseline, table) for table in self._before}
            self._after_xinfo = {table: _xinfo(self.current, table) for table in self._after}
            self.metadata = {
                **self._batch.metadata,
                'source_table_columns': self._after_xinfo,
                'kind': 'baseline_reconciliation',
                'captured_at': datetime.now(timezone.utc).isoformat(),
                'captured_at_kind': 'pinned_read_transaction',
                'removed_tables': sorted(set(self._before) - set(self._after)),
                'baseline_row_counts': {table: self.baseline.execute(
                    'SELECT COUNT(*) FROM ' + _quote(table)).fetchone()[0]
                    for table in self._before},
            }
            self._active = True
            return self
        except BaseException:
            self.__exit__(*__import__('sys').exc_info())
            raise

    def __exit__(self, exc_type, exc, traceback):
        self._active = False
        if self._owns_baseline and self.baseline.in_transaction:
            self.baseline.rollback()
        self._owns_baseline = False
        if self._batch is not None:
            self._batch.__exit__(exc_type, exc, traceback)
            self._batch = None
        return None

    def iter_current_changes(self):
        if not self._active:
            raise RuntimeError('consume reconciliation inside its context')
        for table in sorted(self._after):
            columns = [item['name'] for item in self._after_xinfo[table] if item['hidden'] != 1]
            alias = _rowid_alias(self.current, table, columns)
            before_columns = [item['name'] for item in self._before_xinfo.get(table, [])
                              if item['hidden'] != 1]
            before_alias = (_rowid_alias(self.baseline, table, before_columns)
                            if table in self._before else None)
            common = {'table_name': table, 'columns': columns, 'identity_json': None}
            if (not alias or not before_alias or self._before_xinfo[table] != self._after_xinfo[table]):
                yield {**common, 'operation': 'clear_table', 'source_rowid': None, 'values': None}
                for rowid, values in _rows(self.current, table, columns, alias):
                    yield {**common, 'operation': 'upsert', 'source_rowid': rowid, 'values': values}
                continue
            # Each cursor retains only one row, including its BLOB. Both sides
            # use the original rowid, so deletes and insertion gaps survive.
            before = iter(_rows(self.baseline, table, before_columns, before_alias))
            after = iter(_rows(self.current, table, columns, alias))
            old, new = next(before, None), next(after, None)
            while old is not None or new is not None:
                if new is None or (old is not None and old[0] < new[0]):
                    yield {**common, 'operation': 'delete', 'source_rowid': old[0], 'values': None}
                    old = next(before, None)
                elif old is None or new[0] < old[0]:
                    yield {**common, 'operation': 'upsert', 'source_rowid': new[0], 'values': new[1]}
                    new = next(after, None)
                else:
                    if not _equal(old[1], new[1]):
                        yield {**common, 'operation': 'upsert', 'source_rowid': new[0], 'values': new[1]}
                    old, new = next(before, None), next(after, None)


def read_reconciliation(current, baseline):
    """変更追跡導入前の空白を全表照合で埋めるcontext manager。"""
    return Reconciliation(current, baseline)
