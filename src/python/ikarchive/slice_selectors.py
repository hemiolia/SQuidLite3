"""全情報の共通SQLite断片を参照する、モード・ルール別の入口。

入口の試合だけを全保存情報の範囲と混同しない。各入口から全表・全値の
共通断片へ到達でき、統合SQLite本体には依存しない。
"""
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from datetime import datetime, timezone

MODE_SLICES = (
    'nawabari', 'bankara_open', 'bankara_challenge', 'event', 'xmatch', 'fest',
    'private_four_vs_four', 'private_three_vs_three', 'private_two_vs_two',
    'private_one_vs_one', 'private_other', 'salmon_regular', 'big_run',
    'team_contest', 'hold',
)
UNCLASSIFIED = 'unclassified'
_MODE = re.compile(r'[a-z0-9_]{1,64}\Z')
_RULE = re.compile(r'[A-Za-z0-9_]{1,64}\Z')


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def safe_path(root, relative):
    root = Path(root).absolute()
    part = Path(relative)
    if not isinstance(relative, str) or not relative or part.is_absolute() or '..' in part.parts or '\\' in relative:
        raise ValueError('SELECTOR_UNSAFE_PATH')
    path = root / part
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('SELECTOR_SYMLINK')
    path.relative_to(root)
    return path


def digest(path):
    h = hashlib.sha256()
    size = 0
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            size += len(block)
            h.update(block)
    return {'bytes': size, 'sha256': h.hexdigest()}


def rule_token(raw):
    if raw is None:
        return 'rule_unknown'
    if isinstance(raw, str) and _RULE.fullmatch(raw) and raw != 'rule_unknown' and not raw.startswith('rule_'):
        return raw
    if not isinstance(raw, str):
        raise ValueError('SELECTOR_INVALID_RULE')
    # Disjoint namespaces avoid collisions between an unknown rule and its name.
    return 'rule_' + hashlib.sha256(raw.encode('utf-8')).hexdigest()[:48]


def data_files(manifest):
    rows = [part for table in manifest['tables'] for part in table['parts']]
    rows += manifest['external_values']
    found = {}
    for row in rows:
        name = row['file']
        item = {key: row[key] for key in ('file', 'bytes', 'sha256')}
        if name in found:
            raise ValueError('SELECTOR_DUPLICATE_DEPENDENCY')
        found[name] = item
    return [found[name] for name in sorted(found)]


def _scope(mode, raw=None, rule_filtered=False):
    if mode == UNCLASSIFIED:
        return '''NOT EXISTS (SELECT 1 FROM match_classification c
            WHERE c.account=m.account AND c.kind=m.kind AND c.match_key=m.match_key)''', ()
    condition = 'c.analysis_set=?'
    params = (mode,)
    if rule_filtered:
        condition += ' AND c.rule_raw IS ?'
        params += (raw,)
    return '''EXISTS (SELECT 1 FROM match_classification c
        WHERE c.account=m.account AND c.kind=m.kind AND c.match_key=m.match_key
        AND ''' + condition + ')', params


def _seed_queries(mode, raw=None, rule_filtered=False):
    where, params = _scope(mode, raw, rule_filtered)
    matches = 'SELECT m.* FROM matches m WHERE ' + where + ' ORDER BY m.account,m.kind,m.match_key'
    classification = '''SELECT c.* FROM match_classification c WHERE EXISTS
        (SELECT 1 FROM matches m WHERE m.account=c.account AND m.kind=c.kind
         AND m.match_key=c.match_key AND ''' + where + ') ORDER BY c.account,c.kind,c.match_key'
    return {'matches': (matches, params), 'match_classification': (classification, params)}


def _new_selector(source, root, relative, manifest, source_sha, mode, raw=None, rule_filtered=False):
    path = safe_path(root, relative)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists():
        raise ValueError('SELECTOR_ALREADY_EXISTS')
    connection = sqlite3.connect(path)
    try:
        connection.executescript('''
            PRAGMA journal_mode=DELETE;
            PRAGMA synchronous=FULL;
            CREATE TABLE slice_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE shared_files(file TEXT PRIMARY KEY,bytes INTEGER NOT NULL,sha256 TEXT NOT NULL);
            CREATE TABLE archive_schema_objects(type TEXT,name TEXT,tbl_name TEXT,sql TEXT);
            CREATE TABLE archive_table_columns(table_name TEXT,column_ordinal INTEGER,definition_json TEXT,
                PRIMARY KEY(table_name,column_ordinal));
            CREATE TABLE archive_foreign_keys(table_name TEXT,id INTEGER,seq INTEGER,definition_json TEXT,
                PRIMARY KEY(table_name,id,seq));
        ''')
        meta = {
            'role': 'lossless_slice_selector', 'snapshot_identifier': manifest['snapshot_identifier'],
            'source_sha256': source_sha, 'source_schema_sha256': manifest['source_schema_sha256'],
            'axis': 'rule' if rule_filtered else 'mode', 'analysis_set': mode,
            'rule_raw_json': json.dumps(raw, ensure_ascii=False),
            'manifest': 'manifest.json', 'root_from_selector': '..',
            'information_scope': 'all_source_tables_and_values_via_shared_files',
        }
        connection.executemany('INSERT INTO slice_meta VALUES(?,?)', meta.items())
        connection.executemany('INSERT INTO shared_files VALUES(?,?,?)',
                               ((row['file'], row['bytes'], row['sha256']) for row in data_files(manifest)))
        connection.executemany('INSERT INTO archive_schema_objects VALUES(?,?,?,?)',
                               ((row['type'], row['name'], row['tbl_name'], row['sql']) for row in manifest['schema_objects']))
        for table in manifest['tables']:
            for ordinal, column in enumerate(table['column_schema']):
                connection.execute('INSERT INTO archive_table_columns VALUES(?,?,?)',
                                   (table['name'], ordinal, json.dumps(column, ensure_ascii=False, sort_keys=True)))
            for row in source.execute('PRAGMA foreign_key_list(' + quote(table['name']) + ')'):
                connection.execute('INSERT INTO archive_foreign_keys VALUES(?,?,?,?)',
                                   (table['name'], row[0], row[1], json.dumps(tuple(row), ensure_ascii=False)))
        for table, (query, params) in _seed_queries(mode, raw, rule_filtered).items():
            cursor = source.execute(query, params)
            names = [column[0] for column in cursor.description]
            connection.execute('CREATE TABLE ' + quote(table) + '(' + ','.join(quote(name) for name in names) + ')')
            insert = 'INSERT INTO ' + quote(table) + ' VALUES(' + ','.join('?' for _ in names) + ')'
            for row in cursor:
                connection.execute(insert, tuple(row))
        count = connection.execute('SELECT count(*) FROM matches').fetchone()[0]
        connection.commit()
        if connection.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise ValueError('SELECTOR_INTEGRITY_FAILED')
    finally:
        connection.close()
    os.chmod(path, 0o600)
    if path.stat().st_size > 20 * 1024 * 1024:
        raise ValueError('SELECTOR_EXCEEDS_SIZE_LIMIT')
    row = {'file': relative, 'analysis_set': mode, 'matches': count, **digest(path)}
    if rule_filtered:
        row['rule_raw'] = raw
    return row


def export_selectors(source, root, manifest, source_sha256):
    if manifest.get('role') != 'lossless_sqlite_shards' or manifest.get('version') != 2:
        raise ValueError('SELECTOR_REQUIRES_LOSSLESS_STORE')
    observed = [row[0] for row in source.execute('SELECT DISTINCT analysis_set FROM match_classification ORDER BY analysis_set')]
    if any(not isinstance(name, str) or not _MODE.fullmatch(name) for name in observed):
        raise ValueError('SELECTOR_INVALID_MODE')
    modes = list(dict.fromkeys((*MODE_SLICES, *observed, UNCLASSIFIED)))
    populated = [row[0] for row in source.execute('SELECT DISTINCT analysis_set FROM match_classification ORDER BY analysis_set')]
    rules = [row[0] for row in source.execute('SELECT DISTINCT rule_raw FROM match_classification ORDER BY rule_raw')]
    if len({rule_token(raw) for raw in rules}) != len(rules):
        raise ValueError('SELECTOR_RULE_TOKEN_COLLISION')
    by_mode = [_new_selector(source, root, 'by-mode/' + mode + '.sqlite3', manifest, source_sha256, mode) for mode in modes]
    by_rule = [_new_selector(source, root, 'by-rule/' + mode + '__' + rule_token(raw) + '.sqlite3',
                             manifest, source_sha256, mode, raw, True) for mode in populated for raw in rules]
    manifest.update(by_mode=by_mode, by_rule=by_rule, counts={
        'mode_files': len(by_mode), 'rule_files': len(by_rule), 'distinct_modes_with_matches': len(populated),
        'distinct_rules': len(rules), 'rule_mode_product': len(populated) * len(rules),
    })
    return manifest


def _same(a, b):
    if type(a) is not type(b):
        return False
    return a.hex() == b.hex() if isinstance(a, float) else a == b


def verify_selectors(source, root, manifest, source_sha256):
    expected_dependencies = [(row['file'], row['bytes'], row['sha256']) for row in data_files(manifest)]
    for dependency in data_files(manifest):
        if digest(safe_path(root, dependency['file'])) != {key: dependency[key] for key in ('bytes', 'sha256')}:
            raise ValueError('SELECTOR_DEPENDENCY_HASH_MISMATCH')
    modes = list(dict.fromkeys((*MODE_SLICES,
        *(row[0] for row in source.execute('SELECT DISTINCT analysis_set FROM match_classification ORDER BY analysis_set')),
        UNCLASSIFIED)))
    populated = [row[0] for row in source.execute('SELECT DISTINCT analysis_set FROM match_classification ORDER BY analysis_set')]
    rules = [row[0] for row in source.execute('SELECT DISTINCT rule_raw FROM match_classification ORDER BY rule_raw')]
    expected_modes = {'by-mode/' + mode + '.sqlite3' for mode in modes}
    expected_rules = {'by-rule/' + mode + '__' + rule_token(raw) + '.sqlite3' for mode in populated for raw in rules}
    if {row['file'] for row in manifest['by_mode']} != expected_modes or len(manifest['by_mode']) != len(expected_modes):
        raise ValueError('SELECTOR_MODE_COVERAGE_FAILED')
    if {row['file'] for row in manifest['by_rule']} != expected_rules or len(manifest['by_rule']) != len(expected_rules):
        raise ValueError('SELECTOR_RULE_COVERAGE_FAILED')
    for axis in ('by_mode', 'by_rule'):
        for item in manifest[axis]:
            path = safe_path(root, item['file'])
            if digest(path) != {key: item[key] for key in ('bytes', 'sha256')}:
                raise ValueError('SELECTOR_HASH_MISMATCH')
            connection = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)
            try:
                meta = dict(connection.execute('SELECT key,value FROM slice_meta'))
                if (meta.get('role') != 'lossless_slice_selector'
                    or meta.get('snapshot_identifier') != manifest['snapshot_identifier']
                    or meta.get('source_sha256') != source_sha256
                    or meta.get('source_schema_sha256') != manifest['source_schema_sha256']
                    or meta.get('axis') != ('rule' if axis == 'by_rule' else 'mode')
                    or meta.get('analysis_set') != item['analysis_set']
                    or meta.get('rule_raw_json') != json.dumps(item.get('rule_raw'), ensure_ascii=False)
                    or meta.get('information_scope') != 'all_source_tables_and_values_via_shared_files'
                    or meta.get('manifest') != 'manifest.json' or meta.get('root_from_selector') != '..'):
                    raise ValueError('SELECTOR_GENERATION_MISMATCH')
                if list(connection.execute('SELECT file,bytes,sha256 FROM shared_files ORDER BY file')) != expected_dependencies:
                    raise ValueError('SELECTOR_DEPENDENCY_COVERAGE_FAILED')
                expected_schema = [(row['type'], row['name'], row['tbl_name'], row['sql']) for row in manifest['schema_objects']]
                if list(connection.execute('SELECT type,name,tbl_name,sql FROM archive_schema_objects ORDER BY type,name')) != expected_schema:
                    raise ValueError('SELECTOR_SCHEMA_MISMATCH')
                expected_columns, expected_foreign_keys = [], []
                for table in manifest['tables']:
                    for ordinal, column in enumerate(table['column_schema']):
                        expected_columns.append((table['name'], ordinal, json.dumps(column, ensure_ascii=False, sort_keys=True)))
                    for key in source.execute('PRAGMA foreign_key_list(' + quote(table['name']) + ')'):
                        expected_foreign_keys.append((table['name'], key[0], key[1], json.dumps(tuple(key), ensure_ascii=False)))
                if list(connection.execute('SELECT table_name,column_ordinal,definition_json FROM archive_table_columns ORDER BY table_name,column_ordinal')) != sorted(expected_columns):
                    raise ValueError('SELECTOR_COLUMN_METADATA_MISMATCH')
                if list(connection.execute('SELECT table_name,id,seq,definition_json FROM archive_foreign_keys ORDER BY table_name,id,seq')) != sorted(expected_foreign_keys):
                    raise ValueError('SELECTOR_REFERENCE_METADATA_MISMATCH')
                for table, (query, params) in _seed_queries(item['analysis_set'], item.get('rule_raw'), axis == 'by_rule').items():
                    expected_cursor = source.execute(query, params)
                    found_cursor = connection.execute('SELECT * FROM ' + quote(table) + ' ORDER BY account,kind,match_key')
                    if [row[0] for row in expected_cursor.description] != [row[0] for row in found_cursor.description]:
                        raise ValueError('SELECTOR_SEED_COLUMN_MISMATCH')
                    expected, found = iter(expected_cursor), iter(found_cursor)
                    sentinel = object()
                    count = 0
                    while True:
                        a, b = next(expected, sentinel), next(found, sentinel)
                        if a is sentinel and b is sentinel:
                            break
                        if a is sentinel or b is sentinel or len(a) != len(b) or not all(_same(x, y) for x, y in zip(a, b)):
                            raise ValueError('SELECTOR_SEED_VALUES_MISMATCH')
                        count += 1
                    if table == 'matches' and (type(item.get('matches')) is not int or count != item['matches']):
                        raise ValueError('SELECTOR_MATCH_COUNT_MISMATCH')
            finally:
                connection.close()
    counts = manifest['counts']
    if counts != {'mode_files': len(expected_modes), 'rule_files': len(expected_rules),
                  'distinct_modes_with_matches': len(populated), 'distinct_rules': len(rules),
                  'rule_mode_product': len(populated) * len(rules)}:
        raise ValueError('SELECTOR_COUNTS_MISMATCH')
    return {'status': 'verified', 'snapshot_identifier': manifest['snapshot_identifier'],
            'source_sha256': source_sha256, 'all_shared_files_reachable': True,
            'mode_files': len(expected_modes), 'rule_files': len(expected_rules),
            'distinct_modes_with_matches': len(populated), 'distinct_rules': len(rules),
            'rule_mode_product': counts['rule_mode_product'], 'all_mode_matches': True,
            'all_rule_matches': True, 'verified_at': datetime.now(timezone.utc).isoformat()}
