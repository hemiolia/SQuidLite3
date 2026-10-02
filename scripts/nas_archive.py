#!/usr/bin/env python3
"""Run archive commands against the NAS collector and fetch generated exports."""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
if __package__:
    from .data_root import client_mode, data_root, marker_path
else:
    from data_root import client_mode, data_root, marker_path

REMOTE_DB='/data/database/archive.sqlite3'
REMOTE_EXPORTS={'gui':'/data/exports/gui/index.html'}
LOCAL_EXPORTS={'gui':Path('gui/index.html')}
FORWARDED={'status','audit','verify','sql','tag','sync','records','slice-export','slice-list'}
BLOCKED={'backup','import','export','install-service','login','watch'}
SLICE_DESTINATION='/data/database/slices'
DATASET_RE=re.compile(r'(?:unified|mode:[a-z0-9_]{1,64}|rule:[a-z0-9_]{1,64}:[A-Za-z0-9_]{1,64})\Z')

def read_marker():
    path=marker_path(data_root())
    try:
        if path.is_symlink():raise ValueError('symlink')
        data=json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(data,dict) or type(data.get('schema_version')) is not int or data['schema_version']!=1 or data.get('backend')!='nas':
            raise ValueError('schema or backend')
        host=data.get('ssh_host')
        container=data.get('container')
        if not isinstance(host,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,252}',host):raise ValueError('ssh host')
        if not isinstance(container,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',container):raise ValueError('container')
        if data.get('database')!=REMOTE_DB:raise ValueError('database')
    except (OSError,UnicodeError,json.JSONDecodeError,ValueError,TypeError,KeyError) as exc:
        raise ValueError('NAS_STORAGE_MARKER_MISSING_OR_INVALID: '+str(path)) from exc
    return data

def ssh_command(marker, container_args):
    remote=shlex.join(['docker','exec','-i',marker['container'],*container_args])
    return ['ssh','-o','BatchMode=yes',marker['ssh_host'],remote]

def dataset_database(root_database, token='unified'):
    """固定の名前から、正本か既知の派生ファイルのパスを組み立てる。token はパスではない。"""
    if not isinstance(root_database, str) or not isinstance(token, str) or not DATASET_RE.fullmatch(token):
        raise ValueError('DATASET_TOKEN_INVALID')
    if token=='unified':
        return root_database
    parent, sep, name=root_database.rpartition('/')
    if not sep or not parent or not name or name in ('.','..') or parent.startswith('/') and '/../' in f'/{parent}/':
        raise ValueError('DATASET_TOKEN_INVALID')
    kind, _, rest=token.partition(':')
    if kind=='mode':
        return f'{parent}/slices/by-mode/{rest}.sqlite3'
    mode, _, rule=rest.partition(':')
    return f'{parent}/slices/by-rule/{mode}__{rule}.sqlite3'

def archive_command(marker, command, args, dataset='unified'):
    if marker.get('database')!=REMOTE_DB:
        raise ValueError('NAS_STORAGE_MARKER_MISSING_OR_INVALID')
    database=dataset_database(marker['database'], dataset)
    return ssh_command(marker,['python3','/app/archive.py','--db',database,command,*args])

def copy_remote_export(marker, remote_path, destination):
    destination.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    fd,tmp_name=tempfile.mkstemp(prefix='.'+destination.name+'.',suffix='.tmp',dir=destination.parent)
    tmp=Path(tmp_name)
    try:
        with os.fdopen(fd,'wb') as output:
            result=subprocess.run(ssh_command(marker,['cat',remote_path]),stdout=output)
            if result.returncode:return result.returncode
            output.flush()
            os.fsync(output.fileno())
            if output.tell()==0:raise ValueError('NAS_EXPORT_EMPTY: '+remote_path)
        os.replace(tmp,destination)
    finally:
        tmp.unlink(missing_ok=True)
    return 0

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--no-open',action='store_true',help='Do not open the downloaded GUI on macOS')
    parser.add_argument('command')
    parser.add_argument('args',nargs=argparse.REMAINDER)
    parsed=parser.parse_args(argv)
    command=parsed.command
    args=list(parsed.args)
    no_open=parsed.no_open
    if command=='export-xlsx':
        print('LEGACY_ANALYSIS_XLSX_RETIRED; use the full-data export workflow', file=sys.stderr)
        return 4
    if command=='gui' and '--no-open' in args:
        args.remove('--no-open')
        no_open=True
    if command in BLOCKED:
        parser.error(command+' requires a separate NAS path or daemon procedure and is not supported here')
    if command=='slice-export':
        if args:
            parser.error('slice-export writes only to the NAS database/slices directory and accepts no path')
        args=[SLICE_DESTINATION]
    if command=='slice-list':
        if args:
            parser.error('slice-list accepts no path')
    if command not in FORWARDED|set(REMOTE_EXPORTS):parser.error('unsupported command: '+command)
    if command in REMOTE_EXPORTS and args:parser.error(command+' accepts no destination or other arguments')
    if command!='gui' and no_open:parser.error('--no-open applies only to gui')
    marker=read_marker()
    result=subprocess.run(archive_command(marker,command,args))
    if result.returncode:return result.returncode
    if command in REMOTE_EXPORTS:
        destination=(Path.home()/'Library/Caches/ikaring-archive/exports' if client_mode()
                     else data_root()/'exports')/LOCAL_EXPORTS[command]
        result_code=copy_remote_export(marker,REMOTE_EXPORTS[command],destination)
        if result_code:return result_code
        print(str(destination))
        if command=='gui' and sys.platform=='darwin' and not no_open:
            return subprocess.run(['open',str(destination)]).returncode
    return 0

if __name__=='__main__':
    try:sys.exit(main())
    except (OSError,ValueError) as exc:
        print(str(exc),file=sys.stderr)
        sys.exit(1)
