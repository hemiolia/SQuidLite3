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
REMOTE_EXPORTS={'gui':'/data/exports/gui/index.html','export-xlsx':'/data/exports/分析.xlsx'}
LOCAL_EXPORTS={'gui':Path('gui/index.html'),'export-xlsx':Path('分析.xlsx')}
FORWARDED={'status','audit','verify','sql','tag','sync'}
BLOCKED={'backup','import','export','install-service','login','watch'}

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

def archive_command(marker, command, args):
    return ssh_command(marker,['python3','/app/archive.py','--db',marker['database'],command,*args])

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
    if command=='gui' and '--no-open' in args:
        args.remove('--no-open')
        no_open=True
    if command in BLOCKED:
        parser.error(command+' requires a separate NAS path or daemon procedure and is not supported here')
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
