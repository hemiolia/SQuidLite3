import contextlib
import io
import json
import os
import shlex
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
import archive
from scripts import nas_archive

MARKER={'schema_version':1,'backend':'nas','ssh_host':'nas','container':'ikaring-archive','database':'/data/database/archive.sqlite3'}

class NasRoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.marker=self.root/'config'/'storage-location.json'
        self.marker.parent.mkdir()
        self.marker.write_text(json.dumps(MARKER),encoding='utf-8')
        self.env=patch.dict(os.environ,{'IKARING_ARCHIVE_DATA_DIR':str(self.root)})
        self.env.start()
        self.output=patch.object(archive,'OUTPUT',self.root)
        self.default=patch.object(archive,'DEFAULT',self.root/'database'/'archive.sqlite3')
        self.output.start();self.default.start()
    def tearDown(self):
        self.default.stop();self.output.stop();self.env.stop();self.tmp.cleanup()

    def archive_main(self,*args):
        with patch.object(sys,'argv',['archive.py',*args]):return archive.main()

    def test_default_database_is_guarded_before_creation(self):
        for args in [('sync',),('watch',),('init',),('status',),('--db',str(archive.DEFAULT),'sync')]:
            with self.subTest(args=args),self.assertRaisesRegex(ValueError,'nas_archive.py'):
                self.archive_main(*args)
        self.assertFalse((self.root/'database').exists())

    def test_invalid_marker_fails_closed_and_login_remains_local(self):
        for content in ['{',json.dumps({**MARKER,'backend':'local'}),json.dumps({**MARKER,'schema_version':True}),json.dumps({**MARKER,'ssh_host':'-bad'})]:
            self.marker.write_text(content,encoding='utf-8')
            with self.subTest(content=content),self.assertRaisesRegex(ValueError,'NAS_STORAGE_MARKER_INVALID'):
                self.archive_main('sync')
            self.assertFalse((self.root/'database').exists())
        with patch.object(archive.subprocess,'call',return_value=0) as login:
            self.assertEqual(self.archive_main('login'),0)
            login.assert_called_once()

    def test_explicit_other_database_is_unchanged(self):
        other=self.root/'other.sqlite3'
        with patch.object(sys,'argv',['archive.py','--db',str(other),'init']):
            self.assertEqual(archive.main(),0)
        self.assertTrue(other.is_file())
        self.assertFalse((self.root/'database').exists())

    def test_wrapper_forwards_sql_as_one_quoted_remote_argument(self):
        sql="SELECT 'quoted'; $(touch /tmp/must-not-run)"
        calls=[]
        def run(command,**kwargs):
            calls.append(command)
            return type('Result',(),{'returncode':0})()
        with patch.object(nas_archive.subprocess,'run',side_effect=run):
            self.assertEqual(nas_archive.main(['sql',sql]),0)
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0][:5],['ssh','-o','BatchMode=yes','nas',calls[0][4]])
        self.assertEqual(shlex.split(calls[0][4]),['docker','exec','-i','ikaring-archive','python3','/app/archive.py','--db','/data/database/archive.sqlite3','sql',sql])

    def test_wrapper_rejects_missing_or_unsafe_marker_without_ssh(self):
        with patch.object(nas_archive.subprocess,'run') as run:
            for data in [{**MARKER,'container':'-bad'},{**MARKER,'database':'/tmp/x'},{**MARKER,'backend':'local'}]:
                self.marker.write_text(json.dumps(data),encoding='utf-8')
                with self.assertRaisesRegex(ValueError,'NAS_STORAGE_MARKER_MISSING_OR_INVALID'):
                    nas_archive.main(['status'])
            self.marker.unlink()
            with self.assertRaisesRegex(ValueError,'NAS_STORAGE_MARKER_MISSING_OR_INVALID'):
                nas_archive.main(['status'])
            run.assert_not_called()

    def test_export_fetch_replaces_only_after_complete_transfer(self):
        dest=self.root/'exports/gui/index.html'
        dest.parent.mkdir(parents=True)
        dest.write_bytes(b'old')
        calls=[]
        def success(command,**kwargs):
            calls.append(command)
            if 'stdout' in kwargs:kwargs['stdout'].write(b'<html>new</html>')
            return type('Result',(),{'returncode':0})()
        with patch.object(nas_archive.subprocess,'run',side_effect=success):
            self.assertEqual(nas_archive.main(['gui','--no-open']),0)
        self.assertEqual(dest.read_bytes(),b'<html>new</html>')
        self.assertEqual(len(calls),2)
        self.assertEqual(shlex.split(calls[1][4])[-2:],['cat','/data/exports/gui/index.html'])
        def failed_cat(command,**kwargs):
            if 'stdout' in kwargs:
                kwargs['stdout'].write(b'partial')
                return type('Result',(),{'returncode':23})()
            return type('Result',(),{'returncode':0})()
        with patch.object(nas_archive.subprocess,'run',side_effect=failed_cat):
            self.assertEqual(nas_archive.main(['gui','--no-open']),23)
        self.assertEqual(dest.read_bytes(),b'<html>new</html>')
        self.assertFalse(list(dest.parent.glob('.*.tmp')))

    def test_retired_xlsx_route_never_starts_ssh_or_changes_local_file(self):
        destination=self.root/'分析.xlsx'
        destination.write_bytes(b'legacy workbook bytes remain untouched')
        destination.chmod(0o640)
        before=(destination.read_bytes(),destination.stat().st_mode&0o777,destination.stat().st_mtime_ns)
        old_cwd=Path.cwd()
        stdout,stderr=io.StringIO(),io.StringIO()
        try:
            os.chdir(self.root)
            with patch.object(nas_archive.subprocess,'run') as run, \
                 contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):
                code=nas_archive.main(['export-xlsx'])
            run.assert_not_called()
        finally:
            os.chdir(old_cwd)
        self.assertEqual(code,4)
        self.assertEqual(stdout.getvalue(),'')
        self.assertIn('LEGACY_ANALYSIS_XLSX_RETIRED',stderr.getvalue())
        self.assertEqual((destination.read_bytes(),destination.stat().st_mode&0o777,destination.stat().st_mtime_ns),before)

    def test_slice_export_uses_only_the_nas_slices_directory(self):
        calls=[]
        def run(command,**kwargs):
            calls.append(command)
            return type('Result',(),{'returncode':0})()
        with patch.object(nas_archive.subprocess,'run',side_effect=run):
            self.assertEqual(nas_archive.main(['slice-export']),0)
        self.assertEqual(len(calls),1)
        self.assertEqual(shlex.split(calls[0][4])[-2:],['slice-export','/data/database/slices'])
        with patch.object(nas_archive.subprocess,'run') as run:
            with self.assertRaises(SystemExit):
                nas_archive.main(['slice-export','/tmp/elsewhere'])
            run.assert_not_called()

    def test_slice_list_accepts_no_path(self):
        calls=[]
        def run(command,**kwargs):
            calls.append(command)
            return type('Result',(),{'returncode':0})()
        with patch.object(nas_archive.subprocess,'run',side_effect=run):
            self.assertEqual(nas_archive.main(['slice-list']),0)
        remote=shlex.split(calls[0][4])
        self.assertEqual(remote[-3:],['--db','/data/database/archive.sqlite3','slice-list'])
        with patch.object(nas_archive.subprocess,'run') as run:
            with self.assertRaises(SystemExit):
                nas_archive.main(['slice-list','/tmp/elsewhere'])
            run.assert_not_called()

    def test_blocked_path_commands_never_start_ssh(self):
        with patch.object(nas_archive.subprocess,'run') as run:
            for command in sorted(nas_archive.BLOCKED):
                with self.subTest(command=command),self.assertRaises(SystemExit):
                    nas_archive.main([command])
            run.assert_not_called()

if __name__=='__main__':unittest.main()
