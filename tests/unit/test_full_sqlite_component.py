import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'tests/unit'))
import stage_full_sqlite_component as component
import test_shard_reader as fixtures

class FakeRemote:
    def __init__(self):
        self.objects={};self.reads=[];self.writes=[];self.fail=False
    def stat(self,path):
        return {'IsDir':False,'Size':len(self.objects[path])} if path in self.objects else None
    def readback(self,path):
        self.reads.append(path)
        raw=self.objects[path]
        return len(raw),hashlib.sha256(raw).hexdigest()
    def copyto(self,path,target,*,immutable):
        if self.fail:raise component.publisher.PublishError('REMOTE_UPLOAD_FAILED')
        raw=Path(path).read_bytes()
        if immutable and target in self.objects and self.objects[target]!=raw:
            raise component.publisher.PublishError('REMOTE_UPLOAD_FAILED')
        self.objects[target]=raw;self.writes.append(target)
    def readback_bytes(self,path,size,sha):
        raw=self.objects[path]
        assert (len(raw),hashlib.sha256(raw).hexdigest())==(size,sha)
        self.reads.append(path)
        return raw

class SqliteComponentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures.LosslessShardReaderTests.setUpClass()
        cls.fixture=fixtures.LosslessShardReaderTests
    @classmethod
    def tearDownClass(cls):
        fixtures.LosslessShardReaderTests.tearDownClass()
    def run_stage(self,state,remote):
        with patch.object(component.publisher,'Rclone',return_value=remote):
            return component.stage_sqlite(self.fixture.package,fixtures.GENERATION,
                '2026-10-02T12:00:00Z','fake:database',state)
    def test_all_information_component_readback_and_idempotent_retry_without_latest(self):
        remote=FakeRemote();state=self.fixture.root/'component-state'
        state.mkdir(mode=0o700)
        first=self.run_stage(state,remote)
        self.assertEqual(first['status'],'verified')
        self.assertFalse(first['global_generation_complete'])
        self.assertFalse(first['realtime_synchronized'])
        self.assertTrue(all(not path.endswith('/latest.json') for path in remote.objects))
        index=[path for path in remote.objects if path.endswith('/sqlite-component-index.json')][0]
        doc=json.loads(remote.objects[index])
        self.assertTrue(doc['sqlite_verification']['coverage']['all_values'])
        self.assertTrue(doc['selector_verification']['all_shared_files_reachable'])
        self.assertEqual(len(doc['files']),first['files'])
        before=dict(remote.objects);remote.reads.clear();remote.writes.clear()
        second=self.run_stage(state,remote)
        self.assertEqual(first['index_sha256'],second['index_sha256'])
        self.assertEqual(before,remote.objects)
        self.assertEqual(remote.writes,[])
        self.assertEqual(set(remote.reads),set(remote.objects))
    def test_upload_failure_keeps_latest_and_has_no_completed_component_index(self):
        state=self.fixture.root/'component-failure-state';state.mkdir(mode=0o700)
        remote=FakeRemote();remote.objects['fake:database/latest.json']=b'previous'
        remote.fail=True
        with self.assertRaises(component.publisher.PublishError):
            self.run_stage(state,remote)
        self.assertEqual(remote.objects,{'fake:database/latest.json':b'previous'})
        self.assertFalse((state/f'sqlite-component-{fixtures.GENERATION}.verified.json').exists())
    def test_incomplete_component_rejected_before_any_external_call(self):
        state=self.fixture.root/'component-unsafe-state';state.mkdir(mode=0o700)
        package=self.fixture.root/'component-incomplete'
        import shutil
        shutil.copytree(self.fixture.package,package)
        (package/'verification.json').unlink()
        with patch.object(component.publisher,'Rclone') as client:
            with self.assertRaises(ValueError):
                component.stage_sqlite(package,fixtures.GENERATION,'2026-10-02T12:00:00Z',
                    'fake:database',state)
        client.assert_not_called()

if __name__=='__main__':
    unittest.main()
