"""成功した試合詳細を取り直さない・同じ試合の詳細取得の仕事を重複して作らない（2026-10-06 前田さんの判断）。

前田さんの判断: 取得に成功した試合詳細を取り直すこと自体が誤り。重複を作らない。

検査する仕様
  1. collector.sync は、成功（done）した仕事を時間が経っても pending に戻さない。
     一覧の起点（roots）の戻し（histories は毎回、ほかの roots は next_attempt 経過後）は従来どおり。
  2. 詳細取得の仕事は、同じ (account, kind, match_key) について、詳細が保存済み、または同じ詳細 operation の
     仕事がすでに jobs にあれば作らない。一覧ごとに ID の文字列が違っても（…:PRIVATE:… と …:RECENT:…）同じ試合。
     詳細取得の仕事を作る経路は二つある（Store._matches と、Store.project の planner.related）。
     どちらかの門が欠けると二つ目の仕事ができるので、二つの一覧に同じ試合を出す検査は両方を通る。
  3. 詳細取得の仕事が成功したら、同じ試合の同じ詳細 operation の pending・retry の仕事は superseded になる
     （同じトランザクション）。失敗（再試行・unavailable）では何も変えない。
実ネットワークは使わない。collector は偽の Bridge で通す。
"""
import json, tempfile, time, unittest
from pathlib import Path
from unittest.mock import patch
from test_archive import Store, Planner, MANIFEST, response, encoded
from ikarchive.collector import sync
from ikarchive.planner import identity
from ikarchive.store import js

USER='u-abcdefghijklmnopqrst'
VS='VsHistoryDetailQuery'
COOP='CoopHistoryDetailQuery'
FIELD={VS:'vsResultId',COOP:'coopHistoryDetailId'}
LISTING_ROOT={'LatestBattleHistoriesQuery':'latestBattleHistories','PrivateBattleHistoriesQuery':'privateBattleHistories','RegularBattleHistoriesQuery':'regularBattleHistories'}

def vs_id(mode,name):
    return encoded(f'VsHistoryDetail-{USER}:{mode}:20260922T010101_{name}')

def coop_id(name):
    return encoded(f'CoopHistoryDetail-{USER}:20260922T010101_{name}')

def key(name):
    return f'{USER}:20260922T010101_{name}'

def vs_listing(op,*ids):
    nodes=[{'__typename':'VsHistoryDetail','id':i} for i in ids]
    return {'data':{LISTING_ROOT[op]:{'historyGroups':{'nodes':[{'historyDetails':{'nodes':nodes}}]}}}}

def coop_listing(*ids):
    nodes=[{'__typename':'CoopHistoryDetail','id':i} for i in ids]
    return {'data':{'coopResult':{'historyGroups':{'nodes':[{'historyDetails':{'nodes':nodes}}]}}}}

def vs_detail(remote_id):
    return {'data':{'vsHistoryDetail':{'id':remote_id,'playedTime':'2026-09-22T01:01:01Z','judgement':'WIN'}}}

def coop_detail(remote_id):
    return {'data':{'coopHistoryDetail':{'id':remote_id,'rule':'REGULAR','playedTime':'2026-09-22T01:01:01Z','dangerRate':0}}}

class NoRefetchTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=Store(Path(self.tmp.name)/'a.sqlite')
        self.planner=Planner(MANIFEST)
    def tearDown(self):
        self.store.close();self.tmp.cleanup()

    # --- 道具 ---
    def ingest(self,op,body,variables=None,status=200,account='account-a'):
        rid=self.store.record(response(op,body,account=account,variables=variables,status=status))
        self.store.project(rid,self.planner);return rid
    def listing(self,op,*ids,account='account-a'):
        return self.ingest(op,vs_listing(op,*ids),account=account)
    def sight(self,op,*ids):
        """同じ試合を載せた一覧を取り込む（vs は戦績一覧、coop はバイトの一覧）。"""
        if op==VS:return self.listing('RegularBattleHistoriesQuery',*ids)
        return self.ingest('CoopHistoryQuery',coop_listing(*ids))
    def fetched(self,op,remote_id):
        """詳細取得が成功した応答を取り込む。"""
        body=vs_detail(remote_id) if op==VS else coop_detail(remote_id)
        return self.ingest(op,body,variables={FIELD[op]:remote_id})
    def jobs(self,op=VS,account='account-a'):
        rows=self.store.db.execute('SELECT * FROM jobs WHERE account=? AND operation=? ORDER BY variables_json',(account,op))
        return [dict(r) for r in rows]
    def states(self,name,op=VS,account='account-a'):
        """その試合（name）の、同じ詳細 operation の仕事を {変数の ID: state} で返す。"""
        return {json.loads(j['variables_json'])[FIELD[op]]:j['state'] for j in self.jobs(op,account) if j['match_key']==key(name)}
    def add_job(self,remote_id,name,state,op=VS,account='account-a'):
        """旧版が作っていた二つ目以降の仕事（別の一覧の ID 文字列）を、実データの形のまま足す。"""
        self.store.db.execute('INSERT INTO jobs(account,operation,variables_json,kind,match_key,state) VALUES(?,?,?,?,?,?)',
            (account,op,js({FIELD[op]:remote_id}),'vs' if op==VS else 'coop',key(name),state))
        self.store.db.commit()
    def detail_response_id(self,op,name):
        return self.store.db.execute('SELECT detail_response_id FROM matches WHERE kind=? AND match_key=?',('vs' if op==VS else 'coop',key(name))).fetchone()[0]
    def run_sync(self,operations,budget=10):
        """偽の Bridge で collector.sync を通す。戻りは (結果, 要求された [(operation, variables)])。"""
        seen=[];store=self.store
        manifest={**MANIFEST,'queries':{k:MANIFEST['queries'][k] for k in operations},'expected':len(operations)}
        class FakeBridge:
            def call(self,command,**kw):
                if command=='init':return {'account':'account-a','country':'JP'}
                op,variables=kw['operation'],kw['variables'];seen.append((op,variables))
                if op==VS:body=vs_detail(variables['vsResultId'])
                elif op=='HistoryRecordQuery':body={'data':{'playHistory':{}}}
                else:body={'data':{'regularBattleHistories':{'historyGroups':{'nodes':[]}}}}
                e=response(op,body,variables=variables)
                f=store.output_root/'spool'/(e['event_id']+'.json');f.write_text(json.dumps(e));return {'spool_file':str(f)}
            def close(self):pass
        with patch('ikarchive.collector.catalog',return_value=manifest),patch('ikarchive.collector.Bridge',FakeBridge),patch('ikarchive.collector.fetch_assets',return_value=0):
            result=sync(store,budget=budget,delay=0)
        return result,seen

    # --- 仕様 2: 同じ試合の詳細取得の仕事は一つだけ ---
    def test_one_match_in_two_listings_makes_one_detail_job(self):
        recent,private=vs_id('RECENT','m1'),vs_id('PRIVATE','m1')
        self.assertNotEqual(recent,private)
        self.assertEqual(identity(recent),identity(private))
        self.listing('LatestBattleHistoriesQuery',recent)
        self.listing('PrivateBattleHistoriesQuery',private)
        jobs=self.jobs()
        self.assertEqual(len(jobs),1)
        self.assertEqual((jobs[0]['kind'],jobs[0]['match_key'],jobs[0]['state']),('vs',key('m1'),'pending'))
        self.assertEqual(json.loads(jobs[0]['variables_json']),{'vsResultId':recent})
        # 取りこぼしは無い: 二つの ID も、二つの一覧への登場も残り、試合の行は一つのまま。
        self.assertEqual({r[0] for r in self.store.db.execute('SELECT remote_id FROM match_refs')},{recent,private})
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM sightings').fetchone()[0],2)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM matches').fetchone()[0],1)

    def test_control_without_the_gate_the_same_inputs_make_two_jobs(self):
        # 負の対照: 判定を常に「要る」にすると、同じ入力で仕事が二つできる（この検査の入力が重複を作れること）。
        recent,private=vs_id('RECENT','m1'),vs_id('PRIVATE','m1')
        with patch.object(Store,'_detail_job_needed',return_value=True):
            self.listing('LatestBattleHistoriesQuery',recent)
            self.listing('PrivateBattleHistoriesQuery',private)
        self.assertEqual(len(self.jobs()),2)

    def test_saved_detail_blocks_new_jobs_even_without_a_job_row(self):
        for op in (VS,COOP):
            with self.subTest(op=op):
                name='d1-'+op;fresh_name='d2-'+op
                first=vs_id('RECENT',name) if op==VS else coop_id(name)
                again=vs_id('REGULAR',name) if op==VS else first   # coop の ID の書き方は一つだけ
                fresh=vs_id('REGULAR',fresh_name) if op==VS else coop_id(fresh_name)
                self.sight(op,first)
                self.fetched(op,first)
                self.assertIsNotNone(self.detail_response_id(op,name))
                self.assertEqual(self.states(name,op),{first:'done'})
                # 詳細が成功したあとに、同じ試合が別の一覧に出ても、仕事は増えない。
                self.sight(op,again)
                self.assertEqual(self.states(name,op),{first:'done'})
                # 仕事の行が無くても、詳細が保存済みなら作らない。同じ一覧に出た詳細の無い新しい試合の仕事は作る。
                self.store.db.execute('DELETE FROM jobs WHERE operation=?',(op,));self.store.db.commit()
                self.sight(op,again,fresh)
                self.assertEqual(self.states(name,op),{})
                self.assertEqual(self.states(fresh_name,op),{fresh:'pending'})

    def test_existing_job_blocks_new_job_whatever_its_state(self):
        for state in ('pending','retry','unavailable','done','superseded'):
            with self.subTest(state=state):
                name='b-'+state
                self.listing('LatestBattleHistoriesQuery',vs_id('RECENT',name))
                self.store.db.execute('UPDATE jobs SET state=? WHERE match_key=?',(state,key(name)));self.store.db.commit()
                self.assertIsNone(self.detail_response_id(VS,name))
                self.listing('PrivateBattleHistoriesQuery',vs_id('PRIVATE',name))
                self.assertEqual(self.states(name),{vs_id('RECENT',name):state})

    def test_new_matches_still_get_their_job_per_account_and_kind(self):
        recent=vs_id('RECENT','n1');coop=coop_id('n1')
        self.listing('LatestBattleHistoriesQuery',recent)
        self.listing('LatestBattleHistoriesQuery',recent,account='account-b')
        self.ingest('CoopHistoryQuery',coop_listing(coop))
        for account in ('account-a','account-b'):
            jobs=self.jobs(VS,account)
            self.assertEqual(len(jobs),1)
            self.assertEqual((jobs[0]['kind'],jobs[0]['match_key'],jobs[0]['state'],json.loads(jobs[0]['variables_json'])),('vs',key('n1'),'pending',{'vsResultId':recent}))
        jobs=self.jobs(COOP)
        self.assertEqual(len(jobs),1)
        self.assertEqual((jobs[0]['kind'],jobs[0]['match_key'],jobs[0]['state'],json.loads(jobs[0]['variables_json'])),('coop',key('n1'),'pending',{'coopHistoryDetailId':coop}))
        # 詳細取得の二つの operation 以外の仕事は、従来どおり作る（仕様 4）。
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM jobs WHERE operation='CoopHistoryDetailRefetchQuery' AND match_key=?",(key('n1'),)).fetchone()[0],1)

    # --- 仕様 3: 詳細が成功したら、同じ試合のほかの未処理の仕事は superseded ---
    def test_detail_success_supersedes_only_unfetched_siblings_of_the_same_match(self):
        for op,other_op in ((VS,COOP),(COOP,VS)):
            with self.subTest(op=op):
                name='s-'+op;first=vs_id('RECENT',name) if op==VS else coop_id(name)
                self.sight(op,first)
                legacy={state:f'{name}-legacy-{state}' for state in ('pending','retry','unavailable','done')}
                for state,remote in legacy.items():self.add_job(remote,name,state,op)
                # 触ってはいけないもの: ほかの試合、ほかのアカウント、同じ試合キーの別の詳細 operation。
                self.add_job(name+'-other-match',name+'-other','pending',op)
                self.add_job(name+'-other-account',name,'pending',op,account='account-b')
                self.add_job(name+'-other-operation',name,'pending',other_op)
                self.assertEqual(self.states(name,op),{first:'pending',**{remote:state for state,remote in legacy.items()}})
                self.fetched(op,first)
                self.assertEqual(self.states(name,op),{first:'done',legacy['pending']:'superseded',legacy['retry']:'superseded',legacy['unavailable']:'unavailable',legacy['done']:'done'})
                self.assertEqual(self.states(name+'-other',op),{name+'-other-match':'pending'})
                self.assertEqual(self.states(name,op,'account-b'),{name+'-other-account':'pending'})
                self.assertEqual(self.states(name,other_op),{name+'-other-operation':'pending'})

    def test_failed_or_missing_detail_supersedes_nothing(self):
        cases=(
            ('errors',lambda i:{'data':{'vsHistoryDetail':{'id':i}},'errors':[{'message':'partial'}]},200,'retry'),
            ('http503',lambda i:{'errors':[{'message':'down'}]},503,'retry'),
            ('null_root',lambda i:{'data':{'vsHistoryDetail':None}},200,'unavailable'),
        )
        for label,body,status,expected in cases:
            with self.subTest(label=label):
                name='f-'+label;first=vs_id('RECENT',name)
                self.listing('LatestBattleHistoriesQuery',first)
                pending,retry=name+'-legacy-pending',name+'-legacy-retry'
                self.add_job(pending,name,'pending');self.add_job(retry,name,'retry')
                self.ingest(VS,body(first),variables={'vsResultId':first},status=status)
                self.assertEqual(self.states(name),{first:expected,pending:'pending',retry:'retry'})

    def test_supersede_is_in_the_same_transaction_as_the_job_result(self):
        recent,private=vs_id('RECENT','tx'),vs_id('PRIVATE','tx')
        self.listing('LatestBattleHistoriesQuery',recent)
        self.add_job(private,'tx','pending')
        with patch.object(Store,'_supersede_detail_jobs',side_effect=RuntimeError('boom')):
            with self.assertRaises(RuntimeError):
                self.fetched(VS,recent)
        # superseded にできなければ、done も付かない（片方だけが残らない）。
        self.assertEqual(self.states('tx'),{recent:'pending',private:'pending'})
        spool=Path(self.tmp.name)/'spool';spool.mkdir()
        self.store.recover(self.planner,spool)
        self.assertEqual(self.states('tx'),{recent:'done',private:'superseded'})

    def test_superseded_sibling_is_never_fetched_by_sync(self):
        recent,private=vs_id('RECENT','e2e'),vs_id('PRIVATE','e2e')
        self.listing('LatestBattleHistoriesQuery',recent)
        self.add_job(private,'e2e','pending')
        result,seen=self.run_sync(('RegularBattleHistoriesQuery',VS))
        self.assertEqual([op for op,_ in seen].count(VS),1)
        self.assertEqual(sorted(self.states('e2e').values()),['done','superseded'])
        self.assertEqual(result['pending_jobs'],0)

    # --- 仕様 1: 成功した仕事を毎日 pending に戻さない。roots の戻しは従来どおり ---
    def test_done_jobs_are_not_reset_as_time_passes(self):
        old,fresh=vs_id('RECENT','old'),vs_id('RECENT','fresh')
        self.listing('LatestBattleHistoriesQuery',old,fresh)
        self.fetched(VS,old)
        self.assertEqual(self.states('old'),{old:'done'})
        attempts=self.store.db.execute('SELECT attempts FROM jobs WHERE match_key=?',(key('old'),)).fetchone()[0]
        self.assertEqual(attempts,1)
        # 詳細以外の成功済みの仕事（ここでは続きのページ）も、戻されない。
        paging={'cursor':'next','first':10}
        self.store.queue('account-a','FestRecordPaginationQuery',paging)
        self.store.db.execute("UPDATE jobs SET state='done' WHERE operation='FestRecordPaginationQuery'")
        # 三日が過ぎた: 成功済みの仕事の next_attempt はすべて過去。
        self.store.db.execute("UPDATE jobs SET next_attempt=? WHERE state='done'",(time.time()-3*86400,));self.store.db.commit()
        _,seen=self.run_sync(('RegularBattleHistoriesQuery','HistoryRecordQuery',VS))
        # 成功済みの詳細は要求されず、未取得の詳細だけが要求される（この検査が詳細の要求を観測できること）。
        self.assertEqual([v for op,v in seen if op==VS],[{'vsResultId':fresh}])
        job=[j for j in self.jobs() if json.loads(j['variables_json'])['vsResultId']==old][0]
        self.assertEqual((job['state'],job['attempts']),('done',attempts))
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE operation='FestRecordPaginationQuery' AND variables_json=?",(js(paging),)).fetchone()[0],'done')

    def test_roots_are_still_reset_histories_every_pass_others_after_next_attempt(self):
        ops=('RegularBattleHistoriesQuery','HistoryRecordQuery')
        _,seen=self.run_sync(ops)
        self.assertEqual([op for op,_ in seen],['RegularBattleHistoriesQuery','HistoryRecordQuery'])
        # 直後の二回目: histories は done でも毎回戻る。ほかの roots は next_attempt（成功の 24 時間後）まで戻らない。
        _,seen=self.run_sync(ops)
        self.assertEqual([op for op,_ in seen],['RegularBattleHistoriesQuery'])
        # next_attempt が過ぎたら、ほかの roots も戻る。
        self.store.db.execute("UPDATE jobs SET next_attempt=1 WHERE operation='HistoryRecordQuery'");self.store.db.commit()
        _,seen=self.run_sync(ops)
        self.assertEqual([op for op,_ in seen],['RegularBattleHistoriesQuery','HistoryRecordQuery'])

    def test_retry_jobs_are_still_retried_when_due(self):
        due,later=vs_id('RECENT','due'),vs_id('RECENT','later')
        self.listing('LatestBattleHistoriesQuery',due,later)
        self.store.db.execute("UPDATE jobs SET state='retry',attempts=1,next_attempt=? WHERE match_key=?",(time.time()-10,key('due')))
        self.store.db.execute("UPDATE jobs SET state='retry',attempts=1,next_attempt=? WHERE match_key=?",(time.time()+3000,key('later')));self.store.db.commit()
        _,seen=self.run_sync(('RegularBattleHistoriesQuery',VS))
        self.assertEqual([v for op,v in seen if op==VS],[{'vsResultId':due}])
        self.assertEqual(self.states('due'),{due:'done'})
        self.assertEqual(self.states('later'),{later:'retry'})

if __name__=='__main__':
    unittest.main()
