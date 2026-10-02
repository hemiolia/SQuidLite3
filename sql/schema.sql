PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY);
INSERT OR IGNORE INTO schema_version VALUES(1);
CREATE TABLE IF NOT EXISTS runs(id INTEGER PRIMARY KEY,started_at TEXT NOT NULL,finished_at TEXT,status TEXT NOT NULL,account TEXT,error TEXT);
CREATE TABLE IF NOT EXISTS bodies(sha256 TEXT PRIMARY KEY,body BLOB NOT NULL,byte_length INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS responses(
 id INTEGER PRIMARY KEY, event_id TEXT UNIQUE NOT NULL,run_id INTEGER REFERENCES runs(id),account TEXT NOT NULL,
 fetched_at TEXT NOT NULL,operation TEXT NOT NULL,variables_json TEXT NOT NULL,query_id TEXT,app_version TEXT,
 http_status INTEGER,headers_json TEXT NOT NULL,body_sha256 TEXT NOT NULL REFERENCES bodies(sha256),
 json_text TEXT,parse_error TEXT,projected INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS response_operation ON responses(account,operation,fetched_at);
CREATE TABLE IF NOT EXISTS issues(id INTEGER PRIMARY KEY,run_id INTEGER REFERENCES runs(id),response_id INTEGER REFERENCES responses(id),code TEXT NOT NULL,context TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS matches(account TEXT NOT NULL,kind TEXT NOT NULL,match_key TEXT NOT NULL,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,detail_response_id INTEGER REFERENCES responses(id),PRIMARY KEY(account,kind,match_key));
CREATE TABLE IF NOT EXISTS match_refs(account TEXT NOT NULL,kind TEXT NOT NULL,remote_id TEXT NOT NULL,match_key TEXT NOT NULL,PRIMARY KEY(account,kind,remote_id),FOREIGN KEY(account,kind,match_key) REFERENCES matches(account,kind,match_key));
CREATE TABLE IF NOT EXISTS sightings(response_id INTEGER NOT NULL REFERENCES responses(id),account TEXT NOT NULL,kind TEXT NOT NULL,match_key TEXT NOT NULL,path TEXT NOT NULL,summary_json TEXT NOT NULL,PRIMARY KEY(response_id,path));
CREATE TABLE IF NOT EXISTS documents(response_id INTEGER NOT NULL REFERENCES responses(id),account TEXT NOT NULL,kind TEXT NOT NULL,match_key TEXT NOT NULL,json_text TEXT NOT NULL,PRIMARY KEY(response_id,kind,match_key));
CREATE TABLE IF NOT EXISTS jobs(account TEXT NOT NULL,operation TEXT NOT NULL,variables_json TEXT NOT NULL,kind TEXT,match_key TEXT,state TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,empty_retries INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,last_response_id INTEGER REFERENCES responses(id),PRIMARY KEY(account,operation,variables_json));
CREATE TABLE IF NOT EXISTS endpoint_heads(account TEXT NOT NULL,operation TEXT NOT NULL,response_id INTEGER NOT NULL REFERENCES responses(id),PRIMARY KEY(account,operation));
CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS match_classification(
 account TEXT NOT NULL,kind TEXT NOT NULL,match_key TEXT NOT NULL,
 genre TEXT NOT NULL,mode_raw TEXT,bankara_mode TEXT,rule_raw TEXT,rule_name TEXT,
 roster_class TEXT NOT NULL,analysis_set TEXT NOT NULL,team_count INTEGER,my_player_count INTEGER,
 opponent_counts TEXT,detail_response_id INTEGER,classified_at TEXT NOT NULL,
 PRIMARY KEY(account,kind,match_key),
 FOREIGN KEY(account,kind,match_key) REFERENCES matches(account,kind,match_key));
CREATE TABLE IF NOT EXISTS rate_points(
 account TEXT NOT NULL,series_id TEXT NOT NULL,label TEXT NOT NULL,genre TEXT NOT NULL,rule_raw TEXT,
 match_key TEXT NOT NULL,played_time TEXT,value REAL,source TEXT NOT NULL,priority TEXT NOT NULL,
 PRIMARY KEY(account,series_id,match_key));
CREATE TABLE IF NOT EXISTS match_tags(
 account TEXT NOT NULL,match_key TEXT NOT NULL,tag TEXT NOT NULL,note TEXT,
 created_at TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(account,match_key,tag));
CREATE TABLE IF NOT EXISTS analysis_genre(genre TEXT PRIMARY KEY,label TEXT NOT NULL,top_level INTEGER NOT NULL);
INSERT INTO analysis_genre(genre,label,top_level) VALUES
 ('nawabari','ナワバリ',1),('bankara_open','オープン',1),('bankara_challenge','チャレンジ',1),
 ('private','プラベ',1),('event','イベマ',1),('xmatch','Xマッチ',1),('fest','フェス',1),
 ('salmon_regular','バイト',1),('big_run','ビッグラン',1),('team_contest','バイトチームコンテスト',1),
 ('bankara_unspecified','バンカラ（区分不明）',0),('unknown','不明',0)
 ON CONFLICT(genre) DO UPDATE SET label=excluded.label,top_level=excluded.top_level;
CREATE VIEW IF NOT EXISTS all_fields AS
 SELECT r.id AS response_id,r.account,r.operation,j.fullkey,j.path,j.type,j.atom,j.value
 FROM responses r,json_tree(r.json_text) j WHERE r.json_text IS NOT NULL;
CREATE VIEW IF NOT EXISTS match_details AS
 SELECT m.account,m.kind,m.match_key,m.first_seen,m.last_seen,m.detail_response_id,d.json_text
 FROM matches m JOIN documents d ON d.response_id=m.detail_response_id AND d.account=m.account AND d.kind=m.kind AND d.match_key=m.match_key;
DROP VIEW IF EXISTS battles;
CREATE VIEW battles AS
 SELECT d.account,d.match_key,d.detail_response_id,json_extract(d.json_text,'$.playedTime') played_time,
 json_extract(d.json_text,'$.vsMode.mode') mode,json_extract(d.json_text,'$.vsRule.name') rule,
 json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,
 json_extract(d.json_text,'$.duration') duration,json_extract(d.json_text,'$.knockout') knockout,
 c.genre,c.roster_class,c.analysis_set,d.json_text
 FROM match_details d LEFT JOIN match_classification c ON c.account=d.account AND c.kind=d.kind AND c.match_key=d.match_key
 WHERE d.kind='vs';
CREATE VIEW IF NOT EXISTS battle_teams AS
 SELECT account,match_key,0 team_index,1 is_my_team,json_extract(json_text,'$.myTeam') json_text FROM match_details WHERE kind='vs' AND json_type(json_text,'$.myTeam')='object'
 UNION ALL SELECT d.account,d.match_key,CAST(t.key AS INTEGER)+1,0,t.value FROM match_details d,json_each(d.json_text,'$.otherTeams') t WHERE d.kind='vs';
CREATE VIEW IF NOT EXISTS battle_players AS
 SELECT t.account,t.match_key,t.team_index,t.is_my_team,p.key player_index,
 json_extract(p.value,'$.id') player_id,json_extract(p.value,'$.name') name,json_extract(p.value,'$.isMyself') is_myself,
 json_extract(p.value,'$.weapon.name') weapon,json_extract(p.value,'$.paint') paint,
 json_extract(p.value,'$.result.kill') kills,json_extract(p.value,'$.result.assist') assists,
 json_extract(p.value,'$.result.death') deaths,json_extract(p.value,'$.result.special') specials,p.value json_text
 FROM battle_teams t,json_each(t.json_text,'$.players') p;
CREATE VIEW IF NOT EXISTS battle_awards AS
 SELECT d.account,d.match_key,a.key award_index,a.value json_text FROM match_details d,json_each(d.json_text,'$.awards') a WHERE d.kind='vs';
CREATE VIEW IF NOT EXISTS battle_gear AS
 SELECT p.account,p.match_key,p.team_index,p.player_index,g.key slot,g.value json_text
 FROM battle_players p,json_each(p.json_text) g WHERE g.key IN ('headGear','clothingGear','shoesGear');
CREATE VIEW IF NOT EXISTS salmon_runs AS
 SELECT account,match_key,detail_response_id,json_extract(json_text,'$.playedTime') played_time,
 json_extract(json_text,'$.coopStage.name') stage,json_extract(json_text,'$.rule') rule,
 json_extract(json_text,'$.dangerRate') danger_rate,json_extract(json_text,'$.resultWave') result_wave,json_text
 FROM match_details WHERE kind='coop';
CREATE VIEW IF NOT EXISTS salmon_players AS
 SELECT account,match_key,0 player_index,1 is_myself,json_extract(json_text,'$.myResult') json_text FROM match_details WHERE kind='coop' AND json_type(json_text,'$.myResult')='object'
 UNION ALL SELECT d.account,d.match_key,CAST(p.key AS INTEGER)+1,0,p.value FROM match_details d,json_each(d.json_text,'$.memberResults') p WHERE d.kind='coop';
CREATE VIEW IF NOT EXISTS salmon_waves AS
 SELECT d.account,d.match_key,w.key wave_index,w.value json_text FROM match_details d,json_each(d.json_text,'$.waveResults') w WHERE d.kind='coop';
CREATE VIEW IF NOT EXISTS salmon_bosses AS
 SELECT d.account,d.match_key,b.key boss_index,b.value json_text FROM match_details d,json_each(d.json_text,'$.enemyResults') b WHERE d.kind='coop';
DROP VIEW IF EXISTS pending_details;
DROP VIEW IF EXISTS unavailable_details;
CREATE VIEW pending_details AS
 SELECT m.* FROM matches m
 WHERE m.detail_response_id IS NULL
 AND EXISTS(
  SELECT 1 FROM jobs j
  WHERE j.account=m.account AND j.match_key=m.match_key AND j.state IN ('pending','retry')
 );
CREATE VIEW unavailable_details AS
 SELECT m.* FROM matches m
 WHERE m.detail_response_id IS NULL
 AND EXISTS(
  SELECT 1 FROM jobs j
  WHERE j.account=m.account AND j.match_key=m.match_key AND j.state='unavailable'
 )
 AND NOT EXISTS(
  SELECT 1 FROM jobs j
  WHERE j.account=m.account AND j.match_key=m.match_key AND j.state IN ('pending','retry')
 );
CREATE TABLE IF NOT EXISTS entities(account TEXT NOT NULL,typename TEXT NOT NULL,entity_id TEXT NOT NULL,response_id INTEGER NOT NULL REFERENCES responses(id),json_text TEXT NOT NULL,PRIMARY KEY(account,typename,entity_id));
CREATE INDEX IF NOT EXISTS entities_response_id ON entities(response_id);
CREATE TABLE IF NOT EXISTS page_fingerprints(account TEXT NOT NULL,operation TEXT NOT NULL,binding TEXT NOT NULL,field_path TEXT NOT NULL,sha256 TEXT NOT NULL,PRIMARY KEY(account,operation,binding,field_path,sha256));
CREATE TABLE IF NOT EXISTS assets(url TEXT PRIMARY KEY,state TEXT NOT NULL DEFAULT 'pending',body_sha256 TEXT REFERENCES bodies(sha256),content_type TEXT,attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,last_error TEXT);
CREATE TABLE IF NOT EXISTS asset_refs(response_id INTEGER NOT NULL REFERENCES responses(id),url TEXT NOT NULL REFERENCES assets(url),path TEXT NOT NULL,PRIMARY KEY(response_id,path));
CREATE TABLE IF NOT EXISTS manifests(sha256 TEXT PRIMARY KEY,fetched_at TEXT NOT NULL,json_text TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS response_fetches(
 event_id TEXT PRIMARY KEY,response_id INTEGER NOT NULL REFERENCES responses(id),
 run_id INTEGER REFERENCES runs(id),fetched_at TEXT NOT NULL,headers_json TEXT NOT NULL,
 acknowledged INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS response_fetches_response ON response_fetches(response_id,fetched_at);
INSERT OR IGNORE INTO response_fetches(event_id,response_id,run_id,fetched_at,headers_json,acknowledged)
 SELECT event_id,id,run_id,fetched_at,headers_json,projected FROM responses;
DROP VIEW IF EXISTS analysis_private_four_vs_four_tags;
DROP VIEW IF EXISTS analysis_private_one_vs_one_tags;
DROP VIEW IF EXISTS analysis_private_other_tags;
DROP VIEW IF EXISTS analysis_nawabari_four_vs_four_by_rule;
DROP VIEW IF EXISTS analysis_bankara_open_four_vs_four_by_rule;
DROP VIEW IF EXISTS analysis_bankara_challenge_four_vs_four_by_rule;
DROP VIEW IF EXISTS analysis_private_four_vs_four_by_rule;
DROP VIEW IF EXISTS analysis_event_four_vs_four_by_rule;
DROP VIEW IF EXISTS analysis_xmatch_four_vs_four_by_rule;
DROP VIEW IF EXISTS analysis_nawabari_four_vs_four;
DROP VIEW IF EXISTS analysis_nawabari_one_vs_one;
DROP VIEW IF EXISTS analysis_nawabari_other;
DROP VIEW IF EXISTS analysis_bankara_open_four_vs_four;
DROP VIEW IF EXISTS analysis_bankara_open_one_vs_one;
DROP VIEW IF EXISTS analysis_bankara_open_other;
DROP VIEW IF EXISTS analysis_bankara_challenge_four_vs_four;
DROP VIEW IF EXISTS analysis_bankara_challenge_one_vs_one;
DROP VIEW IF EXISTS analysis_bankara_challenge_other;
DROP VIEW IF EXISTS analysis_private_four_vs_four;
DROP VIEW IF EXISTS analysis_private_one_vs_one;
DROP VIEW IF EXISTS analysis_private_other;
DROP VIEW IF EXISTS analysis_event_four_vs_four;
DROP VIEW IF EXISTS analysis_event_one_vs_one;
DROP VIEW IF EXISTS analysis_event_other;
DROP VIEW IF EXISTS analysis_xmatch_four_vs_four;
DROP VIEW IF EXISTS analysis_xmatch_one_vs_one;
DROP VIEW IF EXISTS analysis_xmatch_other;
DROP VIEW IF EXISTS analysis_hold;
DROP VIEW IF EXISTS analysis_nawabari_by_rule;
DROP VIEW IF EXISTS analysis_nawabari_tags;
DROP VIEW IF EXISTS analysis_nawabari;
DROP VIEW IF EXISTS analysis_bankara_open_by_rule;
DROP VIEW IF EXISTS analysis_bankara_open_tags;
DROP VIEW IF EXISTS analysis_bankara_open;
DROP VIEW IF EXISTS analysis_bankara_challenge_by_rule;
DROP VIEW IF EXISTS analysis_bankara_challenge_tags;
DROP VIEW IF EXISTS analysis_bankara_challenge;
DROP VIEW IF EXISTS analysis_event_by_rule;
DROP VIEW IF EXISTS analysis_event_tags;
DROP VIEW IF EXISTS analysis_event;
DROP VIEW IF EXISTS analysis_xmatch_by_rule;
DROP VIEW IF EXISTS analysis_xmatch_tags;
DROP VIEW IF EXISTS analysis_xmatch;
DROP VIEW IF EXISTS analysis_fest_by_rule;
DROP VIEW IF EXISTS analysis_fest_tags;
DROP VIEW IF EXISTS analysis_fest;
DROP VIEW IF EXISTS analysis_private_four_vs_four_by_rule;
DROP VIEW IF EXISTS analysis_private_four_vs_four_tags;
DROP VIEW IF EXISTS analysis_private_four_vs_four;
DROP VIEW IF EXISTS analysis_private_three_vs_three_by_rule;
DROP VIEW IF EXISTS analysis_private_three_vs_three_tags;
DROP VIEW IF EXISTS analysis_private_three_vs_three;
DROP VIEW IF EXISTS analysis_private_two_vs_two_by_rule;
DROP VIEW IF EXISTS analysis_private_two_vs_two_tags;
DROP VIEW IF EXISTS analysis_private_two_vs_two;
DROP VIEW IF EXISTS analysis_private_one_vs_one_by_rule;
DROP VIEW IF EXISTS analysis_private_one_vs_one_tags;
DROP VIEW IF EXISTS analysis_private_one_vs_one;
DROP VIEW IF EXISTS analysis_private_other_by_rule;
DROP VIEW IF EXISTS analysis_private_other_tags;
DROP VIEW IF EXISTS analysis_private_other;
DROP VIEW IF EXISTS analysis_salmon_regular_by_rule;
DROP VIEW IF EXISTS analysis_salmon_regular_tags;
DROP VIEW IF EXISTS analysis_salmon_regular;
DROP VIEW IF EXISTS analysis_big_run_by_rule;
DROP VIEW IF EXISTS analysis_big_run_tags;
DROP VIEW IF EXISTS analysis_big_run;
DROP VIEW IF EXISTS analysis_team_contest_by_rule;
DROP VIEW IF EXISTS analysis_team_contest_tags;
DROP VIEW IF EXISTS analysis_team_contest;
DROP VIEW IF EXISTS analysis_hold_by_rule;
DROP VIEW IF EXISTS analysis_hold_tags;
DROP VIEW IF EXISTS analysis_hold;
CREATE VIEW analysis_nawabari AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='nawabari';
CREATE VIEW analysis_bankara_open AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='bankara_open';
CREATE VIEW analysis_bankara_challenge AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='bankara_challenge';
CREATE VIEW analysis_event AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='event';
CREATE VIEW analysis_xmatch AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='xmatch';
CREATE VIEW analysis_fest AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='fest';
CREATE VIEW analysis_private_four_vs_four AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='private_four_vs_four';
CREATE VIEW analysis_private_three_vs_three AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='private_three_vs_three';
CREATE VIEW analysis_private_two_vs_two AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='private_two_vs_two';
CREATE VIEW analysis_private_one_vs_one AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='private_one_vs_one';
CREATE VIEW analysis_private_other AS SELECT c.account,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.team_count,c.my_player_count,c.opponent_counts,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.vsStage.name') stage,json_extract(d.json_text,'$.judgement') judgement,json_extract(d.json_text,'$.knockout') knockout,json_extract(d.json_text,'$.duration') duration,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='private_other';
CREATE VIEW analysis_salmon_regular AS SELECT c.account,c.match_key,c.genre,c.rule_raw,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.coopStage.name') stage,json_extract(d.json_text,'$.dangerRate') danger_rate,json_extract(d.json_text,'$.resultWave') result_wave,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='salmon_regular';
CREATE VIEW analysis_big_run AS SELECT c.account,c.match_key,c.genre,c.rule_raw,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.coopStage.name') stage,json_extract(d.json_text,'$.dangerRate') danger_rate,json_extract(d.json_text,'$.resultWave') result_wave,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='big_run';
CREATE VIEW analysis_team_contest AS SELECT c.account,c.match_key,c.genre,c.rule_raw,json_extract(d.json_text,'$.playedTime') played_time,json_extract(d.json_text,'$.coopStage.name') stage,json_extract(d.json_text,'$.dangerRate') danger_rate,json_extract(d.json_text,'$.resultWave') result_wave,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='team_contest';
CREATE VIEW analysis_hold AS SELECT c.account,c.kind,c.match_key,c.genre,c.mode_raw,c.bankara_mode,c.rule_raw,c.rule_name,c.roster_class,c.analysis_set,c.team_count,c.my_player_count,c.opponent_counts,d.detail_response_id FROM match_classification c JOIN match_details d ON d.account=c.account AND d.kind=c.kind AND d.match_key=c.match_key WHERE c.analysis_set='hold';
CREATE VIEW analysis_nawabari_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_nawabari GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_bankara_open_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_bankara_open GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_bankara_challenge_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_bankara_challenge GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_event_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_event GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_xmatch_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_xmatch GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_fest_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_fest GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_private_four_vs_four_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_private_four_vs_four GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_private_three_vs_three_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_private_three_vs_three GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_private_two_vs_two_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_private_two_vs_two GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_private_one_vs_one_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_private_one_vs_one GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_private_other_by_rule AS SELECT account,rule_raw,rule_name,count(*) matches,sum(judgement='WIN') wins,sum(judgement='LOSE') losses,sum(judgement='DRAW') draws,sum(judgement IS NULL OR judgement NOT IN ('WIN','LOSE','DRAW')) other_judgements FROM analysis_private_other GROUP BY account,rule_raw,rule_name;
CREATE VIEW analysis_private_four_vs_four_tags AS SELECT p.*,t.tag,t.note FROM analysis_private_four_vs_four p LEFT JOIN match_tags t ON t.account=p.account AND t.match_key=p.match_key;
CREATE VIEW analysis_private_three_vs_three_tags AS SELECT p.*,t.tag,t.note FROM analysis_private_three_vs_three p LEFT JOIN match_tags t ON t.account=p.account AND t.match_key=p.match_key;
CREATE VIEW analysis_private_two_vs_two_tags AS SELECT p.*,t.tag,t.note FROM analysis_private_two_vs_two p LEFT JOIN match_tags t ON t.account=p.account AND t.match_key=p.match_key;
CREATE VIEW analysis_private_one_vs_one_tags AS SELECT p.*,t.tag,t.note FROM analysis_private_one_vs_one p LEFT JOIN match_tags t ON t.account=p.account AND t.match_key=p.match_key;
CREATE VIEW analysis_private_other_tags AS SELECT p.*,t.tag,t.note FROM analysis_private_other p LEFT JOIN match_tags t ON t.account=p.account AND t.match_key=p.match_key;
CREATE VIEW analysis_bankara_open_tags AS SELECT p.*,t.tag,t.note FROM analysis_bankara_open p LEFT JOIN match_tags t ON t.account=p.account AND t.match_key=p.match_key;
