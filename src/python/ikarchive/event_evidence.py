"""保存済みのイベント記録と、試合・シフトの詳細を混同しない監査集計。"""

import json


_OPERATIONS = (
    'DetailFestRecordDetailQuery',
    'DetailFestRefethQuery',
    'CoopRecordQuery',
    'CoopRecordRefetchQuery',
    'CoopRecordBigRunRecordContainerPaginationQuery',
)
_SETS = ('fest', 'big_run', 'team_contest')


def event_evidence(db):
    """公開済み応答にある証拠だけを数え、識別子や原文は返さない。

    フェス結果はアカウント＋イベントID、ビッグランはアカウント＋開催期間
    ＋ステージIDで重複を除く。イベント記録は試合・シフト詳細の代用ではない。
    """
    fest = {}
    big_run = {}
    team_contest_attend = None
    unusable = 0
    placeholders = ','.join('?' for _ in _OPERATIONS)
    rows = db.execute(
        f'''SELECT account, operation, json_text FROM responses
            WHERE operation IN ({placeholders}) AND http_status=200
              AND projected=1 AND json_text IS NOT NULL''',
        _OPERATIONS,
    )
    for account, operation, text in rows:
        try:
            data = json.loads(text).get('data')
        except (ValueError, AttributeError):
            unusable += 1
            continue
        if not isinstance(data, dict):
            unusable += 1
            continue
        if operation.startswith('DetailFest'):
            record = data.get('fest')
            if not isinstance(record, dict) or not isinstance(record.get('id'), str) or not record['id']:
                unusable += 1
                continue
            key = (account, record['id'])
            fest[key] = fest.get(key, False) or isinstance(record.get('playerResult'), dict)
            continue
        coop = data.get('coopRecord')
        if not isinstance(coop, dict):
            unusable += 1
            continue
        contest = coop.get('teamContestRecord')
        if isinstance(contest, dict):
            attend = contest.get('attend')
            if isinstance(attend, int) and not isinstance(attend, bool) and attend >= 0:
                team_contest_attend = max(team_contest_attend or 0, attend)
        big = coop.get('bigRunRecord')
        if not isinstance(big, dict):
            continue  # 部分再取得ではビッグランの欄が無くてもよい。
        records = big.get('records')
        if not isinstance(records, dict) or not isinstance(records.get('edges'), list):
            unusable += 1
            continue
        for edge in records['edges']:
            node = edge.get('node') if isinstance(edge, dict) else None
            if not isinstance(node, dict):
                unusable += 1
                continue
            stage = node.get('coopStage')
            stage_id = stage.get('id') if isinstance(stage, dict) else None
            start, end = node.get('startTime'), node.get('endTime')
            if not all(isinstance(x, str) and x for x in (stage_id, start, end)):
                unusable += 1
                continue
            score = node.get('highestJobScore')
            positive = isinstance(score, int) and not isinstance(score, bool) and score > 0
            key = (account, start, end, stage_id)
            big_run[key] = big_run.get(key, False) or positive
    classified = {name: 0 for name in _SETS}
    for name, count in db.execute(
        "SELECT analysis_set, count(*) FROM match_classification "
        "WHERE analysis_set IN ('fest','big_run','team_contest') GROUP BY analysis_set"
    ):
        classified[name] = count
    return {
        'fest_event_details': len(fest),
        'fest_events_with_player_result': sum(fest.values()),
        'big_run_event_records': len(big_run),
        'big_run_events_with_positive_highest_job_score': sum(big_run.values()),
        'team_contest_max_observed_attend': team_contest_attend,
        'classified_match_counts': classified,
        'unusable_event_responses_or_rows': unusable,
        'limit': (
            'Event summaries are not match or shift details. Missing classified matches '
            'cannot establish non-participation or completeness of historical records.'
        ),
    }
