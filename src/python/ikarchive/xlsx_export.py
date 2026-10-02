"""分析ビューを人が開く xlsx にする。正本ではない。編集はデータベースへ戻らない。"""
import json, os, tempfile, zipfile
from pathlib import Path
from xml.sax.saxutils import escape
from .display import GENRE_LABELS, format_rule

CELL_LIMIT = 32767
NS_MAIN = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
NS_REL = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
NS_PKG = 'http://schemas.openxmlformats.org/package/2006/relationships'
NS_CT = 'http://schemas.openxmlformats.org/package/2006/content-types'

VS_HEADERS = ('対戦日時','ルール','ルールコード','ステージ','勝敗','ノックアウト','試合秒','自分側人数','相手人数','ブキ','キル','アシスト','デス','スペシャル','塗り','タグ','試合ID')
SALMON_HEADERS = ('プレイ日時','ルール','ステージ','キケン度（%）','WAVE','試合ID')
HOLD_HEADERS = ('種別','区分','APIモード','ルール','人数区分','試合ID')

SHEETS = tuple((GENRE_LABELS.get(genre,'区分保留'), 'analysis_'+genre,kind) for genre,kind in (
    ('fest','vs'),('nawabari','vs'),('bankara_challenge','vs'),('bankara_open','vs'),
    ('xmatch','vs'),('event','vs'),('private_four_vs_four','vs'),('private_three_vs_three','vs'),
    ('private_two_vs_two','vs'),('private_one_vs_one','vs'),('private_other','vs'),
    ('salmon_regular','salmon'),('big_run','salmon'),('team_contest','salmon'),('hold','hold'),
))

def _col(n):
    s=''
    while n:
        n,r=divmod(n-1,26)
        s=chr(65+r)+s
    return s

def _cell(ref,value):
    if value is None or value=='':return ''
    if isinstance(value,(int,float)) and not isinstance(value,bool):
        return f'<c r="{ref}"><v>{value}</v></c>'
    text=escape(str(value))
    if len(text)>CELL_LIMIT:raise ValueError('CELL_TOO_LONG')
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>'

def _sheet(rows):
    body=['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',f'<worksheet xmlns="{NS_MAIN}"><sheetData>']
    for r,row in enumerate(rows,1):
        cells=''.join(_cell(f'{_col(c)}{r}',value) for c,value in enumerate(row,1))
        body.append(f'<row r="{r}">{cells}</row>')
    body.append('</sheetData></worksheet>')
    return ''.join(body)

def _vs_rows(db,view):
    tags_map = {
        (r[0], r[1]): r[2]
        for r in db.execute("SELECT account,match_key,group_concat(tag,'、') FROM match_tags GROUP BY account,match_key")
    }
    sql = f'''SELECT a.played_time,a.rule_name,a.rule_raw,a.stage,a.judgement,a.knockout,a.duration,a.my_player_count,a.opponent_counts,a.account,a.match_key,d.json_text
        FROM {view} a
        JOIN documents d ON d.response_id=a.detail_response_id AND d.account=a.account AND d.kind='vs' AND d.match_key=a.match_key
        ORDER BY a.played_time,a.match_key'''
    rows = [VS_HEADERS]
    for r in db.execute(sql):
        played_time, rule_name, rule_raw, stage, judgement, knockout, duration, my_player_count, opponent_counts, account, match_key, json_text = r
        weapon = kills = assists = deaths = specials = paint = None
        if json_text:
            try:
                data = json.loads(json_text)
            except Exception:
                data = None
            if isinstance(data, dict):
                teams = []
                my_team = data.get('myTeam')
                if isinstance(my_team, dict):
                    teams.append(my_team)
                other_teams = data.get('otherTeams')
                if isinstance(other_teams, list):
                    for ot in other_teams:
                        if isinstance(ot, dict):
                            teams.append(ot)
                target_player = None
                for team in teams:
                    players = team.get('players')
                    if isinstance(players, list):
                        for p in players:
                            if isinstance(p, dict):
                                is_myself = p.get('isMyself')
                                if is_myself is True or is_myself == 1:
                                    target_player = p
                                    break
                    if target_player is not None:
                        break
                if target_player is not None:
                    w = target_player.get('weapon')
                    if isinstance(w, dict):
                        weapon = w.get('name')
                    res = target_player.get('result')
                    if isinstance(res, dict):
                        kills = res.get('kill')
                        assists = res.get('assist')
                        deaths = res.get('death')
                        specials = res.get('special')
                    paint = target_player.get('paint')
        tag = tags_map.get((account, match_key))
        rows.append((
            played_time,
            rule_name,
            rule_raw,
            stage,
            judgement,
            knockout,
            duration,
            my_player_count,
            opponent_counts,
            weapon,
            kills,
            assists,
            deaths,
            specials,
            paint,
            tag,
            match_key,
        ))
    return rows

def _salmon_rows(db,view):
    sql=f'''SELECT played_time,rule_raw,stage,danger_rate,result_wave,match_key FROM {view} ORDER BY played_time,match_key'''
    return [SALMON_HEADERS]+[(r[0],format_rule(r[1]),r[2],r[3]*100 if r[3] is not None else None,r[4],r[5]) for r in db.execute(sql)]

def _hold_rows(db):
    sql='''SELECT kind,genre,mode_raw,rule_raw,roster_class,match_key FROM analysis_hold ORDER BY kind,match_key'''
    return [HOLD_HEADERS]+[tuple(row) for row in db.execute(sql)]

def _rows(db,kind,view):
    if kind=='vs':return _vs_rows(db,view)
    if kind=='salmon':return _salmon_rows(db,view)
    return _hold_rows(db)

def _zip_entry(name):
    # エントリの時刻を固定する。中身が同じなら、作る時刻が違ってもバイト列は同じになる。
    info=zipfile.ZipInfo(filename=name,date_time=(1980,1,1,0,0,0))
    info.compress_type=zipfile.ZIP_DEFLATED
    info.external_attr=0o600<<16
    return info

def export_xlsx(store,destination):
    destination=Path(destination)
    destination.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    tables={row[0] for row in store.db.execute("SELECT name FROM sqlite_master WHERE type='view'")}
    missing=[view for _,view,_ in SHEETS if view not in tables]
    if missing:raise ValueError('MISSING_ANALYSIS_VIEW')
    counts={}
    sheets=[('説明',_sheet([
        ('このファイルはデータベースから作り直します。',),
        ('セルを編集してもデータベースには戻りません。',),
        ('シートをまたいで集計しないでください。',),
        ('原文の応答は Excel のセルに入らないため、ここには入れていません。',),
        ('プライベートマッチの2対2を、自動でイカップルとは記録していません。',),
    ]))]
    counts['説明']=5
    for name,view,kind in SHEETS:
        rows=_rows(store.db,kind,view)
        sheets.append((name,_sheet(rows)))
        counts[name]=max(0,len(rows)-1)
    fd,name=tempfile.mkstemp(prefix=destination.name+'.',suffix='.tmp',dir=destination.parent)
    os.close(fd);tmp=Path(name)
    try:
        with zipfile.ZipFile(tmp,'w',compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr(_zip_entry('[Content_Types].xml'),f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="{NS_CT}"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>{''.join(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' for i in range(1,len(sheets)+1))}</Types>''')
            z.writestr(_zip_entry('_rels/.rels'),f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="{NS_PKG}"><Relationship Id="rId1" Type="{NS_REL}/officeDocument" Target="xl/workbook.xml"/></Relationships>''')
            workbook_sheets=''.join(f'<sheet name="{escape(name)}" sheetId="{i}" r:id="rId{i}"/>' for i, (name,_) in enumerate(sheets,1))
            z.writestr(_zip_entry('xl/workbook.xml'),f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="{NS_MAIN}" xmlns:r="{NS_REL}"><sheets>{workbook_sheets}</sheets></workbook>''')
            rels=''.join(f'<Relationship Id="rId{i}" Type="{NS_REL}/worksheet" Target="worksheets/sheet{i}.xml"/>' for i in range(1,len(sheets)+1))
            z.writestr(_zip_entry('xl/_rels/workbook.xml.rels'),f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="{NS_PKG}">{rels}</Relationships>''')
            for i,(_,xml) in enumerate(sheets,1):z.writestr(_zip_entry(f'xl/worksheets/sheet{i}.xml'),xml)
        tmp.replace(destination)
    finally:
        tmp.unlink(missing_ok=True)
    destination.chmod(0o600)
    return {'path':str(destination),'rows':counts,'canonical':'sqlite','edits_return_to_database':False}

PUBLISHED_XLSX_NAME = '分析.xlsx'
MAX_PUBLISHED_XLSX_BYTES = 5 * 1024 * 1024
LEGACY_ANALYSIS_XLSX_RETIRED = 'LEGACY_ANALYSIS_XLSX_RETIRED'

class PublishedXlsxRetired(Exception):
    category = LEGACY_ANALYSIS_XLSX_RETIRED

    def __init__(self):
        super().__init__(self.category)

class PublishedXlsxMissing(Exception):
    pass

class PublishedXlsxUnusable(Exception):
    pass

def published_xlsx_path(db_path):
    db_path = Path(db_path)
    parent = db_path.parent
    root = parent.parent if parent.name == 'database' else parent
    return root / 'exports' / PUBLISHED_XLSX_NAME

def read_published_xlsx(db_path):
    """旧分析表は不完全なため、ファイルやDBへ触れず利用を拒否する。"""
    raise PublishedXlsxRetired()
