/**
 * SQuidLite3 記録閲覧画面
 *
 * 制約:
 * - 閲覧は読み取り。タグの追加と削除だけ書き込む。
 * - 外部ネットワーク/依存ライブラリ/CDNなし、vanilla JS
 * - 絵文字なし、日本語
 * - token は変数メモリ内のみ保持（console/log/storage/cookie 保存禁止）
 * - fetch Authorization Bearer token, cache no-store, credentials omit
 * - innerHTML への API テキスト代入禁止（textContent のみ使用）
 * - 元本文（body_base64）を JSON parse して再 serialize しない
 * - 日時順とは表示しない（バックエンド返却順をそのまま表示）
 * - 統合GUI全完成と書かない
 */

const ikaringStatusText = {
  count: function (value) {
    if (typeof value === 'number' && Number.isInteger(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER) {
      return String(value);
    }
    return '未確認';
  },
  openJobs: function (jobs) {
    if (!Array.isArray(jobs)) {
      return '未確認';
    }
    let total = 0;
    for (let i = 0; i < jobs.length; i += 1) {
      const row = jobs[i];
      if (!row || typeof row !== 'object') {
        return '未確認';
      }
      if (row.state !== 'pending' && row.state !== 'retry' && row.state !== 'awaiting_scope') {
        continue;
      }
      if (typeof row.count !== 'number' || !Number.isInteger(row.count) || row.count < 0 || row.count > Number.MAX_SAFE_INTEGER) {
        return '未確認';
      }
      total += row.count;
      if (total > Number.MAX_SAFE_INTEGER) {
        return '未確認';
      }
    }
    return String(total);
  },
  coverage: function (flag) {
    if (flag === true) {
      return '照合済み';
    }
    if (flag === false) {
      return '未了';
    }
    return '未確認';
  },
  incident: function (code, formattedAt) {
    if (code == null || code === '') {
      return '記録なし';
    }
    if (typeof code !== 'string' || !/^[A-Z][A-Z0-9_]{0,63}$/.test(code)) {
      return '未確認';
    }
    if (typeof formattedAt === 'string' && /^\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}$/.test(formattedAt)) {
      return code + ' ' + formattedAt;
    }
    return code;
  },
  historyLines: function (histories, formatTime, nowMs) {
    const now = typeof nowMs === 'number' && Number.isFinite(nowMs) ? nowMs : Date.now();
    const order = [
      ['LatestBattleHistoriesQuery', '最新の履歴'],
      ['RegularBattleHistoriesQuery', 'レギュラーマッチ'],
      ['BankaraBattleHistoriesQuery', 'バンカラマッチ'],
      ['XBattleHistoriesQuery', 'Xマッチ'],
      ['EventBattleHistoriesQuery', 'イベントマッチ'],
      ['PrivateBattleHistoriesQuery', 'プライベートマッチ'],
      ['CoopHistoryQuery', 'バイト'],
    ];
    const byOp = {};
    if (Array.isArray(histories)) {
      for (let i = 0; i < histories.length; i += 1) {
        const row = histories[i];
        if (row && typeof row.operation === 'string') {
          byOp[row.operation] = row;
        }
      }
    }
    const lines = [];
    for (let i = 0; i < order.length; i += 1) {
      const op = order[i][0];
      const label = order[i][1];
      const row = byOp[op];
      const raw = row && typeof row.last_success_at === 'string' ? row.last_success_at : '';
      const parsed = raw ? Date.parse(raw) : NaN;
      const clockLate = raw !== '' && (!Number.isFinite(parsed) || now - parsed > 600000);
      const late = !row || row.stale !== false || raw === '' || clockLate;
      let when = '未確認';
      if (raw && Number.isFinite(parsed)) {
        when = typeof formatTime === 'function' ? formatTime(raw) : raw;
      }
      lines.push(label + ' ' + (late ? '遅延' : '確認済み') + ' ' + when);
    }
    return lines.join('\n');
  },
};
globalThis.ikaringStatusText = ikaringStatusText;

const ikaringRateText = {
  number: function (value) {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      return '未確認';
    }
    if (Math.abs(value - Math.round(value)) < 1e-9) {
      const rounded = Math.round(value);
      const sign = rounded < 0 ? '-' : '';
      const body = String(Math.abs(rounded)).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
      return sign + body;
    }
    const digits = Math.abs(value) >= 1 ? 2 : 4;
    const negative = value < 0;
    let text = Math.abs(value).toFixed(digits).replace(/0+$/, '').replace(/\.$/, '');
    const parts = text.split('.');
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    text = parts.join('.');
    return negative ? '-' + text : text;
  },
  value: function (value, unit) {
    if (unit !== 'number' && unit !== 'ratio') {
      return '未確認';
    }
    const shown = unit === 'ratio' ? value * 100 : value;
    const text = this.number(shown);
    if (text === '未確認') {
      return text;
    }
    return unit === 'ratio' ? text + '%' : text;
  },
  delta: function (value, unit) {
    if (value == null) {
      return '—';
    }
    const text = this.value(value, unit);
    if (text === '未確認') {
      return text;
    }
    if (typeof value === 'number' && Number.isFinite(value) && Math.abs(value) < 1e-9) {
      return '±' + this.value(0, unit);
    }
    if (value > 0 && text.charAt(0) !== '+') {
      return '+' + text;
    }
    return text;
  },
  priority: function (value) {
    if (value === 'primary') {
      return '主指標';
    }
    if (value === 'secondary') {
      return '副指標';
    }
    return '未確認';
  },
  source: function (value) {
    if (value === 'api') {
      return '応答の数値';
    }
    if (value === 'api_snapshot') {
      return '取得時点';
    }
    return '未確認';
  },
  chart: function (spec) {
    if (!spec || typeof spec.label !== 'string' || !Array.isArray(spec.points) || spec.points.length === 0) {
      throw new Error('Invalid rate summary');
    }
    if (spec.unit !== 'number' && spec.unit !== 'ratio') {
      throw new Error('Invalid rate summary');
    }
    const rate = this;
    const width = 720;
    const height = 280;
    const padLeft = 64;
    const padRight = 32;
    const padTop = 28;
    const padBottom = 44;
    const plotX = padLeft;
    const plotY = padTop;
    const plotW = width - padLeft - padRight;
    const plotH = height - padTop - padBottom;
    const xMin = plotX;
    const xMax = plotX + plotW;
    const yMin = plotY;
    const yMax = plotY + plotH;
    const gradeKeys = {
      afterGradePoint: true,
      gradePoint: true,
      regularGradePoint: true,
      highestGradePoint: true,
    };

    function pythonRound(value) {
      const negative = value < 0;
      const abs = Math.abs(value);
      const base = Math.floor(abs);
      const frac = abs - base;
      let rounded;
      if (Math.abs(frac - 0.5) <= 1e-10) {
        rounded = base % 2 === 0 ? base : base + 1;
      } else {
        rounded = Math.round(abs);
      }
      return negative ? -rounded : rounded;
    }

    function roundDigits(value, digits) {
      const factor = 10 ** digits;
      return pythonRound(value * factor) / factor;
    }

    function fixed1(value) {
      const rounded = roundDigits(value, 1);
      const negative = rounded < 0;
      return (negative ? '-' : '') + Math.abs(rounded).toFixed(1);
    }

    function metricKey(seriesId) {
      if (!seriesId) {
        return '';
      }
      const tail = String(seriesId).split('|').pop();
      return tail.split('.').pop();
    }

    function formatChartDate(val) {
      if (!val || typeof val !== 'string') {
        return val == null ? '' : String(val);
      }
      let clean = val.trim();
      if (clean.endsWith('Z')) {
        clean = clean.slice(0, -1) + '+00:00';
      }
      const matched = clean.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?:[+-]\d{2}:\d{2})?)?$/);
      if (!matched) {
        return String(val).slice(0, 16);
      }
      const month = Number(matched[2]);
      const day = Number(matched[3]);
      const hour = Number(matched[4] || '0');
      const minute = Number(matched[5] || '0');
      const second = Number(matched[6] || '0');
      if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59) {
        return String(val).slice(0, 16);
      }
      const hh = matched[4] || '00';
      const mm = matched[5] || '00';
      return matched[2] + '/' + matched[3] + ' ' + hh + ':' + mm;
    }

    function calcGradePointTicks(low, high) {
      const base = [0, 200, 400, 600, 800, 999];
      const lowTicks = [];
      if (low < 0) {
        let cur = -200;
        while (cur >= low - 200) {
          lowTicks.push(cur);
          cur -= 200;
        }
        lowTicks.reverse();
      }
      const highTicks = [];
      if (high > 999) {
        let cur = 1200;
        while (cur < high + 200) {
          highTicks.push(cur);
          cur += 200;
        }
      }
      return lowTicks.concat(base, highTicks);
    }

    function calcYTicks(low, high) {
      let lo = low;
      let hi = high;
      if (lo === hi) {
        if (lo === 0) {
          lo = -2;
          hi = 2;
        } else {
          const delta = Math.max(1, Math.abs(lo) * 0.1);
          hi = lo + delta;
          lo = lo - delta;
        }
      }
      const rawStep = (hi - lo) / 5;
      const power = 10 ** Math.floor(Math.log10(rawStep));
      const fraction = rawStep / power;
      let step;
      if (fraction <= 1) {
        step = power;
      } else if (fraction <= 2) {
        step = 2 * power;
      } else if (fraction <= 2.5) {
        step = 2.5 * power;
      } else if (fraction <= 5) {
        step = 5 * power;
      } else {
        step = 10 * power;
      }
      let niceMin = Math.floor(lo / step) * step;
      let niceMax = Math.ceil(hi / step) * step;
      let nSteps = pythonRound((niceMax - niceMin) / step);
      if (nSteps < 4) {
        const ratio = step / power;
        if (Math.abs(ratio - 5) < 1e-6) {
          step = 2 * power;
        } else if (Math.abs(ratio - 2) < 1e-6) {
          step = power;
        } else if (Math.abs(ratio - 10) < 1e-6) {
          step = 5 * power;
        }
        niceMin = Math.floor(lo / step) * step;
        niceMax = Math.ceil(hi / step) * step;
        nSteps = pythonRound((niceMax - niceMin) / step);
      }
      const ticks = [];
      for (let i = 0; i <= nSteps; i += 1) {
        ticks.push(roundDigits(niceMin + i * step, 6));
      }
      return ticks;
    }

    const plotted = [];
    for (let i = 0; i < spec.points.length; i += 1) {
      const point = spec.points[i];
      if (!point || typeof point.value !== 'number' || !Number.isFinite(point.value)) {
        throw new Error('Invalid rate summary');
      }
      if (point.played_time != null && typeof point.played_time !== 'string') {
        throw new Error('Invalid rate summary');
      }
      plotted.push({
        played_time: point.played_time == null ? null : point.played_time,
        value: spec.unit === 'ratio' ? point.value * 100 : point.value,
        stored: point.value,
      });
    }
    const n = plotted.length;
    const values = plotted.map(function (point) { return point.value; });
    const low = Math.min.apply(null, values);
    const high = Math.max.apply(null, values);
    const grade = gradeKeys[metricKey(spec.seriesId)] === true;
    const ticks = grade ? calcGradePointTicks(low, high) : calcYTicks(low, high);
    const yTickMin = ticks[0];
    const yTickMax = ticks[ticks.length - 1];
    const ySpan = (yTickMax - yTickMin) || 1;

    function yAt(val) {
      return yMax - (val - yTickMin) / ySpan * plotH;
    }

    function xAt(idx) {
      if (n === 1) {
        return plotX + plotW / 2;
      }
      return plotX + plotW * idx / (n - 1);
    }

    const yTicks = ticks.map(function (tick) {
      const y = yAt(tick);
      const zero = Math.abs(tick) < 1e-6;
      const label = spec.unit === 'ratio' ? rate.number(tick) + '%' : rate.number(tick);
      return {
        value: tick,
        label: label,
        zero: zero,
        x1: fixed1(xMin),
        y1: fixed1(y),
        x2: fixed1(xMax),
        y2: fixed1(y),
        labelX: fixed1(xMin - 8),
        labelY: fixed1(y + 4),
      };
    });

    const indices = [];
    if (n <= 6) {
      for (let i = 0; i < n; i += 1) {
        indices.push(i);
      }
    } else {
      const seen = {};
      for (let i = 0; i < 5; i += 1) {
        const idx = pythonRound(i * (n - 1) / 4);
        if (!Object.prototype.hasOwnProperty.call(seen, idx)) {
          seen[idx] = true;
          indices.push(idx);
        }
      }
      indices.sort(function (a, b) { return a - b; });
    }

    const xTicks = indices.map(function (idx) {
      const point = plotted[idx];
      const x = xAt(idx);
      const time = point.played_time || '';
      return {
        index: idx,
        time: time,
        label: formatChartDate(point.played_time),
        x1: fixed1(x),
        y1: fixed1(yMax),
        x2: fixed1(x),
        y2: fixed1(yMax + 5),
        labelX: fixed1(x),
        labelY: fixed1(yMax + 20),
      };
    });

    const coords = plotted.map(function (point, idx) {
      return { x: xAt(idx), y: yAt(point.value) };
    });
    const lineText = coords.map(function (coord) {
      return fixed1(coord.x) + ',' + fixed1(coord.y);
    }).join(' ');
    const line = n >= 2 ? lineText : null;
    const area = n >= 2
      ? lineText + ' ' + fixed1(coords[n - 1].x) + ',' + fixed1(yMax) + ' ' + fixed1(coords[0].x) + ',' + fixed1(yMax)
      : null;
    const dots = plotted.map(function (point, idx) {
      const when = point.played_time || '';
      const shown = rate.value(point.stored, spec.unit);
      let title = when + ' ' + shown;
      const focus = idx === n - 1 && n > 1;
      if (focus) {
        title += ' (最新)';
      }
      return {
        cx: fixed1(coords[idx].x),
        cy: fixed1(coords[idx].y),
        focus: focus,
        title: title,
      };
    });
    const storedValues = plotted.map(function (point) { return point.stored; });
    const genreLabel = spec.genreLabel || '';
    const ruleLabel = spec.ruleLabel || '';
    const ruleDesc = ruleLabel ? ' ' + ruleLabel : '';
    const desc = genreLabel + ruleDesc + ' ' + spec.label + '。データ数' + n + '点、最新値'
      + rate.value(storedValues[n - 1], spec.unit) + '、最低値'
      + rate.value(Math.min.apply(null, storedValues), spec.unit) + '、最高値'
      + rate.value(Math.max.apply(null, storedValues), spec.unit) + '。';
    return {
      width: width,
      height: height,
      paper: '#fbf5ec',
      ink: '#241a33',
      grid: '#7a6a8a',
      link: '#5b3bb0',
      title: spec.label + 'の推移グラフ',
      desc: desc,
      yTicks: yTicks,
      xTicks: xTicks,
      xIndices: indices,
      plotted: values,
      line: line,
      area: area,
      dots: dots,
      focus: n > 1,
    };
  },
};
globalThis.ikaringRateText = ikaringRateText;

(function () {
  'use strict';

  // トークンは変数メモリのみで保持
  let authToken = '';

  // 画面状態
  let currentAccount = '';
  let currentKind = '';
  let currentAnalysisSet = '';
  let currentRule = '';
  let currentWeapon = '';
  let currentTagFilter = '';
  let currentQuery = '';
  let currentOffset = 0;
  const PAGE_LIMIT = 50;
  let currentTotal = 0;

  // 詳細ダウンロード用データ
  let currentSourceBase64 = null;
  let lastDetailParams = null;

  // 非同期競合防止（AbortController と request sequence）
  let statusSequence = 0;
  let statusAbortController = null;

  let listSequence = 0;
  let listAbortController = null;

  let detailSequence = 0;
  let detailAbortController = null;

  // ジャンル・ルールの正式名称辞書（src/python/ikarchive/display.py より正確に移植）
  const GENRE_LABELS = {
    fest: 'フェスマッチ',
    nawabari: 'レギュラーマッチ',
    bankara_challenge: 'バンカラマッチ（チャレンジ）',
    bankara_open: 'バンカラマッチ（オープン）',
    xmatch: 'Xマッチ',
    event: 'イベントマッチ',
    private: 'プライベートマッチ',
    private_four_vs_four: 'プライベートマッチ（4対4）',
    private_three_vs_three: 'プライベートマッチ（3対3）',
    private_two_vs_two: 'プライベートマッチ（2対2）',
    private_one_vs_one: 'プライベートマッチ（1対1）',
    private_other: 'プライベートマッチ（その他）',
    salmon_regular: 'いつものバイト',
    big_run: 'ビッグラン',
    team_contest: 'バイトチームコンテスト',
    hold: '区分保留',
    unclassified: '分類なし',
  };

  const RULE_LABELS = {
    TURF_WAR: 'ナワバリバトル',
    AREA: 'ガチエリア',
    LOFT: 'ガチヤグラ',
    GOAL: 'ガチホコバトル',
    CLAM: 'ガチアサリ',
    REGULAR: 'いつものバイト',
    BIG_RUN: 'ビッグラン',
    TEAM_CONTEST: 'バイトチームコンテスト',
  };

  // タグ候補定数（候補であり入力欄の自由なタグを拒否しない）
  const OPEN_TAG_SUGGESTIONS = ['下げラン', '練習', 'エンジョイ', 'ガチ'];
  const PRIVATE_TAG_SUGGESTIONS = ['エンジョイ', '対抗戦', 'イカップル'];

  // DOM要素参照
  let statusLiveEl = null;
  let noTokenContainerEl = null;
  let appContainerEl = null;

  let statusLoadingEl = null;
  let statusErrorEl = null;
  let statusErrorMessageEl = null;
  let statusRetryBtn = null;
  let statusContentEl = null;
  let statusLastCheckedEl = null;
  let statusSyncStateEl = null;
  let statusAuthStateEl = null;
  let statusMatchCountEl = null;
  let statusMissingDetailEl = null;
  let statusPendingDetailsEl = null;
  let statusUnavailableDetailsEl = null;
  let statusOpenJobsEl = null;
  let statusIssueCountEl = null;
  let statusCoverageEl = null;
  let statusAuthFailureEl = null;
  let statusSyncErrorEl = null;
  let statusHistoryPathsEl = null;
  let lastStatusHistories = null;
  let lastStatusIsSlice = false;
  let currentDataset = 'unified';
  let rebuildingDatasets = false;
  let facetCandidatesFailed = false;
  let viewEpoch = 0;
  let datasetSelectEl = null;
  let statusHistoryTimer = null;
  let tagSummaryLoadingEl = null;
  let tagSummaryErrorEl = null;
  let tagSummaryErrorMessageEl = null;
  let tagSummaryBodyEl = null;
  let ruleResultsLoadingEl = null;
  let ruleResultsErrorEl = null;
  let ruleResultsErrorMessageEl = null;
  let ruleResultsBodyEl = null;
  let rateSummaryLoadingEl = null;
  let rateSummaryErrorEl = null;
  let rateSummaryErrorMessageEl = null;
  let rateSummaryBodyEl = null;
  let rateChartsEl = null;
  let publishedXlsxButtonEl = null;
  let publishedXlsxMessageEl = null;

  let filterFormEl = null;
  let accountSelectEl = null;
  let kindSelectEl = null;
  let analysisSetSelectEl = null;
  let ruleSelectEl = null;
  let playedFromEl = null;
  let playedToEl = null;
  let weaponSelectEl = null;
  let tagFilterSelectEl = null;
  let recordQueryEl = null;
  let refreshBtn = null;

  let recordsCountEl = null;
  let prevPageBtn = null;
  let nextPageBtn = null;
  let pageDisplayEl = null;
  let recordsLoadingEl = null;
  let recordsErrorEl = null;
  let recordsErrorMessageEl = null;
  let recordsRetryBtn = null;
  let recordsTableBodyEl = null;

  let detailSectionEl = null;
  let detailHeadingEl = null;
  let detailLoadingEl = null;
  let detailErrorEl = null;
  let detailErrorMessageEl = null;
  let detailRetryBtn = null;
  let detailBodyEl = null;
  let detailPlayedTimeEl = null;
  let detailKindEl = null;
  let detailGenreEl = null;
  let detailRuleEl = null;
  let detailStateEl = null;
  let sourceDownloadAreaEl = null;
  let downloadSourceBtn = null;
  let foldedDetailJsonEl = null;
  let detailJsonPreEl = null;
  let foldedSourceInfoEl = null;
  let sourceInfoContentEl = null;

  let tagSectionEl = null;
  let tagHeadingEl = null;
  let tagScopeNoteEl = null;
  let tagListEl = null;
  let tagEmptyEl = null;
  let tagSuggestionsEl = null;
  let tagFormEl = null;
  let tagInputEl = null;
  let tagAddButtonEl = null;
  let tagErrorEl = null;

  /**
   * 支援技術向けステータス通知 (aria-live)
   */
  function announceStatus(message) {
    if (statusLiveEl) {
      statusLiveEl.textContent = message;
    }
  }

  /**
   * 日時フォーマット (played_time用、first_seen代用禁止)
   */
  function formatPlayedTime(playedTime) {
    if (playedTime === null || playedTime === undefined || playedTime === '') {
      return '不明';
    }
    return formatIsoDate(playedTime);
  }

  /**
   * ISO 8601 日時文字列を日本語ローカル形式に安全変換
   */
  function formatIsoDate(isoStr) {
    if (!isoStr || typeof isoStr !== 'string') {
      return '不明';
    }
    try {
      const d = new Date(isoStr);
      if (isNaN(d.getTime())) {
        return isoStr;
      }
      const pad = function (n) {
        return String(n).padStart(2, '0');
      };
      const y = d.getFullYear();
      const m = pad(d.getMonth() + 1);
      const day = pad(d.getDate());
      const h = pad(d.getHours());
      const min = pad(d.getMinutes());
      const s = pad(d.getSeconds());
      return y + '/' + m + '/' + day + ' ' + h + ':' + min + ':' + s;
    } catch (e) {
      return isoStr;
    }
  }

  function playedBound(value) {
    if (!value) {
      return '';
    }
    const parsed = new Date(value);
    if (isNaN(parsed.getTime())) {
      return null;
    }
    return parsed.toISOString().replace(/\.\d{3}Z$/, 'Z');
  }

  function markFacetCandidatesFailed(clearSelects) {
    facetCandidatesFailed = true;
    if (clearSelects) {
      currentAnalysisSet = fillSelect(analysisSetSelectEl, [], formatGenre, '');
      currentRule = fillSelect(ruleSelectEl, [], formatRule, '');
      currentWeapon = fillSelect(weaponSelectEl, [], function (value) { return value; }, '');
      currentTagFilter = fillSelect(tagFilterSelectEl, [], function (value) { return value; }, '');
    }
    announceStatus('絞り込み候補の取得でエラーが発生しました。');
  }

  function fillSelect(select, values, labelFor, previous) {
    select.textContent = '';
    const all = document.createElement('option');
    all.value = '';
    all.textContent = 'すべて';
    select.appendChild(all);
    values.forEach(function (value) {
      const option = document.createElement('option');
      option.value = value;
      option.textContent = labelFor(value);
      select.appendChild(option);
    });
    select.value = values.indexOf(previous) >= 0 ? previous : '';
    return select.value;
  }

  function applyRecordFilters() {
    currentKind = kindSelectEl.value;
    currentAnalysisSet = analysisSetSelectEl.value;
    currentRule = ruleSelectEl.value;
    currentWeapon = weaponSelectEl.value;
    currentTagFilter = tagFilterSelectEl.value;
    currentQuery = recordQueryEl.value.trim();
    if (currentQuery.length > 80 || hasControlCharacter(currentQuery)) {
      recordsErrorEl.classList.remove('hidden');
      recordsErrorMessageEl.textContent = '検索は80文字までで、制御文字は使えません。';
      announceStatus('検索の条件を確認してください。');
      return;
    }
    recordsErrorEl.classList.add('hidden');
    currentOffset = 0;
    resetDetailPane();
    fetchRecords(0);
  }

  async function fetchFacets() {
    if (!currentAccount || !analysisSetSelectEl) {
      return;
    }
    const epoch = viewEpoch;
    const params = new URLSearchParams({ account: currentAccount });
    applyDataset(params);
    const res = await apiFetch('/api/record-facets?' + params.toString());
    if (epoch !== viewEpoch) {
      return;
    }
    if (!res.ok) {
      throw { status: res.status };
    }
    const data = await res.json();
    if (epoch !== viewEpoch) {
      return;
    }
    facetCandidatesFailed = false;
    currentAnalysisSet = fillSelect(
      analysisSetSelectEl,
      Array.isArray(data.analysis_sets) ? data.analysis_sets : [],
      formatGenre,
      currentAnalysisSet
    );
    currentRule = fillSelect(
      ruleSelectEl,
      Array.isArray(data.rules) ? data.rules : [],
      formatRule,
      currentRule
    );
    currentWeapon = fillSelect(
      weaponSelectEl,
      Array.isArray(data.weapons) ? data.weapons : [],
      function (value) { return value; },
      currentWeapon
    );
    currentTagFilter = fillSelect(
      tagFilterSelectEl,
      Array.isArray(data.tags) ? data.tags : [],
      function (value) { return value; },
      currentTagFilter
    );
  }

  function clearTagSummary(message) {
    if (!tagSummaryBodyEl) {
      return;
    }
    tagSummaryBodyEl.textContent = '';
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 4;
    td.textContent = message;
    tr.appendChild(td);
    tagSummaryBodyEl.appendChild(tr);
  }

  function renderTagSummary(data) {
    if (!data || !Array.isArray(data.sets)) {
      throw new Error('Invalid tag summary');
    }
    for (let i = 0; i < data.sets.length; i += 1) {
      const set = data.sets[i];
      if (!set || typeof set.analysis_set !== 'string' || !Array.isArray(set.tags)) {
        throw new Error('Invalid tag summary');
      }
      for (let j = 0; j < set.tags.length; j += 1) {
        const item = set.tags[j];
        if (!item || typeof item.tag !== 'string') {
          throw new Error('Invalid tag summary');
        }
      }
    }
    if (data.sets.length === 0) {
      clearTagSummary('件数はありません');
      return;
    }
    tagSummaryBodyEl.textContent = '';
    for (let i = 0; i < data.sets.length; i += 1) {
      const set = data.sets[i];
      const tr = document.createElement('tr');
      const genre = document.createElement('td');
      genre.textContent = formatGenre(set.analysis_set);
      const matches = document.createElement('td');
      matches.textContent = ikaringStatusText.count(set.matches);
      const untagged = document.createElement('td');
      untagged.textContent = ikaringStatusText.count(set.untagged);
      const tags = document.createElement('td');
      if (set.tags.length === 0) {
        tags.textContent = 'なし';
      } else {
        for (let j = 0; j < set.tags.length; j += 1) {
          const item = set.tags[j];
          const line = document.createElement('div');
          line.textContent = item.tag + ' ' + ikaringStatusText.count(item.matches);
          tags.appendChild(line);
        }
      }
      tr.appendChild(genre);
      tr.appendChild(matches);
      tr.appendChild(untagged);
      tr.appendChild(tags);
      tagSummaryBodyEl.appendChild(tr);
    }
  }

  async function fetchTagSummary() {
    if (!tagSummaryBodyEl || !tagSummaryLoadingEl || !tagSummaryErrorEl) {
      return;
    }
    if (!currentAccount) {
      tagSummaryLoadingEl.classList.add('hidden');
      tagSummaryErrorEl.classList.add('hidden');
      clearTagSummary('アカウントがありません');
      return;
    }
    const epoch = viewEpoch;
    tagSummaryLoadingEl.classList.remove('hidden');
    tagSummaryErrorEl.classList.add('hidden');
    try {
      const params = new URLSearchParams({ account: currentAccount });
      applyDataset(params);
      const res = await apiFetch('/api/tag-summary?' + params.toString());
      if (epoch !== viewEpoch) {
        return;
      }
      if (!res.ok) {
        throw { status: res.status };
      }
      const data = await res.json();
      if (epoch !== viewEpoch) {
        return;
      }
      renderTagSummary(data);
      tagSummaryLoadingEl.classList.add('hidden');
    } catch (err) {
      if (epoch !== viewEpoch) {
        return;
      }
      tagSummaryLoadingEl.classList.add('hidden');
      tagSummaryBodyEl.textContent = '';
      tagSummaryErrorEl.classList.remove('hidden');
      tagSummaryErrorMessageEl.textContent = getErrorMessage(err && err.status, 'タグの件数の取得に失敗しました。');
    }
  }

  function clearCountedRow(body, columns, message) {
    if (!body) {
      return;
    }
    body.textContent = '';
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = columns;
    td.textContent = message;
    tr.appendChild(td);
    body.appendChild(tr);
  }

  function appendCountCell(tr, value) {
    const td = document.createElement('td');
    td.textContent = ikaringStatusText.count(value);
    tr.appendChild(td);
  }

  function clearRuleResults(message) {
    clearCountedRow(ruleResultsBodyEl, 7, message);
  }

  function renderRuleResults(data) {
    if (!data || !Array.isArray(data.sets) || Object.keys(data).some(function (key) { return key !== 'sets'; })) {
      throw new Error('Invalid rule results');
    }
    if (data.sets.length === 0) {
      clearRuleResults('件数はありません');
      return;
    }
    ruleResultsBodyEl.textContent = '';
    for (let i = 0; i < data.sets.length; i += 1) {
      const set = data.sets[i];
      if (!set || typeof set.analysis_set !== 'string' || !Array.isArray(set.rules)) {
        throw new Error('Invalid rule results');
      }
      const rules = set.rules.length === 0 ? [null] : set.rules;
      for (let j = 0; j < rules.length; j += 1) {
        const rule = rules[j];
        if (rule !== null) {
          if (!rule || (rule.rule_raw != null && typeof rule.rule_raw !== 'string')) {
            throw new Error('Invalid rule results');
          }
        }
        const tr = document.createElement('tr');
        const genre = document.createElement('td');
        genre.textContent = formatGenre(set.analysis_set);
        const ruleCell = document.createElement('td');
        ruleCell.textContent = rule === null ? 'なし' : formatRule(rule.rule_raw);
        tr.appendChild(genre);
        tr.appendChild(ruleCell);
        appendCountCell(tr, rule === null ? 0 : rule.matches);
        appendCountCell(tr, rule === null ? 0 : rule.wins);
        appendCountCell(tr, rule === null ? 0 : rule.losses);
        appendCountCell(tr, rule === null ? 0 : rule.draws);
        appendCountCell(tr, rule === null ? 0 : rule.other);
        ruleResultsBodyEl.appendChild(tr);
      }
    }
  }

  async function fetchRuleResults() {
    if (!ruleResultsBodyEl || !ruleResultsLoadingEl || !ruleResultsErrorEl) {
      return;
    }
    if (!currentAccount) {
      ruleResultsLoadingEl.classList.add('hidden');
      ruleResultsErrorEl.classList.add('hidden');
      clearRuleResults('アカウントがありません');
      return;
    }
    const epoch = viewEpoch;
    ruleResultsLoadingEl.classList.remove('hidden');
    ruleResultsErrorEl.classList.add('hidden');
    try {
      const params = new URLSearchParams({ account: currentAccount });
      applyDataset(params);
      const res = await apiFetch('/api/rule-results?' + params.toString());
      if (epoch !== viewEpoch) {
        return;
      }
      if (!res.ok) {
        throw { status: res.status };
      }
      const data = await res.json();
      if (epoch !== viewEpoch) {
        return;
      }
      renderRuleResults(data);
      ruleResultsLoadingEl.classList.add('hidden');
    } catch (err) {
      if (epoch !== viewEpoch) {
        return;
      }
      ruleResultsLoadingEl.classList.add('hidden');
      ruleResultsBodyEl.textContent = '';
      ruleResultsErrorEl.classList.remove('hidden');
      ruleResultsErrorMessageEl.textContent = getErrorMessage(err && err.status, '勝敗の取得に失敗しました。');
    }
  }

  function clearRateSummary(message) {
    clearCountedRow(rateSummaryBodyEl, 10, message);
    if (rateChartsEl) {
      rateChartsEl.textContent = '';
    }
  }

  function assertRateSeries(item) {
    if (!item || typeof item !== 'object') {
      throw new Error('Invalid rate summary');
    }
    const allowed = {
      series_id: true,
      label: true,
      genre: true,
      rule_raw: true,
      source: true,
      priority: true,
      unit: true,
      count: true,
      latest: true,
      previous: true,
      delta: true,
      minimum: true,
      maximum: true,
      points: true,
    };
    const keys = Object.keys(item);
    for (let i = 0; i < keys.length; i += 1) {
      if (!allowed[keys[i]]) {
        throw new Error('Invalid rate summary');
      }
    }
    if (typeof item.series_id !== 'string' || typeof item.label !== 'string' || typeof item.genre !== 'string') {
      throw new Error('Invalid rate summary');
    }
    if (item.rule_raw != null && typeof item.rule_raw !== 'string') {
      throw new Error('Invalid rate summary');
    }
    if (item.unit !== 'number' && item.unit !== 'ratio') {
      throw new Error('Invalid rate summary');
    }
    if (!Array.isArray(item.points) || item.points.length === 0 || item.count !== item.points.length) {
      throw new Error('Invalid rate summary');
    }
    for (let i = 0; i < item.points.length; i += 1) {
      const point = item.points[i];
      if (!point || typeof point !== 'object') {
        throw new Error('Invalid rate summary');
      }
      const pointKeys = Object.keys(point);
      if (pointKeys.length !== 2 || !Object.prototype.hasOwnProperty.call(point, 'played_time') || !Object.prototype.hasOwnProperty.call(point, 'value')) {
        throw new Error('Invalid rate summary');
      }
      if (point.played_time != null && typeof point.played_time !== 'string') {
        throw new Error('Invalid rate summary');
      }
      if (typeof point.value !== 'number' || !Number.isFinite(point.value)) {
        throw new Error('Invalid rate summary');
      }
    }
  }

  function buildRateSvg(geometry, index) {
    const svgNs = 'http://www.w3.org/2000/svg';
    const wrap = document.createElement('div');
    wrap.className = 'chart-wrap';
    const svg = document.createElementNS(svgNs, 'svg');
    const titleId = 'rate-chart-title-' + index;
    const descId = 'rate-chart-desc-' + index;
    svg.setAttribute('viewBox', '0 0 ' + geometry.width + ' ' + geometry.height);
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-labelledby', titleId + ' ' + descId);
    const title = document.createElementNS(svgNs, 'title');
    title.setAttribute('id', titleId);
    title.textContent = geometry.title;
    const desc = document.createElementNS(svgNs, 'desc');
    desc.setAttribute('id', descId);
    desc.textContent = geometry.desc;
    svg.appendChild(title);
    svg.appendChild(desc);
    const rect = document.createElementNS(svgNs, 'rect');
    rect.setAttribute('width', String(geometry.width));
    rect.setAttribute('height', String(geometry.height));
    rect.setAttribute('fill', geometry.paper);
    svg.appendChild(rect);
    for (let i = 0; i < geometry.yTicks.length; i += 1) {
      const tick = geometry.yTicks[i];
      const line = document.createElementNS(svgNs, 'line');
      line.setAttribute('class', tick.zero ? 'grid-line zero-line' : 'grid-line');
      line.setAttribute('x1', tick.x1);
      line.setAttribute('y1', tick.y1);
      line.setAttribute('x2', tick.x2);
      line.setAttribute('y2', tick.y2);
      line.setAttribute('stroke', tick.zero ? geometry.ink : geometry.grid);
      line.setAttribute('stroke-width', tick.zero ? '1.5' : '1');
      if (!tick.zero) {
        line.setAttribute('stroke-dasharray', '3,3');
      }
      svg.appendChild(line);
      const text = document.createElementNS(svgNs, 'text');
      text.setAttribute('class', 'tick-label y-tick-label');
      text.setAttribute('x', tick.labelX);
      text.setAttribute('y', tick.labelY);
      text.setAttribute('text-anchor', 'end');
      text.setAttribute('fill', geometry.ink);
      text.setAttribute('font-size', '12');
      text.textContent = tick.label;
      svg.appendChild(text);
    }
    const yAxis = document.createElementNS(svgNs, 'line');
    yAxis.setAttribute('class', 'axis y-axis');
    yAxis.setAttribute('x1', geometry.yTicks[0].x1);
    yAxis.setAttribute('y1', '28.0');
    yAxis.setAttribute('x2', geometry.yTicks[0].x1);
    yAxis.setAttribute('y2', geometry.xTicks.length ? geometry.xTicks[0].y1 : '236.0');
    yAxis.setAttribute('stroke', geometry.ink);
    yAxis.setAttribute('stroke-width', '1.5');
    const xAxis = document.createElementNS(svgNs, 'line');
    xAxis.setAttribute('class', 'axis x-axis');
    xAxis.setAttribute('x1', geometry.yTicks[0].x1);
    xAxis.setAttribute('y1', yAxis.getAttribute('y2'));
    xAxis.setAttribute('x2', geometry.yTicks[0].x2);
    xAxis.setAttribute('y2', yAxis.getAttribute('y2'));
    xAxis.setAttribute('stroke', geometry.ink);
    xAxis.setAttribute('stroke-width', '1.5');
    svg.appendChild(yAxis);
    svg.appendChild(xAxis);
    for (let i = 0; i < geometry.xTicks.length; i += 1) {
      const tick = geometry.xTicks[i];
      const mark = document.createElementNS(svgNs, 'line');
      mark.setAttribute('class', 'x-tick-mark');
      mark.setAttribute('x1', tick.x1);
      mark.setAttribute('y1', tick.y1);
      mark.setAttribute('x2', tick.x2);
      mark.setAttribute('y2', tick.y2);
      mark.setAttribute('stroke', geometry.ink);
      mark.setAttribute('stroke-width', '1.5');
      svg.appendChild(mark);
      const text = document.createElementNS(svgNs, 'text');
      text.setAttribute('class', 'tick-label x-tick-label');
      text.setAttribute('data-time', tick.time);
      text.setAttribute('x', tick.labelX);
      text.setAttribute('y', tick.labelY);
      text.setAttribute('text-anchor', 'middle');
      text.setAttribute('fill', geometry.ink);
      text.setAttribute('font-size', '11');
      text.textContent = tick.label;
      svg.appendChild(text);
    }
    if (geometry.area) {
      const area = document.createElementNS(svgNs, 'polygon');
      area.setAttribute('class', 'chart-area');
      area.setAttribute('points', geometry.area);
      area.setAttribute('fill', geometry.link);
      area.setAttribute('fill-opacity', '0.12');
      svg.appendChild(area);
    }
    if (geometry.line) {
      const line = document.createElementNS(svgNs, 'polyline');
      line.setAttribute('class', 'chart-line');
      line.setAttribute('fill', 'none');
      line.setAttribute('stroke', geometry.link);
      line.setAttribute('stroke-width', '2.5');
      line.setAttribute('points', geometry.line);
      svg.appendChild(line);
    }
    for (let i = 0; i < geometry.dots.length; i += 1) {
      const dot = geometry.dots[i];
      if (dot.focus) {
        const ring = document.createElementNS(svgNs, 'circle');
        ring.setAttribute('class', 'dot-focus');
        ring.setAttribute('cx', dot.cx);
        ring.setAttribute('cy', dot.cy);
        ring.setAttribute('r', '6.5');
        ring.setAttribute('fill', 'none');
        ring.setAttribute('stroke', geometry.link);
        ring.setAttribute('stroke-width', '2');
        svg.appendChild(ring);
      }
      const circle = document.createElementNS(svgNs, 'circle');
      circle.setAttribute('class', dot.focus ? 'dot dot-latest' : 'dot');
      circle.setAttribute('cx', dot.cx);
      circle.setAttribute('cy', dot.cy);
      circle.setAttribute('r', '3.5');
      circle.setAttribute('fill', geometry.ink);
      const tip = document.createElementNS(svgNs, 'title');
      tip.textContent = dot.title;
      circle.appendChild(tip);
      svg.appendChild(circle);
    }
    wrap.appendChild(svg);
    return wrap;
  }

  function buildRatePointTable(item, ruleLabel) {
    const wrap = document.createElement('div');
    wrap.className = 'table-container';
    const table = document.createElement('table');
    table.className = 'rate-point-table';
    const caption = document.createElement('caption');
    caption.textContent = '同じ数値の表';
    const head = document.createElement('thead');
    const headRow = document.createElement('tr');
    const headers = [item.source === 'api_snapshot' ? '取得日時' : '日時', 'ルール', '値'];
    for (let i = 0; i < headers.length; i += 1) {
      const th = document.createElement('th');
      th.scope = 'col';
      th.textContent = headers[i];
      headRow.appendChild(th);
    }
    head.appendChild(headRow);
    const body = document.createElement('tbody');
    for (let i = 0; i < item.points.length; i += 1) {
      const point = item.points[i];
      const tr = document.createElement('tr');
      const timeCell = document.createElement('td');
      const time = document.createElement('time');
      if (typeof point.played_time === 'string') {
        time.setAttribute('datetime', point.played_time);
        time.textContent = point.played_time;
      }
      timeCell.appendChild(time);
      const ruleCell = document.createElement('td');
      ruleCell.textContent = ruleLabel || '—';
      const valueCell = document.createElement('td');
      valueCell.textContent = ikaringRateText.value(point.value, item.unit);
      tr.appendChild(timeCell);
      tr.appendChild(ruleCell);
      tr.appendChild(valueCell);
      body.appendChild(tr);
    }
    table.appendChild(caption);
    table.appendChild(head);
    table.appendChild(body);
    wrap.appendChild(table);
    return wrap;
  }

  function renderRateCharts(items) {
    if (!rateChartsEl) {
      throw new Error('Invalid rate summary');
    }
    rateChartsEl.textContent = '';
    for (let i = 0; i < items.length; i += 1) {
      const item = items[i];
      const genreLabel = formatGenre(item.genre);
      const ruleLabel = item.rule_raw ? formatRule(item.rule_raw) : '';
      const geometry = ikaringRateText.chart({
        seriesId: item.series_id,
        label: item.label,
        unit: item.unit,
        genreLabel: genreLabel,
        ruleLabel: ruleLabel,
        points: item.points,
      });
      const section = document.createElement('section');
      section.className = 'rate-chart';
      const heading = document.createElement('h3');
      const headingParts = [genreLabel];
      if (ruleLabel) {
        headingParts.push(ruleLabel);
      }
      headingParts.push(item.label);
      heading.textContent = headingParts.join(' / ');
      const meta = document.createElement('p');
      meta.className = 'rate-chart-meta';
      meta.textContent = ikaringRateText.priority(item.priority) + '、' + ikaringRateText.source(item.source);
      section.appendChild(heading);
      section.appendChild(meta);
      section.appendChild(buildRateSvg(geometry, i));
      section.appendChild(buildRatePointTable(item, ruleLabel));
      rateChartsEl.appendChild(section);
    }
  }

  function renderRateSummary(data) {
    if (!data || !Array.isArray(data.series) || Object.keys(data).some(function (key) { return key !== 'series'; })) {
      throw new Error('Invalid rate summary');
    }
    if (data.series.length === 0) {
      clearRateSummary('件数はありません');
      return;
    }
    for (let i = 0; i < data.series.length; i += 1) {
      assertRateSeries(data.series[i]);
    }
    rateSummaryBodyEl.textContent = '';
    for (let i = 0; i < data.series.length; i += 1) {
      const item = data.series[i];
      const tr = document.createElement('tr');
      const genre = document.createElement('td');
      genre.textContent = formatGenre(item.genre);
      const rule = document.createElement('td');
      rule.textContent = formatRule(item.rule_raw);
      const label = document.createElement('td');
      label.textContent = item.label;
      const priority = document.createElement('td');
      priority.textContent = ikaringRateText.priority(item.priority);
      const source = document.createElement('td');
      source.textContent = ikaringRateText.source(item.source);
      const latest = document.createElement('td');
      latest.textContent = ikaringRateText.value(item.latest, item.unit);
      const delta = document.createElement('td');
      delta.textContent = ikaringRateText.delta(item.delta, item.unit);
      const minimum = document.createElement('td');
      minimum.textContent = ikaringRateText.value(item.minimum, item.unit);
      const maximum = document.createElement('td');
      maximum.textContent = ikaringRateText.value(item.maximum, item.unit);
      const count = document.createElement('td');
      count.textContent = ikaringStatusText.count(item.count);
      tr.appendChild(genre);
      tr.appendChild(rule);
      tr.appendChild(label);
      tr.appendChild(priority);
      tr.appendChild(source);
      tr.appendChild(latest);
      tr.appendChild(delta);
      tr.appendChild(minimum);
      tr.appendChild(maximum);
      tr.appendChild(count);
      rateSummaryBodyEl.appendChild(tr);
    }
    renderRateCharts(data.series);
  }

  async function fetchRateSummary() {
    if (!rateSummaryBodyEl || !rateSummaryLoadingEl || !rateSummaryErrorEl) {
      return;
    }
    if (!currentAccount) {
      rateSummaryLoadingEl.classList.add('hidden');
      rateSummaryErrorEl.classList.add('hidden');
      clearRateSummary('アカウントがありません');
      return;
    }
    const epoch = viewEpoch;
    rateSummaryLoadingEl.classList.remove('hidden');
    rateSummaryErrorEl.classList.add('hidden');
    try {
      const params = new URLSearchParams({ account: currentAccount });
      applyDataset(params);
      const res = await apiFetch('/api/rate-summary?' + params.toString());
      if (epoch !== viewEpoch) {
        return;
      }
      if (!res.ok) {
        throw { status: res.status };
      }
      const data = await res.json();
      if (epoch !== viewEpoch) {
        return;
      }
      renderRateSummary(data);
      rateSummaryLoadingEl.classList.add('hidden');
    } catch (err) {
      if (epoch !== viewEpoch) {
        return;
      }
      rateSummaryLoadingEl.classList.add('hidden');
      rateSummaryBodyEl.textContent = '';
      if (rateChartsEl) {
        rateChartsEl.textContent = '';
      }
      rateSummaryErrorEl.classList.remove('hidden');
      rateSummaryErrorMessageEl.textContent = getErrorMessage(err && err.status, 'レートの取得に失敗しました。');
    }
  }

  /**
   * ジャンル表示変換 (未定義はraw値をそのまま表示、None/空は不明)
   */
  function formatGenre(genre) {
    if (genre === null || genre === undefined || genre === '') {
      return '不明';
    }
    if (Object.prototype.hasOwnProperty.call(GENRE_LABELS, genre)) {
      return GENRE_LABELS[genre];
    }
    if (typeof genre === 'string' && genre.startsWith('private_')) {
      const suffix = genre.slice('private_'.length);
      return 'プライベートマッチ（' + suffix + '）';
    }
    return String(genre);
  }

  function formatClassifiedGenre(record) {
    if (record && typeof record.analysis_set === 'string' && record.analysis_set) {
      return formatGenre(record.analysis_set);
    }
    return formatGenre(record ? record.genre : null);
  }

  /**
   * ルール表示変換 (未定義はraw値をそのまま表示、None/空は不明)
   */
  function applyDataset(params) {
    if (currentDataset && currentDataset !== 'unified') {
      params.set('dataset', currentDataset);
    }
    return params;
  }

  function datasetEndpoint(path) {
    if (!currentDataset || currentDataset === 'unified') {
      return path;
    }
    return path + '?dataset=' + encodeURIComponent(currentDataset);
  }

  function datasetLabel(item) {
    if (!item || item.axis === 'unified' || item.token === 'unified') {
      return '統合版';
    }
    const genre = formatGenre(item.analysis_set);
    const count = typeof item.matches === 'number' ? '（' + String(item.matches) + '）' : '';
    if (item.axis === 'mode') {
      return 'モード: ' + genre + count;
    }
    const rule = item.rule_raw ? formatRule(item.rule_raw) : 'ルール不明';
    return 'ルール: ' + genre + ' / ' + rule + count;
  }

  function resetFiltersForDataset() {
    currentKind = '';
    currentAnalysisSet = '';
    currentRule = '';
    currentWeapon = '';
    currentTagFilter = '';
    currentQuery = '';
    if (kindSelectEl) {
      kindSelectEl.value = '';
    }
    if (playedFromEl) {
      playedFromEl.value = '';
    }
    if (playedToEl) {
      playedToEl.value = '';
    }
    if (recordQueryEl) {
      recordQueryEl.value = '';
    }
  }

  function reloadAfterDatasetChange() {
    currentAccount = '';
    currentOffset = 0;
    resetFiltersForDataset();
    resetDetailPane();
    fetchStatus();
    fetchAccountsAndInitialList();
  }

  async function fetchDatasets() {
    if (!datasetSelectEl) {
      return;
    }
    const epoch = viewEpoch;
    try {
      const res = await apiFetch('/api/datasets');
      if (epoch !== viewEpoch) {
        return;
      }
      if (!res.ok) {
        throw { status: res.status };
      }
      const data = await res.json();
      if (epoch !== viewEpoch) {
        return;
      }
      if (!data || !Array.isArray(data.items) || data.items.length === 0) {
        throw new Error('Invalid datasets');
      }
      const previous = currentDataset;
      let found = false;
      const options = [];
      data.items.forEach(function (item) {
        if (!item || typeof item.token !== 'string' || typeof item.axis !== 'string') {
          return;
        }
        if (item.token.indexOf('/') >= 0 || item.token.indexOf('\\') >= 0) {
          return;
        }
        const option = document.createElement('option');
        option.value = item.token;
        option.textContent = datasetLabel(item);
        options.push(option);
        if (item.token === previous) {
          found = true;
        }
      });
      if (!options.length) {
        const option = document.createElement('option');
        option.value = 'unified';
        option.textContent = '統合版';
        options.push(option);
      }
      const nextDataset = found ? previous : options[0].value;
      rebuildingDatasets = true;
      try {
        datasetSelectEl.textContent = '';
        options.forEach(function (option) {
          datasetSelectEl.appendChild(option);
        });
        datasetSelectEl.value = nextDataset;
      } finally {
        rebuildingDatasets = false;
      }
      if (epoch !== viewEpoch) {
        return;
      }
      currentDataset = nextDataset;
      if (nextDataset !== previous) {
        viewEpoch += 1;
        reloadAfterDatasetChange();
      }
    } catch (err) {
      if (epoch !== viewEpoch) {
        return;
      }
      if (datasetSelectEl && !datasetSelectEl.options.length) {
        const option = document.createElement('option');
        option.value = 'unified';
        option.textContent = '統合版';
        datasetSelectEl.appendChild(option);
        currentDataset = 'unified';
      }
    }
  }

  function formatRule(ruleRaw) {
    if (ruleRaw === null || ruleRaw === undefined || ruleRaw === '') {
      return '不明';
    }
    if (Object.prototype.hasOwnProperty.call(RULE_LABELS, ruleRaw)) {
      return RULE_LABELS[ruleRaw];
    }
    return String(ruleRaw);
  }

  /**
   * 種別表示変換 (vs -> 対戦, coop -> バイト)
   */
  function formatKind(kind) {
    if (kind === 'vs') {
      return '対戦';
    }
    if (kind === 'coop') {
      return 'バイト';
    }
    return kind ? String(kind) : '不明';
  }

  /**
   * 詳細取得状態表示変換
   */
  function formatDetailState(state) {
    switch (state) {
      case 'available':
        return '取得済み';
      case 'pending':
        return '取得待ち';
      case 'unavailable':
        return '取得不能';
      case 'unresolved':
        return '未確定';
      default:
        return state ? String(state) : '不明';
    }
  }

  /**
   * 一般日本語エラーメッセージ取得 (技術スタックトレースや秘密情報を出さない)
   */
  function getErrorMessage(status, fallback) {
    if (status === 401 || status === 403) {
      return '認証の有効期限が切れているか、アクセス権がありません。起動用のリンクから開き直してください。';
    }
    if (status === 404) {
      return '指定された記録が見つかりませんでした。';
    }
    if (status === 502 || status === 504) {
      return 'サーバーが一時的に応答していません。時間をおいて再試行してください。';
    }
    return fallback || 'データの取得に失敗しました。時間をおいて再試行してください。';
  }

  /**
   * 安全な fetch ラッパー (Authorization Bearer, no-store, credentials omit)
   */
  async function apiFetch(endpoint, options, abortSignal) {
    const opts = options || {};
    const headers = Object.assign({}, opts.headers || {}, {
      Authorization: 'Bearer ' + authToken,
    });
    const fetchOptions = Object.assign({}, opts, {
      headers: headers,
      cache: 'no-store',
      credentials: 'omit',
    });
    if (abortSignal) {
      fetchOptions.signal = abortSignal;
    }
    return fetch(endpoint, fetchOptions);
  }

  /**
   * 収集状態の取得と表示 (GET /api/status)
   */
  async function fetchStatus() {
    if (statusAbortController) {
      statusAbortController.abort();
    }
    statusAbortController = new AbortController();
    const signal = statusAbortController.signal;
    const seq = ++statusSequence;

    statusLoadingEl.classList.remove('hidden');
    statusErrorEl.classList.add('hidden');
    statusContentEl.classList.add('hidden');
    if (statusHistoryPathsEl) {
      statusHistoryPathsEl.classList.add('hidden');
    }

    const epoch = viewEpoch;
    try {
      const res = await apiFetch(datasetEndpoint('/api/status'), {}, signal);
      if (seq !== statusSequence || epoch !== viewEpoch) {
        return;
      }
      if (!res.ok) {
        throw { status: res.status };
      }
      const data = await res.json();
      if (seq !== statusSequence || epoch !== viewEpoch) {
        return;
      }
      if (!data || typeof data !== 'object') {
        throw new Error('Invalid status data');
      }

      renderStatus(data);
      statusLoadingEl.classList.add('hidden');
      statusContentEl.classList.remove('hidden');
    } catch (err) {
      if (err && err.name === 'AbortError') {
        return;
      }
      if (seq !== statusSequence || epoch !== viewEpoch) {
        return;
      }
      statusLoadingEl.classList.add('hidden');
      statusContentEl.classList.add('hidden');
      statusErrorEl.classList.remove('hidden');
      if (statusHistoryPathsEl) {
        statusHistoryPathsEl.classList.add('hidden');
      }
      statusErrorMessageEl.textContent = getErrorMessage(err && err.status, '収集状態の取得に失敗しました。');
    }
  }

  /**
   * 収集状態の描画 (store.status schema に基づく確実な fields のみ使用)
   */
  function renderStatus(statusData) {
    let latestSuccessAt = null;
    const syncHealth = statusData && statusData.sync_health;

    if (syncHealth && Array.isArray(syncHealth.histories)) {
      for (const h of syncHealth.histories) {
        if (h && typeof h.last_success_at === 'string') {
          if (!latestSuccessAt || h.last_success_at > latestSuccessAt) {
            latestSuccessAt = h.last_success_at;
          }
        }
      }
    }

    if (latestSuccessAt && !isNaN(new Date(latestSuccessAt).getTime())) {
      statusLastCheckedEl.textContent = formatIsoDate(latestSuccessAt);
    } else {
      statusLastCheckedEl.textContent = '未確認';
    }

    // 同期状況判定 (current の場合だけ正常、delayed なら遅延、それ以外は未確認)
    if (syncHealth && syncHealth.state === 'current') {
      statusSyncStateEl.textContent = '正常（定期同期間隔内）';
      statusSyncStateEl.className = 'status-value status-ok';
    } else if (syncHealth && syncHealth.state === 'delayed') {
      statusSyncStateEl.textContent = '遅延中（同期間隔超過）';
      statusSyncStateEl.className = 'status-value status-delayed';
    } else {
      statusSyncStateEl.textContent = '未確認';
      statusSyncStateEl.className = 'status-value';
    }

    // 認証状態判定 (reauth_required最優先、session_expires_soon次、last_ok_at有効日時のときのみ最終認証成功、他未確認)
    const auth = (statusData && statusData.auth) || (syncHealth && syncHealth.auth);
    if (auth && auth.reauth_required) {
      statusAuthStateEl.textContent = '再ログインが必要です';
      statusAuthStateEl.className = 'status-value status-alert';
    } else if (auth && auth.session_expires_soon) {
      statusAuthStateEl.textContent = '有効期限が近づいています';
      statusAuthStateEl.className = 'status-value status-warning';
    } else if (auth && typeof auth.last_ok_at === 'string' && !isNaN(new Date(auth.last_ok_at).getTime())) {
      statusAuthStateEl.textContent = '最終認証成功';
      statusAuthStateEl.className = 'status-value status-ok';
    } else {
      statusAuthStateEl.textContent = '未確認';
      statusAuthStateEl.className = 'status-value';
    }

    statusMatchCountEl.textContent = ikaringStatusText.count(statusData.matches);
    statusMissingDetailEl.textContent = ikaringStatusText.count(statusData.matches_without_detail);
    statusPendingDetailsEl.textContent = ikaringStatusText.count(statusData.pending_details);
    statusUnavailableDetailsEl.textContent = ikaringStatusText.count(statusData.unavailable_details);
    statusOpenJobsEl.textContent = ikaringStatusText.openJobs(statusData.jobs);
    statusIssueCountEl.textContent = ikaringStatusText.count(statusData.issues);
    statusCoverageEl.textContent = ikaringStatusText.coverage(statusData.all_server_records_verified);
    const authFailureAt = auth && typeof auth.last_failure_at === 'string' ? formatIsoDate(auth.last_failure_at) : '';
    const syncErrorAt = auth && typeof auth.last_sync_error_at === 'string' ? formatIsoDate(auth.last_sync_error_at) : '';
    statusAuthFailureEl.textContent = ikaringStatusText.incident(auth && auth.last_failure, authFailureAt);
    statusSyncErrorEl.textContent = ikaringStatusText.incident(auth && auth.last_sync_error, syncErrorAt);
    lastStatusHistories = syncHealth && syncHealth.histories;
    lastStatusIsSlice = !!(statusData && statusData.slice === true);
    paintHistoryPaths(true);
    if (!statusHistoryTimer) {
      statusHistoryTimer = setInterval(function () {
        paintHistoryPaths(false);
      }, 60000);
    }
  }

  function paintHistoryPaths(reveal) {
    if (!statusHistoryPathsEl) {
      return;
    }
    if (!reveal && statusHistoryPathsEl.classList.contains('hidden')) {
      return;
    }
    statusHistoryPathsEl.textContent = ikaringStatusText.historyLines(lastStatusHistories, formatIsoDate);
    statusHistoryPathsEl.classList.remove('hidden');
  }

  /**
   * アカウント一覧の取得と初期化 (GET /api/accounts)
   */
  async function fetchAccountsAndInitialList() {
    accountSelectEl.disabled = true;
    recordsLoadingEl.classList.remove('hidden');
    recordsErrorEl.classList.add('hidden');

    const epoch = viewEpoch;
    try {
      const res = await apiFetch(datasetEndpoint('/api/accounts'));
      if (epoch !== viewEpoch) {
        return;
      }
      if (!res.ok) {
        throw { status: res.status };
      }
      const data = await res.json();
      if (epoch !== viewEpoch) {
        return;
      }
      if (!data || !Array.isArray(data.items)) {
        throw new Error('Invalid accounts structure');
      }

      accountSelectEl.textContent = '';

      if (data.items.length === 0) {
        currentAccount = '';
        const opt = document.createElement('option');
        opt.value = '';
        opt.textContent = 'アカウントなし';
        accountSelectEl.appendChild(opt);
        accountSelectEl.disabled = true;
        recordsLoadingEl.classList.add('hidden');

        resetDetailPane();
        renderRecordsTable([]);
        clearTagSummary('アカウントがありません');
        clearRuleResults('アカウントがありません');
        clearRateSummary('アカウントがありません');
        recordsCountEl.textContent = '件数: 0 件';
        pageDisplayEl.textContent = '0 / 0';
        prevPageBtn.disabled = true;
        nextPageBtn.disabled = true;
        return;
      }

      // アカウント選択肢の追加 (実accountIDは表示せずlabelのみ表示)
      let validAccountFound = false;
      for (const item of data.items) {
        if (item && typeof item === 'object' && typeof item.account === 'string') {
          const opt = document.createElement('option');
          opt.value = item.account;
          opt.textContent = typeof item.label === 'string' ? item.label : 'アカウント';
          accountSelectEl.appendChild(opt);
          if (!validAccountFound) {
            currentAccount = item.account;
            validAccountFound = true;
          }
        }
      }

      if (!validAccountFound) {
        currentAccount = '';
        const opt = document.createElement('option');
        opt.value = '';
        opt.textContent = 'アカウントなし';
        accountSelectEl.appendChild(opt);
        accountSelectEl.disabled = true;
        recordsLoadingEl.classList.add('hidden');

        resetDetailPane();
        renderRecordsTable([]);
        clearTagSummary('アカウントがありません');
        clearRuleResults('アカウントがありません');
        clearRateSummary('アカウントがありません');
        recordsCountEl.textContent = '件数: 0 件';
        pageDisplayEl.textContent = '0 / 0';
        prevPageBtn.disabled = true;
        nextPageBtn.disabled = true;
        return;
      }

      accountSelectEl.value = currentAccount;
      accountSelectEl.disabled = accountSelectEl.options.length <= 1;

      currentOffset = 0;
      if (epoch !== viewEpoch) {
        return;
      }
      try {
        await fetchFacets();
      } catch (facetErr) {
        if (epoch !== viewEpoch) {
          return;
        }
        markFacetCandidatesFailed(true);
      }
      if (epoch !== viewEpoch) {
        return;
      }
      await fetchRecords(0);
      if (epoch !== viewEpoch) {
        return;
      }
      await fetchTagSummary();
      if (epoch !== viewEpoch) {
        return;
      }
      await fetchRuleResults();
      if (epoch !== viewEpoch) {
        return;
      }
      await fetchRateSummary();
    } catch (err) {
      if (epoch !== viewEpoch) {
        return;
      }
      currentAccount = '';
      resetDetailPane();
      recordsTableBodyEl.textContent = '';
      recordsCountEl.textContent = '件数: -';
      pageDisplayEl.textContent = '- / -';
      recordsLoadingEl.classList.add('hidden');
      recordsErrorEl.classList.remove('hidden');
      recordsErrorMessageEl.textContent = getErrorMessage(err && err.status, 'アカウント情報の取得に失敗しました。');
      clearTagSummary('アカウントがありません');
      clearRuleResults('アカウントがありません');
      clearRateSummary('アカウントがありません');
      announceStatus('アカウント情報の取得でエラーが発生しました。');
    }
  }

  /**
   * 記録一覧の取得 (GET /api/records)
   */
  async function fetchRecords(offset) {
    if (!currentAccount) {
      return;
    }

    resetDetailPane();

    if (listAbortController) {
      listAbortController.abort();
    }
    listAbortController = new AbortController();
    const signal = listAbortController.signal;
    const seq = ++listSequence;
    const epoch = viewEpoch;

    currentOffset = offset;
    recordsTableBodyEl.textContent = '';
    recordsCountEl.textContent = '件数: 読み込み中...';
    pageDisplayEl.textContent = '- / -';

    updateControlsState(true);

    recordsLoadingEl.classList.remove('hidden');
    recordsErrorEl.classList.add('hidden');

    announceStatus('記録一覧を読み込み中...');

    try {
      const params = new URLSearchParams({
        account: currentAccount,
        limit: String(PAGE_LIMIT),
        offset: String(offset),
      });
      if (currentKind) {
        params.set('kind', currentKind);
      }
      if (currentAnalysisSet) {
        params.set('analysis_set', currentAnalysisSet);
      }
      if (currentRule) {
        params.set('rule', currentRule);
      }
      const playedFrom = playedBound(playedFromEl && playedFromEl.value);
      const playedTo = playedBound(playedToEl && playedToEl.value);
      if (playedFrom === null || playedTo === null) {
        throw { status: 400 };
      }
      if (playedFrom) {
        params.set('played_from', playedFrom);
      }
      if (playedTo) {
        params.set('played_to', playedTo);
      }
      if (playedFrom && playedTo && playedFrom > playedTo) {
        throw { status: 400 };
      }
      if (currentWeapon) {
        params.set('weapon', currentWeapon);
      }
      if (currentTagFilter) {
        params.set('tag', currentTagFilter);
      }
      if (currentQuery) {
        params.set('q', currentQuery);
      }
      applyDataset(params);

      const res = await apiFetch('/api/records?' + params.toString(), {}, signal);
      if (seq !== listSequence || epoch !== viewEpoch) {
        return;
      }
      if (!res.ok) {
        throw { status: res.status };
      }
      const data = await res.json();
      if (seq !== listSequence || epoch !== viewEpoch) {
        return;
      }
      if (!data || typeof data.total !== 'number' || !Array.isArray(data.items)) {
        throw new Error('Invalid records response structure');
      }

      currentTotal = data.total;
      renderRecordsTable(data.items);
      updatePagination(data.items.length);

      recordsLoadingEl.classList.add('hidden');
      updateControlsState(false);
      const listMessage = '記録一覧を更新しました。' + currentTotal + '件中' + data.items.length + '件を表示しています。';
      announceStatus(facetCandidatesFailed ? '絞り込み候補の取得でエラーが発生しました。' + listMessage : listMessage);
    } catch (err) {
      if (err && err.name === 'AbortError') {
        return;
      }
      if (seq !== listSequence || epoch !== viewEpoch) {
        return;
      }

      // エラー時は古い行を残さない
      recordsTableBodyEl.textContent = '';
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.setAttribute('colspan', '6');
      td.className = 'empty-cell';
      td.textContent = 'データの読み込みに失敗しました。';
      tr.appendChild(td);
      recordsTableBodyEl.appendChild(tr);

      // 詳細paneもクリアして非表示
      resetDetailPane();

      recordsLoadingEl.classList.add('hidden');
      recordsErrorEl.classList.remove('hidden');
      recordsErrorMessageEl.textContent = getErrorMessage(err && err.status, '記録一覧の取得に失敗しました。');
      updateControlsState(false);
      announceStatus('記録一覧の取得でエラーが発生しました。');
    }
  }

  /**
   * 一覧テーブルの描画 (innerHTML を使わず textContent のみで安全に構築)
   */
  function renderRecordsTable(items) {
    recordsTableBodyEl.textContent = '';

    if (!items || items.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.setAttribute('colspan', '6');
      td.className = 'empty-cell';
      td.textContent = '保存された記録はありません。';
      tr.appendChild(td);
      recordsTableBodyEl.appendChild(tr);
      return;
    }

    for (const item of items) {
      const tr = document.createElement('tr');

      // 日時 (played_time のみ。first_seen を試合日時にしない)
      const tdTime = document.createElement('td');
      tdTime.textContent = formatPlayedTime(item.played_time);
      tr.appendChild(tdTime);

      // 種別
      const tdKind = document.createElement('td');
      tdKind.textContent = formatKind(item.kind);
      tr.appendChild(tdKind);

      // ジャンル
      const tdGenre = document.createElement('td');
      tdGenre.textContent = formatClassifiedGenre(item);
      tr.appendChild(tdGenre);

      // ルール
      const tdRule = document.createElement('td');
      tdRule.textContent = formatRule(item.rule_raw);
      tr.appendChild(tdRule);

      // 詳細状態
      const tdState = document.createElement('td');
      tdState.textContent = formatDetailState(item.detail_state);
      tr.appendChild(tdState);

      // 操作
      const tdAction = document.createElement('td');
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'btn btn-table';
      btn.textContent = '詳細を開く';
      btn.setAttribute(
        'aria-label',
        formatPlayedTime(item.played_time) + ' ' + formatClassifiedGenre(item) + ' の詳細を開く'
      );
      btn.addEventListener('click', function () {
        openDetail(item.account, item.kind, item.match_key);
      });
      tdAction.appendChild(btn);
      tr.appendChild(tdAction);

      recordsTableBodyEl.appendChild(tr);
    }
  }

  /**
   * ページ送り・件数情報の更新
   */
  function updatePagination(currentCount) {
    if (currentTotal === 0) {
      recordsCountEl.textContent = '件数: 0 件';
      pageDisplayEl.textContent = '0 / 0';
      prevPageBtn.disabled = true;
      nextPageBtn.disabled = true;
      return;
    }

    const start = currentOffset + 1;
    const end = currentOffset + currentCount;
    recordsCountEl.textContent = '件数: 全 ' + currentTotal + ' 件（' + start + '〜' + end + ' 件目を表示中）';

    const currentPage = Math.floor(currentOffset / PAGE_LIMIT) + 1;
    const totalPages = Math.ceil(currentTotal / PAGE_LIMIT);
    pageDisplayEl.textContent = currentPage + ' / ' + totalPages;

    prevPageBtn.disabled = currentOffset <= 0;
    nextPageBtn.disabled = currentOffset + PAGE_LIMIT >= currentTotal;
  }

  /**
   * フォーム・ボタンの活性状態制御 (二重押下防止)
   */
  function updateControlsState(isLoading) {
    refreshBtn.disabled = isLoading;
    if (accountSelectEl.options.length > 1) {
      accountSelectEl.disabled = isLoading;
    }
    kindSelectEl.disabled = isLoading;

    if (isLoading) {
      prevPageBtn.disabled = true;
      nextPageBtn.disabled = true;
      refreshBtn.setAttribute('aria-busy', 'true');
    } else {
      prevPageBtn.disabled = currentOffset <= 0;
      nextPageBtn.disabled = currentOffset + PAGE_LIMIT >= currentTotal;
      refreshBtn.removeAttribute('aria-busy');
    }
  }

  /**
   * 詳細ペインを開く (GET /api/record)
   */
  async function openDetail(account, kind, matchKey) {
    lastDetailParams = { account: account, kind: kind, matchKey: matchKey };

    // 画面下詳細セクションを表示し、見出しに focus して導線確保、scrollIntoView
    detailSectionEl.classList.remove('hidden');
    detailHeadingEl.focus();
    detailSectionEl.scrollIntoView({ behavior: 'smooth', block: 'start' });

    clearDetailContent();
    detailLoadingEl.classList.remove('hidden');
    detailErrorEl.classList.add('hidden');
    detailBodyEl.classList.add('hidden');

    if (detailAbortController) {
      detailAbortController.abort();
    }
    detailAbortController = new AbortController();
    const signal = detailAbortController.signal;
    const seq = ++detailSequence;
    const epoch = viewEpoch;

    announceStatus('記録詳細を取得中...');

    try {
      const params = new URLSearchParams({
        account: account,
        kind: kind,
        match_key: matchKey,
      });
      applyDataset(params);

      const res = await apiFetch('/api/record?' + params.toString(), {}, signal);
      if (seq !== detailSequence || epoch !== viewEpoch) {
        return;
      }
      if (!res.ok) {
        throw { status: res.status };
      }
      const data = await res.json();
      if (seq !== detailSequence || epoch !== viewEpoch) {
        return;
      }
      if (!data || typeof data !== 'object') {
        throw new Error('Invalid record data structure');
      }

      renderDetail(data);
      detailLoadingEl.classList.add('hidden');
      detailBodyEl.classList.remove('hidden');
      announceStatus('記録詳細を表示しました。');
    } catch (err) {
      if (err && err.name === 'AbortError') {
        return;
      }
      if (seq !== detailSequence || epoch !== viewEpoch) {
        return;
      }
      detailLoadingEl.classList.add('hidden');
      detailBodyEl.classList.add('hidden');
      detailErrorEl.classList.remove('hidden');
      detailErrorMessageEl.textContent = getErrorMessage(err && err.status, '記録詳細の取得に失敗しました。');
      announceStatus('記録詳細の取得でエラーが発生しました。');
    }
  }

  /**
   * 詳細ペインのデータ描画
   */
  function renderDetail(record) {
    sourceInfoContentEl.textContent = '';
    currentSourceBase64 = null;
    sourceDownloadAreaEl.classList.add('hidden');

    detailPlayedTimeEl.textContent = formatPlayedTime(record.played_time);
    detailKindEl.textContent = formatKind(record.kind);
    detailGenreEl.textContent = formatClassifiedGenre(record);
    detailRuleEl.textContent = formatRule(record.rule_raw);
    detailStateEl.textContent = formatDetailState(record.detail_state);

    // 詳細JSON (textContentでそのまま表示、改変・再シリアライズ禁止)
    if (typeof record.detail_json === 'string' && record.detail_json.length > 0) {
      detailJsonPreEl.textContent = record.detail_json;
    } else {
      detailJsonPreEl.textContent = '保存された詳細データはありません。';
    }

    // 元の応答ダウンロードボタンと保存情報
    const source = record.source && typeof record.source === 'object' ? record.source : null;
    if (source && typeof source.body_base64 === 'string') {
      currentSourceBase64 = source.body_base64;
      sourceDownloadAreaEl.classList.remove('hidden');

      const dl = document.createElement('dl');
      dl.className = 'source-meta-grid';

      if (source.fetched_at) {
        dl.appendChild(createMetaRow('取得日時', formatIsoDate(source.fetched_at)));
      }
      if (source.body_sha256) {
        dl.appendChild(createMetaRow('SHA-256', source.body_sha256));
      }
      if (source.operation) {
        dl.appendChild(createMetaRow('操作名', source.operation));
      }
      sourceInfoContentEl.appendChild(dl);
    } else {
      sourceDownloadAreaEl.classList.add('hidden');

      const p = document.createElement('p');
      p.className = 'source-notice';
      if (record.detail_state === 'pending') {
        p.textContent = '詳細取得待ちのため、元の応答データは未取得です。';
      } else if (record.detail_state === 'unavailable') {
        p.textContent = '取得元から詳細が返されず、保存された元の応答はありません。';
      } else {
        p.textContent = '元の応答データはありません。';
      }
      sourceInfoContentEl.appendChild(p);
    }

    const hasOriginalRecords = Boolean(
      source && Object.prototype.hasOwnProperty.call(source, 'original_records')
    );
    if (hasOriginalRecords) {
      const originalRecords = source.original_records;
      const panel = document.createElement('details');
      panel.className = 'folded-panel';

      const summary = document.createElement('summary');
      summary.className = 'folded-summary';
      summary.textContent = '保存時の全取得情報';
      panel.appendChild(summary);

      const pre = document.createElement('pre');
      pre.className = 'folded-pre';
      pre.textContent = JSON.stringify(originalRecords, null, 2);
      panel.appendChild(pre);

      const bodies = originalRecords && typeof originalRecords === 'object' && !Array.isArray(originalRecords)
        ? originalRecords.bodies
        : null;
      const hasSourceBodyReference = Boolean(
        bodies && Array.isArray(bodies.values) && bodies.values.some(function (cell) {
          return cell && typeof cell === 'object'
            && cell.type === 'BLOB'
            && cell.reference === 'source_body';
        })
      );
      if (hasSourceBodyReference && source && typeof source.body_base64 === 'string') {
        const bodyNote = document.createElement('p');
        bodyNote.className = 'source-notice';
        bodyNote.textContent = '本文は「元の応答を保存」から取得できます。';
        panel.appendChild(bodyNote);
      }

      sourceInfoContentEl.appendChild(panel);
    }

    renderTagSection(record);
  }

  /**
   * 定義リスト項目の生成ヘルパー
   */
  function createMetaRow(label, value) {
    const div = document.createElement('div');
    div.className = 'meta-row';
    const dt = document.createElement('dt');
    dt.textContent = label;
    const dd = document.createElement('dd');
    dd.textContent = value;
    div.appendChild(dt);
    div.appendChild(dd);
    return div;
  }

  /**
   * 試合のタグ対象判定
   * - analysis_set が bankara_open、または genre が bankara_open なら 'open'
   * - analysis_set が private_ で始まる、または genre が private なら 'private'
   * - それ以外は null
   * analysis_set が空でない文字列なら analysis_set を優先し、genre だけでは判定しない。
   */
  function tagSurface(record) {
    if (!record || typeof record !== 'object') {
      return null;
    }
    const analysisSet = typeof record.analysis_set === 'string' ? record.analysis_set : '';
    if (analysisSet) {
      if (analysisSet === 'bankara_open') {
        return 'open';
      }
      if (analysisSet.startsWith('private_')) {
        return 'private';
      }
      return null;
    }
    const genre = typeof record.genre === 'string' ? record.genre : '';
    if (genre === 'bankara_open') {
      return 'open';
    }
    if (genre === 'private') {
      return 'private';
    }
    return null;
  }

  /**
   * 制御文字チェック (ASCII 0-31, 127)
   */
  function hasControlCharacter(str) {
    for (let i = 0; i < str.length; i++) {
      const code = str.charCodeAt(i);
      if (code < 32 || code === 127) {
        return true;
      }
    }
    return false;
  }

  function showTagError(message) {
    if (tagErrorEl) {
      tagErrorEl.textContent = message;
      tagErrorEl.classList.remove('hidden');
    }
  }

  function hideTagError() {
    if (tagErrorEl) {
      tagErrorEl.textContent = '';
      tagErrorEl.classList.add('hidden');
    }
  }

  function setTagControlsDisabled(disabled) {
    if (tagAddButtonEl) {
      tagAddButtonEl.disabled = disabled;
    }
    if (tagSuggestionsEl) {
      const buttons = tagSuggestionsEl.querySelectorAll('button');
      for (const btn of buttons) {
        btn.disabled = disabled;
      }
    }
    if (tagListEl) {
      const buttons = tagListEl.querySelectorAll('button');
      for (const btn of buttons) {
        btn.disabled = disabled;
      }
    }
  }

  /**
   * タグ一覧の描画 (innerHTML を使わず textContent と createElement のみ)
   */
  function renderTagList(tags) {
    if (!tagListEl || !tagEmptyEl) {
      return;
    }
    tagListEl.textContent = '';
    const tagArray = Array.isArray(tags) ? tags : [];

    if (tagArray.length === 0) {
      tagEmptyEl.classList.remove('hidden');
      return;
    }
    tagEmptyEl.classList.add('hidden');

    for (const item of tagArray) {
      if (!item || typeof item !== 'object') {
        continue;
      }
      const tagText = typeof item.tag === 'string' ? item.tag : '';
      if (!tagText) {
        continue;
      }
      const li = document.createElement('li');
      li.className = 'tag-item';

      const contentSpan = document.createElement('span');
      contentSpan.className = 'tag-item-content';

      const nameSpan = document.createElement('span');
      nameSpan.className = 'tag-name';
      nameSpan.textContent = tagText;
      contentSpan.appendChild(nameSpan);

      if (typeof item.note === 'string' && item.note.length > 0) {
        const noteSpan = document.createElement('span');
        noteSpan.className = 'tag-item-note';
        noteSpan.textContent = item.note;
        contentSpan.appendChild(noteSpan);
      }

      if (!lastStatusIsSlice) {
        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'btn btn-tag-remove';
        removeBtn.textContent = '外す';
        removeBtn.setAttribute('aria-label', tagText + ' を外す');
        removeBtn.addEventListener('click', function () {
          submitTagAction('remove', tagText);
        });
        contentSpan.appendChild(removeBtn);
      }

      li.appendChild(contentSpan);
      tagListEl.appendChild(li);
    }
  }

  let tagOperationInProgress = false;

  /**
   * タグの追加・削除送信 (POST /api/tags)
   */
  async function submitTagAction(action, tag) {
    if (lastStatusIsSlice || tagOperationInProgress) {
      return;
    }
    if (!lastDetailParams || !lastDetailParams.account || !lastDetailParams.matchKey) {
      return;
    }

    const currentSeq = detailSequence;
    hideTagError();
    tagOperationInProgress = true;
    setTagControlsDisabled(true);

    try {
      const payload = {
        account: lastDetailParams.account,
        match_key: lastDetailParams.matchKey,
        action: action,
        tag: tag,
      };

      const res = await apiFetch('/api/tags', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      if (currentSeq !== detailSequence) {
        return;
      }

      if (!res.ok) {
        throw { status: res.status };
      }

      const data = await res.json();
      if (currentSeq !== detailSequence) {
        return;
      }

      if (!data || !Array.isArray(data.tags)) {
        showTagError(getErrorMessage(502, 'タグの更新に失敗しました。'));
        announceStatus('タグの更新でエラーが発生しました。');
        return;
      }
      renderTagList(data.tags);
      if (action === 'add' && tagInputEl) {
        tagInputEl.value = '';
      }
      announceStatus(action === 'add' ? 'タグを追加しました。' : 'タグを外しました。');
      fetchFacets().catch(function () {
        facetCandidatesFailed = true;
        const tagMessage = action === 'add' ? 'タグを追加しました。' : 'タグを外しました。';
        announceStatus('絞り込み候補の取得でエラーが発生しました。' + tagMessage);
      });
      fetchTagSummary();
    } catch (err) {
      if (currentSeq !== detailSequence) {
        return;
      }
      const status = err && err.status;
      if (status === 403) {
        showTagError('この試合には画面からタグを付けられません。');
      } else {
        showTagError(getErrorMessage(status, 'タグの更新に失敗しました。'));
      }
      announceStatus('タグの更新でエラーが発生しました。');
    } finally {
      if (currentSeq === detailSequence) {
        tagOperationInProgress = false;
        setTagControlsDisabled(false);
      }
    }
  }

  /**
   * タグセクションの描画
   */
  function renderTagSection(record) {
    const surface = tagSurface(record);
    if (!surface || !tagSectionEl) {
      if (tagSectionEl) {
        tagSectionEl.classList.add('hidden');
      }
      return;
    }

    tagSectionEl.classList.remove('hidden');
    hideTagError();
    tagOperationInProgress = false;
    if (tagFormEl) {
      tagFormEl.classList.toggle('hidden', lastStatusIsSlice);
    }

    if (tagScopeNoteEl) {
      if (lastStatusIsSlice) {
        tagScopeNoteEl.textContent = 'このファイルはモード別の派生です。付いているタグは表示します。追加と削除は、統合版のデータベースに対して行います。';
      } else if (surface === 'open') {
        tagScopeNoteEl.textContent = 'オープンの試合に付けるタグです。下げラン、練習、エンジョイ、ガチは候補です。ほかのタグも追加できます。';
      } else if (surface === 'private') {
        tagScopeNoteEl.textContent = 'プラベの試合に付けるタグです。エンジョイ、対抗戦、イカップルは候補です。ほかのタグも追加できます。';
      } else {
        tagScopeNoteEl.textContent = '';
      }
    }

    if (tagSuggestionsEl) {
      tagSuggestionsEl.textContent = '';
      if (lastStatusIsSlice) {
        renderTagList(record && record.tags);
        return;
      }
      const suggestions = surface === 'open' ? OPEN_TAG_SUGGESTIONS : PRIVATE_TAG_SUGGESTIONS;
      for (const text of suggestions) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'btn btn-secondary btn-tag-suggestion';
        btn.textContent = text;
        btn.addEventListener('click', function () {
          submitTagAction('add', text);
        });
        tagSuggestionsEl.appendChild(btn);
      }
    }

    renderTagList(record && record.tags);
  }

  /**
   * 詳細内容のクリア
   */
  function clearDetailContent() {
    detailPlayedTimeEl.textContent = '-';
    detailKindEl.textContent = '-';
    detailGenreEl.textContent = '-';
    detailRuleEl.textContent = '-';
    detailStateEl.textContent = '-';
    detailJsonPreEl.textContent = '';
    sourceInfoContentEl.textContent = '';
    currentSourceBase64 = null;
    sourceDownloadAreaEl.classList.add('hidden');
    foldedDetailJsonEl.open = false;
    foldedSourceInfoEl.open = false;

    if (tagSectionEl) {
      tagSectionEl.classList.add('hidden');
    }
    if (tagListEl) {
      tagListEl.textContent = '';
    }
    if (tagEmptyEl) {
      tagEmptyEl.classList.add('hidden');
    }
    if (tagSuggestionsEl) {
      tagSuggestionsEl.textContent = '';
    }
    if (tagScopeNoteEl) {
      tagScopeNoteEl.textContent = '';
    }
    if (tagInputEl) {
      tagInputEl.value = '';
    }
    if (tagErrorEl) {
      tagErrorEl.textContent = '';
      tagErrorEl.classList.add('hidden');
    }
    tagOperationInProgress = false;
  }

  /**
   * 詳細ペイン全体のリセットと非表示
   */
  function resetDetailPane() {
    detailSequence++;
    clearDetailContent();
    detailSectionEl.classList.add('hidden');
    detailErrorEl.classList.add('hidden');
    detailLoadingEl.classList.add('hidden');
    detailBodyEl.classList.add('hidden');
    lastDetailParams = null;
    if (detailAbortController) {
      detailAbortController.abort();
      detailAbortController = null;
    }
  }

  /**
   * 元の応答本文のダウンロード
   * (元本文をJSON parseして再 serialize せず、Uint8Array経由でBlobダウンロード)
   */
  function downloadSourceResponse() {
    if (!currentSourceBase64 || typeof currentSourceBase64 !== 'string') {
      return;
    }
    try {
      const binaryString = window.atob(currentSourceBase64);
      const len = binaryString.length;
      const bytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) {
        bytes[i] = binaryString.charCodeAt(i);
      }
      const blob = new Blob([bytes], { type: 'application/json' });
      const blobUrl = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = blobUrl;
      link.download = 'record-response.json';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      setTimeout(function () {
        URL.revokeObjectURL(blobUrl);
      }, 1000);
      announceStatus('元の応答ファイルを保存しました。');
    } catch (err) {
      announceStatus('ファイルの保存に失敗しました。');
    }
  }

  /**
   * 旧分析表の利用を拒否し、準備中であることを画面に示す。
   */
  async function downloadPublishedXlsx() {
    if (!publishedXlsxButtonEl) {
      return;
    }
    publishedXlsxButtonEl.disabled = true;
    if (publishedXlsxMessageEl) {
      publishedXlsxMessageEl.textContent = '旧分析表には欠落があるため利用できません。全情報の書き出しは準備中です。';
      publishedXlsxMessageEl.classList.remove('hidden');
    }
  }

  /**
   * アプリケーション初期化
   */
  async function init() {
    // DOM参照の解決
    statusLiveEl = document.getElementById('status-live');
    noTokenContainerEl = document.getElementById('no-token-container');
    appContainerEl = document.getElementById('app-container');

    statusLoadingEl = document.getElementById('status-loading');
    statusErrorEl = document.getElementById('status-error');
    statusErrorMessageEl = document.getElementById('status-error-message');
    statusRetryBtn = document.getElementById('status-retry-button');
    statusContentEl = document.getElementById('status-content');
    statusLastCheckedEl = document.getElementById('status-last-checked');
    statusSyncStateEl = document.getElementById('status-sync-state');
    statusAuthStateEl = document.getElementById('status-auth-state');
    statusMatchCountEl = document.getElementById('status-match-count');
    statusMissingDetailEl = document.getElementById('status-missing-detail');
    statusPendingDetailsEl = document.getElementById('status-pending-details');
    statusUnavailableDetailsEl = document.getElementById('status-unavailable-details');
    statusOpenJobsEl = document.getElementById('status-open-jobs');
    statusIssueCountEl = document.getElementById('status-issue-count');
    statusCoverageEl = document.getElementById('status-coverage');
    statusAuthFailureEl = document.getElementById('status-auth-failure');
    statusSyncErrorEl = document.getElementById('status-sync-error');
    statusHistoryPathsEl = document.getElementById('status-history-paths');

    filterFormEl = document.getElementById('filter-form');
    datasetSelectEl = document.getElementById('dataset-select');
    accountSelectEl = document.getElementById('account-select');
    kindSelectEl = document.getElementById('kind-select');
    analysisSetSelectEl = document.getElementById('analysis-set-select');
    ruleSelectEl = document.getElementById('rule-select');
    playedFromEl = document.getElementById('played-from');
    playedToEl = document.getElementById('played-to');
    weaponSelectEl = document.getElementById('weapon-select');
    tagFilterSelectEl = document.getElementById('tag-filter-select');
    recordQueryEl = document.getElementById('record-query');
    tagSummaryLoadingEl = document.getElementById('tag-summary-loading');
    tagSummaryErrorEl = document.getElementById('tag-summary-error');
    tagSummaryErrorMessageEl = document.getElementById('tag-summary-error-message');
    tagSummaryBodyEl = document.getElementById('tag-summary-body');
    ruleResultsLoadingEl = document.getElementById('rule-results-loading');
    ruleResultsErrorEl = document.getElementById('rule-results-error');
    ruleResultsErrorMessageEl = document.getElementById('rule-results-error-message');
    ruleResultsBodyEl = document.getElementById('rule-results-body');
    rateSummaryLoadingEl = document.getElementById('rate-summary-loading');
    rateSummaryErrorEl = document.getElementById('rate-summary-error');
    rateSummaryErrorMessageEl = document.getElementById('rate-summary-error-message');
    rateSummaryBodyEl = document.getElementById('rate-summary-body');
    rateChartsEl = document.getElementById('rate-charts');
    publishedXlsxButtonEl = document.getElementById('published-xlsx-button');
    publishedXlsxMessageEl = document.getElementById('published-xlsx-message');
    refreshBtn = document.getElementById('refresh-button');

    recordsCountEl = document.getElementById('records-count');
    prevPageBtn = document.getElementById('prev-page-button');
    nextPageBtn = document.getElementById('next-page-button');
    pageDisplayEl = document.getElementById('page-display');
    recordsLoadingEl = document.getElementById('records-loading');
    recordsErrorEl = document.getElementById('records-error');
    recordsErrorMessageEl = document.getElementById('records-error-message');
    recordsRetryBtn = document.getElementById('records-retry-button');
    recordsTableBodyEl = document.getElementById('records-table-body');

    detailSectionEl = document.getElementById('detail-section');
    detailHeadingEl = document.getElementById('detail-heading');
    detailLoadingEl = document.getElementById('detail-loading');
    detailErrorEl = document.getElementById('detail-error');
    detailErrorMessageEl = document.getElementById('detail-error-message');
    detailRetryBtn = document.getElementById('detail-retry-button');
    detailBodyEl = document.getElementById('detail-body');
    detailPlayedTimeEl = document.getElementById('detail-played-time');
    detailKindEl = document.getElementById('detail-kind');
    detailGenreEl = document.getElementById('detail-genre');
    detailRuleEl = document.getElementById('detail-rule');
    detailStateEl = document.getElementById('detail-state');
    sourceDownloadAreaEl = document.getElementById('source-download-area');
    downloadSourceBtn = document.getElementById('download-source-button');
    foldedDetailJsonEl = document.getElementById('folded-detail-json');
    detailJsonPreEl = document.getElementById('detail-json-pre');
    foldedSourceInfoEl = document.getElementById('folded-source-info');
    sourceInfoContentEl = document.getElementById('source-info-content');

    tagSectionEl = document.getElementById('tag-section');
    tagHeadingEl = document.getElementById('tag-heading');
    tagScopeNoteEl = document.getElementById('tag-scope-note');
    tagListEl = document.getElementById('tag-list');
    tagEmptyEl = document.getElementById('tag-empty');
    tagSuggestionsEl = document.getElementById('tag-suggestions');
    tagFormEl = document.getElementById('tag-form');
    tagInputEl = document.getElementById('tag-input');
    tagAddButtonEl = document.getElementById('tag-add-button');
    tagErrorEl = document.getElementById('tag-error');

    // イベント登録
    if (filterFormEl) {
      filterFormEl.addEventListener('submit', function (e) {
        e.preventDefault();
        applyRecordFilters();
      });
    }

    if (tagFormEl) {
      tagFormEl.addEventListener('submit', function (e) {
        e.preventDefault();
        if (!tagInputEl) {
          return;
        }
        const trimmed = tagInputEl.value.trim();
        if (!trimmed) {
          showTagError('タグを入力してください。');
          return;
        }
        if (trimmed.length > 80) {
          showTagError('タグは80文字までです。');
          return;
        }
        if (hasControlCharacter(trimmed)) {
          showTagError('タグに使えない文字があります。');
          return;
        }
        hideTagError();
        submitTagAction('add', trimmed);
      });
    }

    if (datasetSelectEl) {
      datasetSelectEl.addEventListener('change', function () {
        if (rebuildingDatasets) {
          return;
        }
        viewEpoch += 1;
        currentDataset = datasetSelectEl.value || 'unified';
        reloadAfterDatasetChange();
      });
    }

    accountSelectEl.addEventListener('change', function () {
      const epoch = viewEpoch;
      currentAccount = accountSelectEl.value;
      currentOffset = 0;
      resetDetailPane();
      fetchFacets().then(function () {
        if (epoch !== viewEpoch) {
          return;
        }
        fetchRecords(0);
      }).catch(function () {
        if (epoch !== viewEpoch) {
          return;
        }
        markFacetCandidatesFailed(true);
        fetchRecords(0);
      });
      fetchTagSummary();
      fetchRuleResults();
      fetchRateSummary();
    });

    [kindSelectEl, analysisSetSelectEl, ruleSelectEl, weaponSelectEl, tagFilterSelectEl].forEach(function (select) {
      select.addEventListener('change', applyRecordFilters);
    });
    playedFromEl.addEventListener('change', applyRecordFilters);
    playedToEl.addEventListener('change', applyRecordFilters);
    recordQueryEl.addEventListener('change', applyRecordFilters);

    refreshBtn.addEventListener('click', function () {
      fetchDatasets();
      fetchStatus();
      if (!currentAccount) {
        fetchAccountsAndInitialList();
      } else {
        const epoch = viewEpoch;
        fetchFacets().catch(function () {
          if (epoch !== viewEpoch) {
            return;
          }
          markFacetCandidatesFailed(false);
        });
        fetchRecords(currentOffset);
        fetchTagSummary();
        fetchRuleResults();
        fetchRateSummary();
      }
    });

    statusRetryBtn.addEventListener('click', function () {
      fetchStatus();
    });

    recordsRetryBtn.addEventListener('click', function () {
      if (!currentAccount) {
        fetchAccountsAndInitialList();
      } else {
        fetchRecords(currentOffset);
      }
    });

    prevPageBtn.addEventListener('click', function () {
      if (currentOffset > 0) {
        fetchRecords(Math.max(0, currentOffset - PAGE_LIMIT));
      }
    });

    nextPageBtn.addEventListener('click', function () {
      if (currentOffset + PAGE_LIMIT < currentTotal) {
        fetchRecords(currentOffset + PAGE_LIMIT);
      }
    });

    detailRetryBtn.addEventListener('click', function () {
      if (lastDetailParams) {
        openDetail(lastDetailParams.account, lastDetailParams.kind, lastDetailParams.matchKey);
      }
    });

    downloadSourceBtn.addEventListener('click', function () {
      downloadSourceResponse();
    });

    if (publishedXlsxButtonEl) {
      publishedXlsxButtonEl.addEventListener('click', function () {
        downloadPublishedXlsx();
      });
    }

    // 1. URL fragment (#token=...) を起動時に1回だけ取得
    const hash = window.location.hash;
    if (hash && hash.startsWith('#')) {
      const params = new URLSearchParams(hash.slice(1));
      const token = params.get('token');
      if (token) {
        authToken = token;
      }
    }

    // 2. 直ちに history.replaceState で消す
    try {
      window.history.replaceState(null, '', window.location.pathname);
    } catch (e) {
      // ignore
    }

    // 3. トークン有無の判定
    if (!authToken) {
      noTokenContainerEl.classList.remove('hidden');
      appContainerEl.classList.add('hidden');
      return;
    }

    noTokenContainerEl.classList.add('hidden');
    appContainerEl.classList.remove('hidden');

    // 4. 初回データ取得。記録ファイルの一覧を先に置き、その選択で状態と一覧を読む。
    await fetchDatasets();
    await Promise.all([
      fetchStatus(),
      fetchAccountsAndInitialList(),
    ]);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
