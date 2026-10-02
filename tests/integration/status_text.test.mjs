import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../assets/app/app.js', import.meta.url), 'utf8');
const marker = "(function () {\n  'use strict';";
const cut = source.indexOf(marker);
assert.equal(source.slice(0, cut).split(marker).length, 1);
assert.notEqual(cut, -1);

test('collection status text uses only exact counts and known codes', () => {
  const sandbox = {};
  vm.runInNewContext(source.slice(0, cut), sandbox);
  const text = sandbox.ikaringStatusText;
  assert.equal(text.count(0), '0');
  assert.equal(text.count(12), '12');
  assert.equal(text.count(null), '未確認');
  assert.equal(text.count(-1), '未確認');
  assert.equal(text.count(1.5), '未確認');
  assert.equal(text.count(Number.MAX_SAFE_INTEGER + 1), '未確認');

  assert.equal(text.openJobs([]), '0');
  assert.equal(text.openJobs([
    { state: 'pending', count: 2 },
    { state: 'retry', count: 1 },
    { state: 'awaiting_scope', count: 4 },
    { state: 'done', count: 9 },
    { state: 'unavailable', count: 3 },
  ]), '7');
  assert.equal(text.openJobs(undefined), '未確認');
  assert.equal(text.openJobs([{ state: 'pending', count: 1.2 }]), '未確認');
  assert.equal(text.openJobs([null]), '未確認');
  assert.equal(text.openJobs([{ state: 'done', count: 'x' }, { state: 'pending', count: 1 }]), '1');

  assert.equal(text.coverage(false), '未了');
  assert.equal(text.coverage(true), '照合済み');
  assert.equal(text.coverage(0), '未確認');
  assert.equal(text.coverage(null), '未確認');

  assert.equal(text.incident(null, ''), '記録なし');
  assert.equal(text.incident('', ''), '記録なし');
  assert.equal(text.incident('AUTH_EXPIRED', '2026/10/01 09:00:00'), 'AUTH_EXPIRED 2026/10/01 09:00:00');
  assert.equal(text.incident('AUTH_EXPIRED', '不明'), 'AUTH_EXPIRED');
  assert.equal(text.incident('auth', ''), '未確認');
  assert.equal(text.incident('EXPORT_FAILED', ''), 'EXPORT_FAILED');

  const at = Date.parse('2026-10-01T07:00:00+00:00');
  const lines = text.historyLines([
    { operation: 'RegularBattleHistoriesQuery', last_success_at: '2026-10-01T07:00:00+00:00', stale: false },
    { operation: 'CoopHistoryQuery', last_success_at: '2026-10-01T06:00:00+00:00', stale: true },
  ], function (value) { return 'T:' + value; }, at + 5 * 60 * 1000);
  assert.equal(lines, [
    '最新の履歴 遅延 未確認',
    'レギュラーマッチ 確認済み T:2026-10-01T07:00:00+00:00',
    'バンカラマッチ 遅延 未確認',
    'Xマッチ 遅延 未確認',
    'イベントマッチ 遅延 未確認',
    'プライベートマッチ 遅延 未確認',
    'バイト 遅延 T:2026-10-01T06:00:00+00:00',
  ].join('\n'));
  assert.equal(text.historyLines(null, null, at).split('\n').length, 7);
  const freshEdge = text.historyLines([
    { operation: 'RegularBattleHistoriesQuery', last_success_at: '2026-10-01T07:00:00+00:00', stale: false },
  ], function (value) { return 'T:' + value; }, at + 600000);
  assert.equal(freshEdge.split('\n')[1], 'レギュラーマッチ 確認済み T:2026-10-01T07:00:00+00:00');
  const lateEdge = text.historyLines([
    { operation: 'RegularBattleHistoriesQuery', last_success_at: '2026-10-01T07:00:00+00:00', stale: false },
  ], function (value) { return 'T:' + value; }, at + 600001);
  assert.equal(lateEdge.split('\n')[1], 'レギュラーマッチ 遅延 T:2026-10-01T07:00:00+00:00');
  const unreadable = text.historyLines([
    { operation: 'XBattleHistoriesQuery', last_success_at: 'not-a-time', stale: false },
  ], function (value) { return 'T:' + value; }, at);
  assert.equal(unreadable.split('\n')[3], 'Xマッチ 遅延 未確認');
});

test('programmatic legacy xlsx handler stays disabled without fetch or download', async () => {
  const start = source.indexOf('  async function downloadPublishedXlsx() {');
  const end = source.indexOf('\n\n  /**\n   * アプリケーション初期化', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  const handler = source.slice(start, end);
  const calls = { fetch: 0, download: 0 };
  const button = { disabled: true };
  const classes = new Set(['hidden']);
  const message = {
    textContent: '',
    classList: {
      add(name) { classes.add(name); },
      remove(name) { classes.delete(name); },
    },
  };
  const sandbox = {
    button,
    message,
    apiFetch: async () => {
      calls.fetch += 1;
      return { ok: true, blob: async () => ({}) };
    },
    document: {
      createElement() {
        calls.download += 1;
        return { click() { calls.download += 1; } };
      },
      body: { appendChild() {}, removeChild() {} },
    },
    URL: {
      createObjectURL() { calls.download += 1; return 'blob:fake'; },
      revokeObjectURL() { calls.download += 1; },
    },
    setTimeout() { calls.download += 1; },
    announceStatus() { calls.download += 1; },
  };

  await vm.runInNewContext(`
    let publishedXlsxButtonEl = button;
    let publishedXlsxMessageEl = message;
    ${handler}
    downloadPublishedXlsx();
  `, sandbox);

  assert.deepEqual(calls, { fetch: 0, download: 0 });
  assert.equal(button.disabled, true);
  assert.equal(message.textContent, '旧分析表には欠落があるため利用できません。全情報の書き出しは準備中です。');
  assert.equal(classes.has('hidden'), false);
});

test('desktop and rate screen list the same seven history paths', () => {
  const js = readFileSync(new URL('../../assets/app/app.js', import.meta.url), 'utf8');
  const py = readFileSync(new URL('../../src/python/ikarchive/gui.py', import.meta.url), 'utf8');
  const html = readFileSync(new URL('../../assets/app/index.html', import.meta.url), 'utf8');
  const ops = [
    'LatestBattleHistoriesQuery',
    'RegularBattleHistoriesQuery',
    'BankaraBattleHistoriesQuery',
    'XBattleHistoriesQuery',
    'EventBattleHistoriesQuery',
    'PrivateBattleHistoriesQuery',
    'CoopHistoryQuery',
  ];
  function ordered(source, pattern) {
    const found = [];
    for (const match of source.matchAll(pattern)) {
      if (ops.includes(match[1])) {
        found.push(match[1] + '\t' + match[2]);
      }
    }
    return found;
  }
  const jsPairs = ordered(js, /\['([A-Za-z]+)', '([^']+)'\]/g);
  const pyPairs = ordered(py, /\('([A-Za-z]+)', '([^']+)'\)/g);
  assert.deepEqual(jsPairs, pyPairs);
  assert.deepEqual(jsPairs.map((pair) => pair.split('\t')[0]), ops);
  assert.equal(html.includes('id="status-history-paths"'), true);
  assert.equal(html.includes('class="status-history-paths hidden"'), true);
  assert.equal(/paintHistoryPaths\(false\);\s*\}, 60000\);/.test(js), true);
});

test('rate text keeps stored ratios and hides unknown units', () => {
  const sandbox = {};
  vm.runInNewContext(source.slice(0, cut), sandbox);
  const rate = sandbox.ikaringRateText;
  assert.equal(rate.value(2100, 'number'), '2,100');
  assert.equal(rate.value(1.25, 'number'), '1.25');
  assert.equal(rate.value(0.2, 'ratio'), '20%');
  assert.equal(rate.value(0.25, 'ratio'), '25%');
  assert.equal(rate.value(0.1234, 'ratio'), '12.34%');
  assert.equal(rate.delta(100, 'number'), '+100');
  assert.equal(rate.delta(-5, 'number'), '-5');
  assert.equal(rate.delta(0, 'number'), '±0');
  assert.equal(rate.delta(null, 'number'), '—');
  assert.equal(rate.delta(0.05, 'ratio'), '+5%');
  assert.equal(rate.value(Number.NaN, 'number'), '未確認');
  assert.equal(rate.value(2100, 'points'), '未確認');
  assert.equal(rate.priority('primary'), '主指標');
  assert.equal(rate.priority('secondary'), '副指標');
  assert.equal(rate.priority('derived'), '未確認');
  assert.equal(rate.source('api'), '応答の数値');
  assert.equal(rate.source('api_snapshot'), '取得時点');
  assert.equal(rate.source('derived_judgement'), '未確認');
});

function pointsFrom(values, timeAt) {
  return values.map(function (value, index) {
    return { played_time: timeAt(index, value), value: value };
  });
}

function copied(values, pick) {
  return Array.from(values, pick || function (value) { return value; });
}

test('rate chart geometry matches the existing rate screen', () => {
  const sandbox = {};
  vm.runInNewContext(source.slice(0, cut), sandbox);
  const rate = sandbox.ikaringRateText;

  const crossing = rate.chart({
    seriesId: 'bankara_challenge|AREA|bankaraPower',
    label: 'バンカラパワー',
    unit: 'number',
    genreLabel: 'バンカラマッチ（チャレンジ）',
    ruleLabel: 'ガチエリア',
    points: pointsFrom([-10, 0, 10], function (index) {
      return '2026-09-22T03:0' + index + ':00Z';
    }),
  });
  assert.deepEqual(copied(crossing.yTicks, function (tick) { return tick.value; }), [-10, -5, 0, 5, 10]);
  assert.equal(crossing.yTicks.some(function (tick) { return tick.zero; }), true);
  assert.deepEqual(copied(crossing.xIndices), [0, 1, 2]);
  assert.deepEqual(copied(crossing.xTicks, function (tick) { return tick.label; }), ['09/22 03:00', '09/22 03:01', '09/22 03:02']);
  assert.equal(typeof crossing.line, 'string');
  assert.equal(typeof crossing.area, 'string');
  assert.equal(crossing.focus, true);
  assert.equal(crossing.title.includes('bankaraPower'), false);
  assert.equal(crossing.desc.includes('bankaraPower'), false);
  assert.equal(crossing.desc.includes('バンカラマッチ（チャレンジ） ガチエリア バンカラパワー'), true);

  const single = rate.chart({
    seriesId: 'nawabari|TURF_WAR|vibes',
    label: 'チョーシ',
    unit: 'number',
    genreLabel: 'ナワバリバトル',
    ruleLabel: 'ナワバリバトル',
    points: [{ played_time: '2026-09-22T04:00:00Z', value: 4 }],
  });
  assert.equal(single.line, null);
  assert.equal(single.area, null);
  assert.equal(single.focus, false);
  assert.equal(single.dots.length, 1);
  assert.equal(single.dots[0].cx, '376.0');
  assert.equal(single.dots[0].cy, '132.0');
  assert.deepEqual(copied(single.yTicks, function (tick) { return tick.value; }), [3, 3.5, 4, 4.5, 5]);

  const broken = rate.chart({
    seriesId: 'bankara_challenge|AREA|bankaraPower',
    label: 'バンカラパワー',
    unit: 'number',
    points: [{ played_time: 'not-a-valid-iso-time', value: 2000 }],
  });
  assert.equal(broken.xTicks[0].label, 'not-a-valid-iso-');

  const many = [];
  for (let i = 0; i < 7; i += 1) {
    many.push({ played_time: '2026-09-22T03:0' + i + ':00Z', value: i });
  }
  const wide = rate.chart({
    seriesId: 'bankara_open|AREA|bankaraPower',
    label: 'バンカラパワー',
    unit: 'number',
    points: many,
  });
  assert.deepEqual(copied(wide.xIndices), [0, 2, 3, 4, 6]);
  assert.equal(wide.xTicks.length, 5);

  const grade = rate.chart({
    seriesId: 'fest|AREA|gradePoint',
    label: 'グレード',
    unit: 'number',
    points: [
      { played_time: '2026-09-22T03:00:00Z', value: 0 },
      { played_time: '2026-09-22T04:00:00Z', value: 999 },
    ],
  });
  assert.deepEqual(copied(grade.yTicks, function (tick) { return tick.value; }), [0, 200, 400, 600, 800, 999]);
  assert.equal(grade.yTicks.some(function (tick) { return tick.value === 1200; }), false);
  assert.equal(grade.desc.includes('gradePoint'), false);
  assert.equal(grade.title.includes('gradePoint'), false);

  const danger = rate.chart({
    seriesId: 'salmon_regular|REGULAR|dangerRate',
    label: 'キケン度',
    unit: 'ratio',
    genreLabel: 'いつものバイト',
    ruleLabel: 'いつものバイト',
    points: [
      { played_time: '2026-09-01T00:00:00Z', value: 0.2 },
      { played_time: '2026-09-02T00:00:00Z', value: 0.25 },
    ],
  });
  assert.deepEqual(copied(danger.plotted), [20, 25]);
  assert.deepEqual(copied(danger.yTicks, function (tick) { return tick.value; }), [20, 21, 22, 23, 24, 25]);
  assert.equal(danger.yTicks.every(function (tick) { return tick.label.endsWith('%'); }), true);
  assert.equal(danger.desc.includes('25%'), true);
  assert.equal(danger.desc.includes('0.25'), false);
  assert.equal(danger.dots[1].title.includes('25%'), true);
  assert.equal(danger.dots[1].title.includes('(最新)'), true);

  assert.throws(function () {
    rate.chart({ seriesId: 'x|y|z', label: '空', unit: 'number', points: [] });
  });
});

function asyncFunctionSource(name) {
  const signature = 'async function ' + name + '(';
  const start = source.indexOf(signature);
  assert.notEqual(start, -1, name);
  const brace = source.indexOf('{', start);
  let depth = 0;
  for (let i = brace; i < source.length; i += 1) {
    if (source[i] === '{') depth += 1;
    else if (source[i] === '}') {
      depth -= 1;
      if (depth === 0) return source.slice(start, i + 1);
    }
  }
  throw new Error('unclosed ' + name);
}

test('a failed read from the previous dataset does not paint the new one', () => {
  const names = ['fetchTagSummary', 'fetchRuleResults', 'fetchRateSummary', 'fetchAccountsAndInitialList'];
  for (const name of names) {
    const body = asyncFunctionSource(name);
    const tryAt = body.indexOf('try {');
    const epochAt = body.indexOf('const epoch = viewEpoch;');
    const catchAt = body.lastIndexOf('catch (err) {');
    assert.notEqual(tryAt, -1, name);
    assert.notEqual(epochAt, -1, name);
    assert.notEqual(catchAt, -1, name);
    assert.equal(epochAt < tryAt, true, name);
    const caught = body.slice(catchAt);
    assert.equal(caught.includes('if (epoch !== viewEpoch)'), true, name);
    assert.equal(caught.indexOf('if (epoch !== viewEpoch)') < caught.indexOf('textContent'), true, name);
  }
  const accounts = asyncFunctionSource('fetchAccountsAndInitialList');
  const follow = accounts.slice(accounts.indexOf('currentOffset = 0;'));
  const calls = ['fetchFacets', 'fetchRecords', 'fetchTagSummary', 'fetchRuleResults', 'fetchRateSummary'];
  let cursor = 0;
  for (const call of calls) {
    const guard = follow.indexOf('if (epoch !== viewEpoch)', cursor);
    const invoke = follow.indexOf('await ' + call + '(', cursor);
    assert.notEqual(guard, -1, call);
    assert.notEqual(invoke, -1, call);
    assert.equal(guard < invoke, true, call);
    cursor = invoke + call.length;
  }
});

test('refresh rereads datasets and a stale failure does not rewrite the select', () => {
  const body = asyncFunctionSource('fetchDatasets');
  const compared = body.indexOf('nextDataset !== previous');
  const bumped = body.indexOf('viewEpoch += 1');
  assert.notEqual(compared, -1);
  assert.equal(compared < bumped, true);
  const catchAt = body.lastIndexOf('catch (err) {');
  const caught = body.slice(catchAt);
  const guard = caught.indexOf('if (epoch !== viewEpoch)');
  assert.notEqual(guard, -1);
  assert.equal(guard < caught.indexOf('appendChild'), true);
  const refreshAt = source.indexOf("refreshBtn.addEventListener('click'");
  const refreshEnd = source.indexOf('statusRetryBtn.addEventListener', refreshAt);
  const refresh = source.slice(refreshAt, refreshEnd);
  assert.equal(refresh.includes('fetchDatasets()'), true);
  assert.equal(refresh.includes('fetchFacets()'), true);
  assert.equal(refresh.includes('markFacetCandidatesFailed(false)'), true);
  assert.equal(refresh.includes('markFacetCandidatesFailed(true)'), false);
  const changeAt = source.indexOf("accountSelectEl.addEventListener('change'");
  const changeEnd = source.indexOf('[kindSelectEl, analysisSetSelectEl', changeAt);
  const change = source.slice(changeAt, changeEnd);
  assert.equal(change.includes('markFacetCandidatesFailed(true)'), true);
  const accounts = asyncFunctionSource('fetchAccountsAndInitialList');
  const facetCall = accounts.indexOf('await fetchFacets()');
  const facetCatch = accounts.indexOf('markFacetCandidatesFailed(true)');
  const accountCatch = accounts.lastIndexOf('catch (err) {');
  assert.notEqual(facetCall, -1);
  assert.notEqual(facetCatch, -1);
  assert.equal(facetCall < facetCatch, true);
  assert.equal(facetCatch < accountCatch, true);
  const facetHandler = accounts.slice(facetCall, facetCatch);
  assert.equal(facetHandler.includes("currentAccount = ''"), false);
  const markerStart = source.indexOf('function markFacetCandidatesFailed(');
  assert.notEqual(markerStart, -1);
  const marker = source.slice(markerStart, source.indexOf('function fillSelect(', markerStart));
  assert.equal(marker.includes("announceStatus('絞り込み候補の取得でエラーが発生しました。')"), true);
  assert.equal(marker.includes('fillSelect(analysisSetSelectEl, [], formatGenre'), true);
  const records = asyncFunctionSource('fetchRecords');
  assert.equal(
    records.includes("facetCandidatesFailed ? '絞り込み候補の取得でエラーが発生しました。' + listMessage : listMessage"),
    true
  );
  const timerAt = source.indexOf('statusHistoryTimer = setInterval');
  const timer = source.slice(timerAt, source.indexOf('}, 60000);', timerAt));
  assert.equal(timer.includes('paintHistoryPaths(false)'), true);
  assert.equal(timer.includes('fetchDatasets'), false);
});

function createMockAppHarness(options = {}) {
  const elementsById = new Map();

  function createMockElement(id, tagName = 'div') {
    const children = [];
    const classListSet = new Set();
    const listeners = {};
    const el = {
      id: id || '',
      tagName: tagName.toUpperCase(),
      value: '',
      disabled: false,
      _textContent: '',
      get textContent() { return this._textContent; },
      set textContent(v) {
        this._textContent = String(v);
        if (v === '') children.length = 0;
      },
      children: children,
      options: children,
      classList: {
        add: (cls) => classListSet.add(cls),
        remove: (cls) => classListSet.delete(cls),
        contains: (cls) => classListSet.has(cls),
        toggle: (cls, force) => {
          if (force === undefined) {
            if (classListSet.has(cls)) classListSet.delete(cls); else classListSet.add(cls);
          } else if (force) classListSet.add(cls);
          else classListSet.delete(cls);
        },
      },
      appendChild: (child) => {
        children.push(child);
        return child;
      },
      removeChild: (child) => {
        const idx = children.indexOf(child);
        if (idx >= 0) children.splice(idx, 1);
        return child;
      },
      addEventListener: (type, fn) => {
        listeners[type] = listeners[type] || [];
        listeners[type].push(fn);
      },
      dispatchEvent: (event) => {
        const fns = listeners[event.type] || [];
        for (const fn of fns) fn(event);
      },
      setAttribute: () => {},
      removeAttribute: () => {},
      getAttribute: () => null,
    };
    return el;
  }

  function getOrCreateElement(id) {
    if (!elementsById.has(id)) {
      elementsById.set(id, createMockElement(id));
    }
    return elementsById.get(id);
  }

  const fetchCalls = [];
  const announcements = [];

  const fetchMock = async (url, fetchOpts) => {
    const urlStr = String(url);
    fetchCalls.push({ url: urlStr, options: fetchOpts });

    if (urlStr.includes('/api/datasets')) {
      return { ok: true, status: 200, json: async () => ({ items: ['unified'] }) };
    }
    if (urlStr.includes('/api/status')) {
      return { ok: true, status: 200, json: async () => ({}) };
    }
    if (urlStr.includes('/api/accounts')) {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          items: [
            { account: 'acc-alpha', label: 'プレイヤーA' },
            { account: 'acc-beta', label: 'プレイヤーB' },
          ],
        }),
      };
    }
    if (urlStr.includes('/api/record-facets')) {
      if (options.facetFailureMode === 'reject') {
        throw new Error('facets network rejection');
      }
      return { ok: false, status: 500, json: async () => ({ error: 'internal error' }) };
    }
    if (urlStr.includes('/api/records')) {
      return { ok: true, status: 200, json: async () => ({ items: [], total: 0 }) };
    }
    if (urlStr.includes('/api/tag-summary')) {
      return { ok: true, status: 200, json: async () => ({ tags: [] }) };
    }
    if (urlStr.includes('/api/rule-results')) {
      return { ok: true, status: 200, json: async () => ({ results: [] }) };
    }
    if (urlStr.includes('/api/rate-summary')) {
      return { ok: true, status: 200, json: async () => ({ summaries: [] }) };
    }
    return { ok: true, status: 200, json: async () => ({}) };
  };

  const statusLiveEl = getOrCreateElement('status-live');
  const origSet = Object.getOwnPropertyDescriptor(statusLiveEl, 'textContent').set;
  Object.defineProperty(statusLiveEl, 'textContent', {
    get: function () { return this._textContent; },
    set: function (v) {
      announcements.push(String(v));
      origSet.call(this, v);
    },
    configurable: true,
  });

  const sandbox = {
    fetch: fetchMock,
    AbortController: globalThis.AbortController,
    URLSearchParams: globalThis.URLSearchParams,
    Date: Date,
    Math: Math,
    Number: Number,
    String: String,
    Array: Array,
    Object: Object,
    setInterval: () => 1,
    clearInterval: () => {},
    setTimeout: setTimeout,
    clearTimeout: clearTimeout,
    console: console,
    window: {
      location: { hash: '#token=mock-token', pathname: '/' },
      history: { replaceState: () => {} },
      atob: (s) => Buffer.from(s, 'base64').toString('binary'),
    },
    document: {
      readyState: 'loading',
      getElementById: (id) => getOrCreateElement(id),
      createElement: (tag) => createMockElement(null, tag),
      createElementNS: (ns, tag) => createMockElement(null, tag),
      body: createMockElement('body'),
      addEventListener: (type, fn) => {
        if (type === 'DOMContentLoaded') {
          sandbox.__domLoadedHandler = fn;
        }
      },
    },
  };
  sandbox.window.document = sandbox.document;
  sandbox.globalThis = sandbox;

  const hook = '\nglobalThis.__appInternals = {\n' +
    '  fetchAccountsAndInitialList,\n' +
    '  get currentAccount() { return currentAccount; },\n' +
    '  get facetCandidatesFailed() { return facetCandidatesFailed; },\n' +
    '  markFacetCandidatesFailed,\n' +
    '  fetchFacets,\n' +
    '  fetchRecords,\n' +
    '  fetchTagSummary,\n' +
    '  fetchRuleResults,\n' +
    '  fetchRateSummary,\n' +
    '  init,\n' +
    '};\n})();';

  const hooked = source.replace(/\n\}\)\(\);\s*$/, hook);
  vm.createContext(sandbox);
  vm.runInContext(hooked, sandbox);

  return { sandbox, elementsById, fetchCalls, announcements };
}

test('fetchAccountsAndInitialList preserves currentAccount and advances to records and summaries when fetchFacets responds 500', async () => {
  const harness = createMockAppHarness({ facetFailureMode: 'status500' });
  await harness.sandbox.__domLoadedHandler();
  harness.fetchCalls.length = 0;
  harness.announcements.length = 0;

  await harness.sandbox.__appInternals.fetchAccountsAndInitialList();

  // 1. currentAccount and account-select are preserved
  assert.equal(harness.sandbox.__appInternals.currentAccount, 'acc-alpha');
  const accountSelect = harness.elementsById.get('account-select');
  assert.equal(accountSelect.value, 'acc-alpha');
  assert.equal(accountSelect.disabled, false);
  assert.equal(accountSelect.options.length, 2);
  assert.equal(accountSelect.options[0].value, 'acc-alpha');
  assert.equal(accountSelect.options[0].textContent, 'プレイヤーA');

  // 2. markFacetCandidatesFailed(true) set failure flag and announced facet error
  assert.equal(harness.sandbox.__appInternals.facetCandidatesFailed, true);
  assert.equal(harness.announcements.includes('絞り込み候補の取得でエラーが発生しました。'), true);

  // 3. Facet selects are reset to empty / すべて by markFacetCandidatesFailed(true)
  const analysisSet = harness.elementsById.get('analysis-set-select');
  assert.equal(analysisSet.value, '');
  assert.equal(analysisSet.options.length, 1);
  assert.equal(analysisSet.options[0].textContent, 'すべて');

  const ruleSelect = harness.elementsById.get('rule-select');
  assert.equal(ruleSelect.value, '');
  assert.equal(ruleSelect.options.length, 1);
  assert.equal(ruleSelect.options[0].textContent, 'すべて');

  const weaponSelect = harness.elementsById.get('weapon-select');
  assert.equal(weaponSelect.value, '');
  assert.equal(weaponSelect.options.length, 1);
  assert.equal(weaponSelect.options[0].textContent, 'すべて');

  const tagFilterSelect = harness.elementsById.get('tag-filter-select');
  assert.equal(tagFilterSelect.value, '');
  assert.equal(tagFilterSelect.options.length, 1);
  assert.equal(tagFilterSelect.options[0].textContent, 'すべて');

  // 4. Account fetch error banner remains hidden
  const recordsError = harness.elementsById.get('records-error');
  assert.equal(recordsError.classList.contains('hidden'), true);

  // 5. Subsequent queries are executed in order with currentAccount
  const endpoints = harness.fetchCalls.map((c) => new URL('http://dummy' + c.url).pathname);
  assert.deepEqual(endpoints, [
    '/api/accounts',
    '/api/record-facets',
    '/api/records',
    '/api/tag-summary',
    '/api/rule-results',
    '/api/rate-summary',
  ]);

  const recordsCall = harness.fetchCalls[2];
  const recordsUrl = new URL('http://dummy' + recordsCall.url);
  assert.equal(recordsUrl.searchParams.get('account'), 'acc-alpha');
  assert.equal(recordsUrl.searchParams.get('offset'), '0');
  assert.equal(recordsUrl.searchParams.get('limit'), '50');

  for (let i = 3; i < 6; i += 1) {
    const summaryUrl = new URL('http://dummy' + harness.fetchCalls[i].url);
    assert.equal(summaryUrl.searchParams.get('account'), 'acc-alpha');
  }
});

test('fetchAccountsAndInitialList preserves currentAccount and advances to records and summaries when fetchFacets rejects', async () => {
  const harness = createMockAppHarness({ facetFailureMode: 'reject' });
  await harness.sandbox.__domLoadedHandler();
  harness.fetchCalls.length = 0;
  harness.announcements.length = 0;

  await harness.sandbox.__appInternals.fetchAccountsAndInitialList();

  assert.equal(harness.sandbox.__appInternals.currentAccount, 'acc-alpha');
  const accountSelect = harness.elementsById.get('account-select');
  assert.equal(accountSelect.value, 'acc-alpha');
  assert.equal(accountSelect.disabled, false);
  assert.equal(accountSelect.options.length, 2);

  assert.equal(harness.sandbox.__appInternals.facetCandidatesFailed, true);
  assert.equal(harness.announcements.includes('絞り込み候補の取得でエラーが発生しました。'), true);

  const recordsError = harness.elementsById.get('records-error');
  assert.equal(recordsError.classList.contains('hidden'), true);

  const endpoints = harness.fetchCalls.map((c) => new URL('http://dummy' + c.url).pathname);
  assert.deepEqual(endpoints, [
    '/api/accounts',
    '/api/record-facets',
    '/api/records',
    '/api/tag-summary',
    '/api/rule-results',
    '/api/rate-summary',
  ]);
});
