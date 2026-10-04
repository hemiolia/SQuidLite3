// SQuidLite3 デスクトップ窓（試作）。
// Python の記録画面（scripts/desktop_app.py）を子プロセスで起動し、その URL を窓に読み込む。
const { app, BrowserWindow, shell, dialog } = require('electron');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const IS_PACKAGED = app.isPackaged;
let child = null;
let win = null;
let allowedOrigin = null;
let logPath = null;
let quitting = false;

// 開発時はリポジトリ、配布物では resources/repo を使う。
function repoRoot() {
  return IS_PACKAGED
    ? path.join(process.resourcesPath, 'repo')
    : path.resolve(__dirname, '..');
}

// 同梱 Python を優先し、無ければ PATH の python3。
function pythonCommand() {
  const bundled = path.join(process.resourcesPath || '', 'python', 'bin', 'python3');
  if (IS_PACKAGED && fs.existsSync(bundled)) return bundled;
  return 'python3';
}

function log(line) {
  try {
    fs.appendFileSync(logPath, `${new Date().toISOString()} ${line}\n`);
  } catch (_) { /* ログが書けなくても窓は動かす */ }
}

function showFailure(reason) {
  const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const html = `<!doctype html><meta charset="utf-8"><title>SQuidLite3</title>
<body style="font-family:-apple-system,sans-serif;padding:32px;line-height:1.8">
<h2>記録の画面を開けませんでした。</h2><p>${esc(reason)}</p>
<p style="color:#555">詳しい内容は次のファイルに書きました。<br>${esc(logPath)}</p></body>`;
  win.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent(html));
}

function stopChild() {
  if (child && child.exitCode === null && !child.killed) {
    child.kill('SIGTERM');
    // 止まらない場合に備えて強制終了も予約する。
    const c = child;
    setTimeout(() => { if (c.exitCode === null) c.kill('SIGKILL'); }, 2000).unref();
  }
}

function startBackend() {
  return new Promise((resolve, reject) => {
    const script = path.join(repoRoot(), 'scripts', 'desktop_app.py');
    if (!fs.existsSync(script)) return reject(new Error('記録の画面のプログラムが見つかりません。'));
    const py = pythonCommand();
    log(`起動: ${py} ${script} --no-open --port 0`);
    child = spawn(py, [script, '--no-open', '--port', '0'], {
      cwd: repoRoot(),
      env: { ...process.env, PYTHONUNBUFFERED: '1', PYTHONIOENCODING: 'utf-8' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let buf = '';
    let settled = false;
    const done = (fn, v) => { if (!settled) { settled = true; fn(v); } };
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (d) => {
      buf += d;
      const m = buf.match(/http:\/\/127\.0\.0\.1:(\d+)\/#token=[A-Za-z0-9_-]+/);
      if (m) done(resolve, m[0]);
    });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (d) => log('stderr: ' + d.trimEnd()));
    child.on('error', (e) => {
      log('起動失敗: ' + e.message);
      done(reject, new Error(e.code === 'ENOENT' ? 'Python が見つかりません。' : 'プログラムを起動できませんでした。'));
    });
    child.on('exit', (code, sig) => {
      log(`終了: code=${code} signal=${sig}`);
      done(reject, new Error('記録の画面のプログラムが途中で終了しました。'));
      if (!quitting && settled && win && !win.isDestroyed()) showFailure('記録の画面のプログラムが止まりました。');
    });
    setTimeout(() => done(reject, new Error('記録の画面の準備に時間がかかりすぎました。')), 30000);
  });
}

function isAllowed(url) {
  try { return new URL(url).origin === allowedOrigin; } catch (_) { return false; }
}

async function createWindow() {
  win = new BrowserWindow({
    width: 1200, height: 800, title: 'SQuidLite3',
    webPreferences: { contextIsolation: true, nodeIntegration: false, sandbox: true },
  });
  // 当該ポート以外へは遷移させない。外部 URL は https のみ既定ブラウザへ回す。
  win.webContents.on('will-navigate', (e, url) => {
    if (url.startsWith('data:text/html')) return;
    if (!isAllowed(url)) {
      e.preventDefault();
      if (/^https:\/\//.test(url)) shell.openExternal(url);
    }
  });
  win.webContents.setWindowOpenHandler(({ url }) => {
    if (!isAllowed(url) && /^https:\/\//.test(url)) shell.openExternal(url);
    return { action: 'deny' };
  });
  win.on('closed', () => { win = null; });
  win.loadURL('data:text/html;charset=utf-8,' + encodeURIComponent('<meta charset="utf-8"><body style="font-family:sans-serif;padding:32px">準備しています。'));
  try {
    const url = await startBackend();
    allowedOrigin = new URL(url).origin;
    if (win) await win.loadURL(url);
  } catch (e) {
    stopChild();
    if (win) showFailure(e.message);
  }
  // 検証用: 環境変数があれば表示後に窓の画像を保存して終了する。
  if (win && process.env.SQUIDLITE3_PROOF) {
    await new Promise((r) => setTimeout(r, Number(process.env.SQUIDLITE3_PROOF_WAIT || 6000)));
    const img = await win.webContents.capturePage();
    fs.writeFileSync(process.env.SQUIDLITE3_PROOF, img.resize({ width: 1000 }).toJPEG(80));
    app.quit();
  }
}

app.whenReady().then(() => {
  logPath = path.join(app.getPath('logs'), 'squidlite3.log');
  fs.mkdirSync(path.dirname(logPath), { recursive: true });
  createWindow();
});
app.on('window-all-closed', () => { quitting = true; stopChild(); app.quit(); });
app.on('before-quit', () => { quitting = true; stopChild(); });
process.on('exit', () => { if (child && child.exitCode === null) child.kill('SIGKILL'); });
