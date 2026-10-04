// 配布物に同梱するリポジトリ側の最小ファイルを out/stage/repo に集める。
// Python 側の ROOT = scripts/ の一つ上、という前提を保つため構造は同じにする。
const fs = require('fs');
const path = require('path');

const repo = path.resolve(__dirname, '..', '..');
const dest = path.resolve(__dirname, '..', 'out', 'stage', 'repo');
fs.rmSync(dest, { recursive: true, force: true });

for (const name of fs.readdirSync(path.join(repo, 'scripts'))) {
  // 試作では .py だけを持ち込む（.bak や .sh は不要）。
  if (!name.endsWith('.py')) continue;
  fs.mkdirSync(path.join(dest, 'scripts'), { recursive: true });
  fs.copyFileSync(path.join(repo, 'scripts', name), path.join(dest, 'scripts', name));
}
for (const dir of ['assets/app', 'assets/fonts']) {
  fs.cpSync(path.join(repo, dir), path.join(dest, dir), { recursive: true });
}
// scripts が import する他のモジュール（archive.py と src）も構造ごと持ち込む。
fs.copyFileSync(path.join(repo, 'archive.py'), path.join(dest, 'archive.py'));
fs.cpSync(path.join(repo, 'src'), path.join(dest, 'src'), {
  recursive: true,
  filter: (p) => !p.includes('__pycache__'),
});
console.log('stage:', dest);
