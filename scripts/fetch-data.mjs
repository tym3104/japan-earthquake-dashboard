#!/usr/bin/env node
// ============================================================
//  週次データ取得スクリプト（サーバー側・正本データ生成）
//  data/earthquakes.json を読み込み → P2PQuake API取得 →
//  重複排除・イベント統合マージ → 上限剪定 → 保存
//
//  実行: node scripts/fetch-data.mjs
//  環境変数:
//    SPAN_DAYS   カバーする期間（既定 10日）。cron間隔(7日)より広く取り、
//                取りこぼし（活発な週に古いレコードが offset から押し出される）を防ぐ。
//                取得済みの最古の発生時刻が「今 - SPAN_DAYS」より古くなるまで遡る。
//    MAX_PAGES   1回の最大ページ数（既定 40 = 最大4000件）。レート制限・暴走対策の安全上限。
//    MAX_STORED  蓄積上限（既定 5000。超過分は古い順に剪定）。
//    FETCH_LIMIT （任意）件数固定の旧挙動でこの件数だけ取得（手動バックフィル用）。
//                指定時は SPAN_DAYS より優先。
//    DATA_PATH / API_BASE はテスト用のオーバーライド。
//
//  ※ ダッシュボード(index.html)の mergeData と同一ロジックで整合を保つ。
//  ※ Node 18+ のグローバル fetch を使用（依存パッケージ不要）。
// ============================================================

import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_PATH = process.env.DATA_PATH
  ? resolve(process.env.DATA_PATH)
  : resolve(__dirname, '..', 'data', 'earthquakes.json');

// 正の整数の環境変数を検証付きで読む（非数値・0以下は既定にフォールバックし警告）
function intEnv(name, def) {
  const raw = process.env[name];
  if (raw == null || raw === '') return def;
  const n = parseInt(raw, 10);
  if (!Number.isInteger(n) || n <= 0) {
    console.warn(`環境変数 ${name}="${raw}" は正の整数ではありません。既定 ${def} を使用します。`);
    return def;
  }
  return n;
}

const CONFIG = {
  apiBase:      process.env.API_BASE || 'https://api.p2pquake.net/v2/history',
  apiCodes:     551,                              // 551 = 地震情報
  apiPageSize:  100,                              // P2PQuake APIの1回の最大件数
  spanDays:     intEnv('SPAN_DAYS', 10),          // カバー期間（cron間隔より広く）
  maxPages:     intEnv('MAX_PAGES', 40),          // 安全上限（最大 maxPages*100 件）
  maxStored:    intEnv('MAX_STORED', 5000),       // 蓄積上限
  fetchLimit:   process.env.FETCH_LIMIT ? intEnv('FETCH_LIMIT', 0) : 0, // 任意の件数固定（0=無効）
  fetchTimeoutMs: 20000,
  fetchRetries: 3,                                // リトライ回数（指数バックオフ）
  reqGapMs:     1200,                             // ページ間の間隔（60req/分のレート制限に余裕）
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

// P2PQuakeの発生時刻 "YYYY/MM/DD HH:MM:SS.ss"（JST, タイムゾーン表記なし）→ UTCエポックms
function parseJstMs(t) {
  const m = (t || '').match(/(\d{4})\/(\d{2})\/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})/);
  if (!m) return NaN;
  return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4] - 9, +m[5], +m[6]); // JST(UTC+9) → UTC
}

// 1レコード1行の安定整形（有効なJSON、かつ週次更新の git 差分が行単位で読める）
function serialize(arr) {
  if (!arr.length) return '[]\n';
  return '[\n' + arr.map(r => JSON.stringify(r)).join(',\n') + '\n]\n';
}

// ── マージ + 重複排除 + イベント統合 + 上限剪定（index.html と同一ロジック） ──
function mergeData(existing, incoming) {
  // Step1: id重複除去
  const seenId = new Set(existing.map(d => d.id));
  const combined = [...existing];
  (incoming || []).forEach(d => { if (d && !seenId.has(d.id)) { seenId.add(d.id); combined.push(d); } });

  // Step2: 同一地震の複数レコード（速報→修正）を統合。キー=発生時刻+震源地名、最新idを残す
  const eventMap = new Map();
  combined.forEach(d => {
    const t = d.earthquake?.time || '';
    const n = d.earthquake?.hypocenter?.name || '';
    const key = (t || n) ? (t + '|' + n) : ('id|' + d.id);
    const prev = eventMap.get(key);
    if (!prev || String(d.id) > String(prev.id)) eventMap.set(key, d);
  });

  let merged = [...eventMap.values()];
  merged.sort((a, b) => (b.earthquake?.time || '').localeCompare(a.earthquake?.time || ''));
  if (merged.length > CONFIG.maxStored) merged = merged.slice(0, CONFIG.maxStored);
  return merged;
}

async function fetchJsonWithRetry(url) {
  let lastErr;
  for (let attempt = 0; attempt <= CONFIG.fetchRetries; attempt++) {
    try {
      const res = await fetch(url, {
        signal: AbortSignal.timeout(CONFIG.fetchTimeoutMs),
        headers: { 'Accept': 'application/json' },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (!Array.isArray(json)) throw new Error('想定外のレスポンス形式（配列ではない）');
      return json;
    } catch (e) {
      lastErr = (e.name === 'TimeoutError') ? new Error('タイムアウト') : e;
      if (attempt < CONFIG.fetchRetries) {
        const wait = 1000 * Math.pow(2, attempt);
        console.warn(`  ⚠ 取得失敗 (${lastErr.message}) — ${wait}ms後に再試行 ${attempt + 1}/${CONFIG.fetchRetries}`);
        await sleep(wait);
      }
    }
  }
  throw lastErr;
}

async function loadExisting() {
  try {
    const raw = await readFile(DATA_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      console.warn('既存データが配列形式でないため空から開始します。');
      return [];
    }
    return parsed.filter(d => d && typeof d === 'object' && d.earthquake);
  } catch (e) {
    if (e.code === 'ENOENT') {
      console.log('既存データなし（初回）。空から開始します。');
      return [];
    }
    console.warn(`既存データ読込で問題（${e.message}）。空から開始します。`);
    return [];
  }
}

async function main() {
  console.log('=== 週次地震データ取得 ===');
  console.log(`対象ファイル: ${DATA_PATH}`);

  const existing = await loadExisting();
  const existingIds = new Set(existing.map(d => d.id));
  console.log(`既存蓄積: ${existing.length}件`);

  // 期間ベースのページング（offsetで遡る）。以下のいずれかで打ち切る:
  //  - 取得済みの最古の発生時刻が cutoff より古い（対象期間をカバー済み）
  //  - ページが全件既知（これより古いレコードも蓄積済み = 定常運用）
  //  - フィード末尾（満たない件数）／安全上限 maxPages
  //  ※ FETCH_LIMIT 指定時は従来どおり件数固定（手動バックフィル用）。
  const cutoffMs = Date.now() - CONFIG.spanDays * 86400000;
  const pageCap = CONFIG.fetchLimit
    ? Math.min(Math.ceil(CONFIG.fetchLimit / CONFIG.apiPageSize), CONFIG.maxPages)
    : CONFIG.maxPages;
  if (CONFIG.fetchLimit) console.log(`FETCH_LIMIT=${CONFIG.fetchLimit} 指定 — 件数固定モード`);
  else console.log(`期間モード — 直近 ${CONFIG.spanDays} 日をカバー（cutoff: ${new Date(cutoffMs).toISOString()}）`);

  const fetched = [];
  let failedPage = false, pagesTried = 0, stopReason = 'maxPages';
  for (let page = 0; page < pageCap; page++) {
    pagesTried++;
    const offset = page * CONFIG.apiPageSize;
    const limit = CONFIG.fetchLimit
      ? Math.min(CONFIG.apiPageSize, CONFIG.fetchLimit - offset)
      : CONFIG.apiPageSize;
    const url = `${CONFIG.apiBase}?codes=${CONFIG.apiCodes}&limit=${limit}&offset=${offset}`;
    console.log(`取得中: limit=${limit} offset=${offset} ...`);

    let pageRecs;
    try {
      pageRecs = (await fetchJsonWithRetry(url)).filter(x => x && typeof x === 'object');
    } catch (e) {
      failedPage = true;
      console.error(`  ✖ ページ取得失敗 (offset=${offset}): ${e.message} — これ以上遡らず取得済み分で打ち切ります。`);
      stopReason = 'pageError';
      break;
    }
    fetched.push(...pageRecs);
    console.log(`  → ${pageRecs.length}件`);

    if (CONFIG.fetchLimit) { if (offset + limit >= CONFIG.fetchLimit) { stopReason = 'fetchLimit'; break; } }
    else {
      if (pageRecs.length < CONFIG.apiPageSize) { stopReason = 'feedEnd'; break; }            // フィード末尾
      if (pageRecs.every(r => existingIds.has(r.id))) { stopReason = 'caughtUp'; break; }      // 既知に追いついた
      const times = pageRecs.map(r => parseJstMs(r.earthquake?.time)).filter(Number.isFinite);
      const oldest = times.length ? Math.min(...times) : NaN;
      if (Number.isFinite(oldest) && oldest <= cutoffMs) { stopReason = 'spanCovered'; break; } // 期間カバー済み
    }
    await sleep(CONFIG.reqGapMs);
  }

  if (!fetched.length) {
    // 取得0件 = 全ページ不達（ネットワーク/API障害）。既存ファイルは保持し、失敗として通知する。
    console.error('取得できたデータが0件です（API不達の可能性）。既存ファイルは変更しません。');
    process.exitCode = 1;
    return;
  }
  if (failedPage) console.warn(`一部ページ取得で失敗あり。取得できた分のみ反映します。`);

  const merged = mergeData(existing, fetched);
  // 「新規」は id の集合差で正確に数える（length差だと統合・剪定で負やゼロになりうる）
  const addedCount = merged.filter(d => !existingIds.has(d.id)).length;

  await mkdir(dirname(DATA_PATH), { recursive: true });
  await writeFile(DATA_PATH, serialize(merged), 'utf8');

  console.log('--- 結果 ---');
  console.log(`取得: ${fetched.length}件（${pagesTried}ページ, 停止理由: ${stopReason}） / 新規: ${addedCount}件 / 蓄積合計: ${merged.length}件`);
  console.log('保存完了:', DATA_PATH);
}

main().catch(e => {
  console.error('致命的エラー:', e);
  process.exitCode = 1;
});
