#!/usr/bin/env node
// ============================================================
//  週次データ取得スクリプト（サーバー側・正本データ生成）
//  data/earthquakes.json を読み込み → P2PQuake API取得 →
//  重複排除・イベント統合マージ → 上限剪定 → 保存
//
//  実行: node scripts/fetch-data.mjs
//  環境変数:
//    FETCH_LIMIT  取得件数（既定 300。100件単位でページ分割取得）
//    MAX_STORED   蓄積上限（既定 5000。超過分は古い順に剪定）
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

const CONFIG = {
  apiBase:      process.env.API_BASE || 'https://api.p2pquake.net/v2/history',
  apiCodes:     551,                              // 551 = 地震情報
  apiPageSize:  100,                              // P2PQuake APIの1回の最大件数
  fetchLimit:   parseInt(process.env.FETCH_LIMIT || '300', 10),
  maxStored:    parseInt(process.env.MAX_STORED || '5000', 10),
  fetchTimeoutMs: 20000,
  fetchRetries: 3,                                // リトライ回数（指数バックオフ）
  reqGapMs:     1200,                             // ページ間の間隔（60req/分のレート制限に余裕）
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

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
  console.log(`既存蓄積: ${existing.length}件`);

  // ページ分割取得（offset で遡る）
  const pages = Math.ceil(CONFIG.fetchLimit / CONFIG.apiPageSize);
  const fetched = [];
  let failed = 0;
  for (let i = 0; i < pages; i++) {
    const offset = i * CONFIG.apiPageSize;
    const limit = Math.min(CONFIG.apiPageSize, CONFIG.fetchLimit - offset);
    const url = `${CONFIG.apiBase}?codes=${CONFIG.apiCodes}&limit=${limit}&offset=${offset}`;
    console.log(`取得中: limit=${limit} offset=${offset} ...`);
    try {
      const page = await fetchJsonWithRetry(url);
      fetched.push(...page.filter(x => x && typeof x === 'object'));
      console.log(`  → ${page.length}件`);
    } catch (e) {
      failed++;
      console.error(`  ✖ ページ取得失敗 (offset=${offset}): ${e.message}`);
    }
    if (i < pages - 1) await sleep(CONFIG.reqGapMs);
  }

  if (!fetched.length) {
    console.error('取得できたデータが0件です。既存ファイルは変更しません。');
    process.exitCode = 1;
    return;
  }
  if (failed) console.warn(`一部ページ取得失敗（${failed}/${pages}）。取得できた分のみ反映します。`);

  const merged = mergeData(existing, fetched);
  const added = merged.length - existing.length;

  await mkdir(dirname(DATA_PATH), { recursive: true });
  // 改行区切りに近い安定した整形（差分を読みやすく）。末尾に改行。
  await writeFile(DATA_PATH, JSON.stringify(merged, null, 0) + '\n', 'utf8');

  console.log('--- 結果 ---');
  console.log(`取得: ${fetched.length}件 / 新規追加: ${added}件 / 蓄積合計: ${merged.length}件`);
  console.log('保存完了:', DATA_PATH);
}

main().catch(e => {
  console.error('致命的エラー:', e);
  process.exitCode = 1;
});
