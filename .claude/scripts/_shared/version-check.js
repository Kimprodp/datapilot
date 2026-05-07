#!/usr/bin/env node
/**
 * version-check.js — npm 패키지 업데이트 가능 여부 확인 (24h 캐시)
 *
 * .claude/package.json 에 등록된 패키지 대상.
 * npm outdated 실행 결과를 .claude/state/version-check-cache.json 에 24h 캐시.
 *
 * 사용:
 *   const { checkVersion } = require("../_shared/version-check");
 *   const { current, latest, update_available } = checkVersion("playwright");
 *   if (update_available) log(`${current} -> ${latest} 업데이트 가능`);
 */

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const CLAUDE_DIR = path.resolve(__dirname, "../..");
const CACHE_FILE = path.join(CLAUDE_DIR, "state", "version-check-cache.json");
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

function readCache() {
  try {
    return JSON.parse(fs.readFileSync(CACHE_FILE, "utf-8"));
  } catch {
    return {};
  }
}

function writeCache(data) {
  const dir = path.dirname(CACHE_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(CACHE_FILE, JSON.stringify(data, null, 2), "utf-8");
}

function isFresh(entry) {
  if (!entry || !entry.checked_at) return false;
  const age = Date.now() - new Date(entry.checked_at).getTime();
  return age < CACHE_TTL_MS;
}

function readInstalledVersion(packageName) {
  try {
    const pkgPath = path.join(
      CLAUDE_DIR,
      "node_modules",
      packageName,
      "package.json"
    );
    return JSON.parse(fs.readFileSync(pkgPath, "utf-8")).version;
  } catch {
    return null;
  }
}

function checkVersion(packageName) {
  const cache = readCache();
  if (isFresh(cache[packageName])) return cache[packageName];

  let current = readInstalledVersion(packageName);
  let latest = null;

  // npm outdated 는 아웃데이트 있을 때 exit 1 → stdout 을 err 에서 꺼냄
  let out;
  try {
    out = execSync(`npm outdated ${packageName} --json`, {
      cwd: CLAUDE_DIR,
      stdio: ["pipe", "pipe", "pipe"],
    }).toString();
  } catch (err) {
    out = err.stdout ? err.stdout.toString() : "{}";
  }

  try {
    const info = JSON.parse(out || "{}");
    if (info[packageName]) {
      current = info[packageName].current || current;
      latest = info[packageName].latest || null;
    }
  } catch {
    /* ignore */
  }

  const result = {
    current,
    latest,
    update_available: Boolean(current && latest && current !== latest),
    checked_at: new Date().toISOString(),
  };
  cache[packageName] = result;
  writeCache(cache);
  return result;
}

module.exports = { checkVersion };
