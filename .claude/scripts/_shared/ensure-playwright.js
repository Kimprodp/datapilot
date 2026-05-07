#!/usr/bin/env node
/**
 * ensure-playwright.js — Playwright 자동 설치 보장
 *
 * Playwright 기반 커맨드 (/qa, /design-review 등) 의 runner 스크립트 초입에서 호출.
 * 미설치 상태면 npm install + browser 바이너리 설치를 자동 수행.
 *
 * 사용:
 *   const { ensurePlaywright } = require("../_shared/ensure-playwright");
 *   ensurePlaywright();
 *
 * 반환:
 *   { installed: true, freshInstall: bool }
 */

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const CLAUDE_DIR = path.resolve(__dirname, "../..");
const NODE_MODULES = path.join(CLAUDE_DIR, "node_modules");
const STATE_FILE = path.join(CLAUDE_DIR, "state", "tools-installed.json");

function readState() {
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, "utf-8"));
  } catch {
    return {};
  }
}

function writeState(state) {
  const dir = path.dirname(STATE_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

function isPackageInstalled() {
  return fs.existsSync(path.join(NODE_MODULES, "playwright"));
}

function isBrowserInstalled() {
  const homeDir = process.env.HOME || process.env.USERPROFILE || "";
  const candidates = [
    path.join(homeDir, ".cache", "ms-playwright"),
    path.join(homeDir, "AppData", "Local", "ms-playwright"),
    path.join(homeDir, "Library", "Caches", "ms-playwright"),
  ];
  for (const candidate of candidates) {
    try {
      if (fs.existsSync(candidate)) {
        const entries = fs.readdirSync(candidate);
        if (entries.some((e) => e.startsWith("chromium"))) return true;
      }
    } catch {
      /* skip */
    }
  }
  return false;
}

function ensurePlaywright({ log = console.log } = {}) {
  const pkgOk = isPackageInstalled();
  const browserOk = isBrowserInstalled();

  if (pkgOk && browserOk) {
    return { installed: true, freshInstall: false };
  }

  log("Playwright 설치 필요. 자동 설치 시작...");

  if (!pkgOk) {
    log("  -> cd .claude && npm install (Node 패키지)");
    execSync("npm install", { cwd: CLAUDE_DIR, stdio: "inherit" });
  }

  if (!isBrowserInstalled()) {
    log("  -> npx playwright install chromium (~300MB)");
    execSync("npx playwright install chromium", {
      cwd: CLAUDE_DIR,
      stdio: "inherit",
    });
  }

  const state = readState();
  const now = new Date().toISOString();
  state.playwright = { installed_at: now };
  state.chromium_binary = { installed_at: now };
  writeState(state);

  log("Playwright 설치 완료.");
  return { installed: true, freshInstall: true };
}

module.exports = {
  ensurePlaywright,
  isPackageInstalled,
  isBrowserInstalled,
};
