#!/usr/bin/env node
/**
 * check-dev-server.js — 로컬 dev server 응답 확인
 *
 * Playwright 실행 전 대상 URL 이 살아있는지 빠르게 확인.
 * 외부 의존성 無 (Node 내장 http/https 모듈만 사용).
 *
 * 사용:
 *   const { checkDevServer } = require("../_shared/check-dev-server");
 *   const result = await checkDevServer("http://localhost:3000");
 *   if (!result.ok) { ... }
 */

const http = require("http");
const https = require("https");
const { URL } = require("url");

function checkDevServer(url, { timeoutMs = 3000 } = {}) {
  return new Promise((resolve) => {
    let parsed;
    try {
      parsed = new URL(url);
    } catch {
      resolve({ ok: false, error: "invalid URL", url });
      return;
    }

    const lib = parsed.protocol === "https:" ? https : http;
    const req = lib.get(url, { timeout: timeoutMs }, (res) => {
      resolve({
        ok: res.statusCode < 500,
        status: res.statusCode,
        url,
      });
      res.resume();
    });

    req.on("error", (err) => {
      resolve({ ok: false, error: err.code || err.message, url });
    });

    req.on("timeout", () => {
      req.destroy();
      resolve({ ok: false, error: "timeout", url });
    });
  });
}

module.exports = { checkDevServer };
