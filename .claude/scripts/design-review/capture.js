#!/usr/bin/env node
/**
 * capture.js — /design-review 스크린샷 캡처
 *
 * 다중 viewport (desktop/mobile) 로 동일 URL 캡처.
 * 출력: reports/ 디렉토리에 PNG 저장. stdout 으로 JSON 결과.
 *
 * 사용:
 *   node .claude/scripts/design-review/capture.js --url http://localhost:3000 [--name home] [--dark]
 */

const fs = require("fs");
const path = require("path");
const { ensurePlaywright } = require("../_shared/ensure-playwright");
const { checkDevServer } = require("../_shared/check-dev-server");

const DR_DIR = path.resolve(__dirname);
const REPORTS_DIR = path.join(DR_DIR, "reports");
const CLAUDE_DIR = path.resolve(DR_DIR, "../..");
const PROJECT_ROOT = path.resolve(CLAUDE_DIR, "..");

const DEFAULT_VIEWPORTS = [
  { name: "desktop", width: 1440, height: 900 },
  { name: "mobile", width: 375, height: 812 },
];

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.url) {
    console.error("사용: node capture.js --url <url> [--name <name>] [--dark] [--viewports=desktop,mobile]");
    process.exit(2);
  }

  ensurePlaywright();

  const serverCheck = await checkDevServer(new URL(args.url).origin);
  if (!serverCheck.ok) {
    console.error(JSON.stringify({ failed: true, reason: "dev_server_unreachable", url: args.url, error: serverCheck.error }));
    process.exit(3);
  }

  const { chromium } = require(path.join(CLAUDE_DIR, "node_modules/playwright"));
  const browser = await chromium.launch({ headless: true });

  if (!fs.existsSync(REPORTS_DIR)) fs.mkdirSync(REPORTS_DIR, { recursive: true });

  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const name = args.name || "page";
  const viewports = resolveViewports(args.viewports);

  const results = [];

  for (const vp of viewports) {
    const context = await browser.newContext({
      viewport: { width: vp.width, height: vp.height },
      colorScheme: args.dark ? "dark" : "light",
    });
    const page = await context.newPage();
    try {
      await page.goto(args.url, { waitUntil: "networkidle", timeout: 15000 });
      await page.waitForTimeout(500); // 미세 렌더 대기
      const shotPath = path.join(
        REPORTS_DIR,
        `${ts}-${name}-${vp.name}${args.dark ? "-dark" : ""}.png`
      );
      await page.screenshot({ path: shotPath, fullPage: true });
      results.push({
        viewport: vp.name,
        size: `${vp.width}x${vp.height}`,
        dark: Boolean(args.dark),
        path: relFromProject(shotPath),
        ok: true,
      });
    } catch (err) {
      results.push({
        viewport: vp.name,
        size: `${vp.width}x${vp.height}`,
        ok: false,
        error: err.message,
      });
    } finally {
      await context.close();
    }
  }

  await browser.close();

  console.log(JSON.stringify({
    url: args.url,
    name,
    timestamp: ts,
    captures: results,
  }, null, 2));

  const allOk = results.every((r) => r.ok);
  process.exit(allOk ? 0 : 1);
}

function resolveViewports(spec) {
  if (!spec) return DEFAULT_VIEWPORTS;
  const names = spec.split(",").map((s) => s.trim()).filter(Boolean);
  const map = {
    desktop: DEFAULT_VIEWPORTS[0],
    mobile: DEFAULT_VIEWPORTS[1],
    tablet: { name: "tablet", width: 768, height: 1024 },
    widescreen: { name: "widescreen", width: 1920, height: 1080 },
  };
  return names.map((n) => map[n]).filter(Boolean);
}

function relFromProject(absPath) {
  return path.relative(PROJECT_ROOT, absPath).replace(/\\/g, "/");
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dark") args.dark = true;
    else if (a.startsWith("--url=")) args.url = a.slice(6);
    else if (a === "--url") args.url = argv[++i];
    else if (a.startsWith("--name=")) args.name = a.slice(7);
    else if (a === "--name") args.name = argv[++i];
    else if (a.startsWith("--viewports=")) args.viewports = a.slice(12);
    else if (a === "--viewports") args.viewports = argv[++i];
  }
  return args;
}

main().catch((err) => {
  console.error(JSON.stringify({ failed: true, reason: "crash", error: err.message }));
  process.exit(4);
});
