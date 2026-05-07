#!/usr/bin/env node
/**
 * mockup-gen.js — 타겟 목업 생성
 *
 * 디자인 이슈에 대한 "이렇게 바꾸면 이상적" 목업을 HTML 스니펫 → 렌더링 → 스크린샷 저장.
 *
 * 입력: --html <file> 또는 --html-inline "<html...>"
 * 출력: mockups/<ts>-<name>.png + 원본 vs 목업 비교 HTML
 *
 * AI 가 HTML/CSS 를 작성해 임시 파일에 쓰고, 이 스크립트가 렌더/캡처만 담당.
 */

const fs = require("fs");
const path = require("path");
const { ensurePlaywright } = require("../_shared/ensure-playwright");

const DR_DIR = path.resolve(__dirname);
const MOCKUPS_DIR = path.join(DR_DIR, "mockups");
const CLAUDE_DIR = path.resolve(DR_DIR, "../..");
const PROJECT_ROOT = path.resolve(CLAUDE_DIR, "..");

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.html && !args.htmlInline) {
    console.error(
      "사용: node mockup-gen.js (--html <file> 또는 --html-inline \"<html>\") [--name <name>] [--viewport=1440x900] [--before <orig-screenshot>]"
    );
    process.exit(2);
  }

  ensurePlaywright();

  let htmlContent;
  if (args.html) {
    htmlContent = fs.readFileSync(
      path.isAbsolute(args.html) ? args.html : path.resolve(process.cwd(), args.html),
      "utf-8"
    );
  } else {
    htmlContent = args.htmlInline;
  }

  const { chromium } = require(path.join(CLAUDE_DIR, "node_modules/playwright"));
  const browser = await chromium.launch({ headless: true });

  const [vw, vh] = parseViewport(args.viewport || "1440x900");
  const context = await browser.newContext({
    viewport: { width: vw, height: vh },
  });
  const page = await context.newPage();

  if (!fs.existsSync(MOCKUPS_DIR)) fs.mkdirSync(MOCKUPS_DIR, { recursive: true });

  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const name = args.name || "mockup";

  // 1. 목업 HTML 렌더링 + 캡처
  await page.setContent(htmlContent, { waitUntil: "networkidle" });
  const mockupPath = path.join(MOCKUPS_DIR, `${ts}-${name}.png`);
  await page.screenshot({ path: mockupPath, fullPage: true });

  // 2. 원본 vs 목업 비교 HTML (선택)
  let compareHtmlPath = null;
  if (args.before) {
    const beforeAbs = path.isAbsolute(args.before)
      ? args.before
      : path.resolve(PROJECT_ROOT, args.before);
    const beforeRel = path
      .relative(MOCKUPS_DIR, beforeAbs)
      .replace(/\\/g, "/");
    const afterRel = path.basename(mockupPath);
    const compareHtml = `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Compare — ${name}</title>
<style>
body{margin:0;font-family:system-ui,sans-serif;background:#111;color:#eee}
h1{text-align:center;padding:16px;margin:0}
.row{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#333}
.col{background:#000;padding:8px}
.col h2{font-size:14px;margin:0 0 8px 0;color:#aaa}
.col img{width:100%;display:block}
</style></head>
<body>
<h1>Before / After — ${name}</h1>
<div class="row">
  <div class="col"><h2>원본</h2><img src="${beforeRel}" alt="before"></div>
  <div class="col"><h2>목업 (제안)</h2><img src="${afterRel}" alt="after"></div>
</div></body></html>`;
    compareHtmlPath = path.join(MOCKUPS_DIR, `${ts}-${name}-compare.html`);
    fs.writeFileSync(compareHtmlPath, compareHtml, "utf-8");
  }

  await browser.close();

  console.log(JSON.stringify({
    name,
    timestamp: ts,
    mockup: relFromProject(mockupPath),
    compare: compareHtmlPath ? relFromProject(compareHtmlPath) : null,
    viewport: `${vw}x${vh}`,
  }, null, 2));
}

function parseViewport(spec) {
  const m = spec.match(/(\d+)\s*x\s*(\d+)/);
  if (!m) return [1440, 900];
  return [parseInt(m[1], 10), parseInt(m[2], 10)];
}

function relFromProject(absPath) {
  return path.relative(PROJECT_ROOT, absPath).replace(/\\/g, "/");
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--html=")) args.html = a.slice(7);
    else if (a === "--html") args.html = argv[++i];
    else if (a.startsWith("--html-inline=")) args.htmlInline = a.slice(14);
    else if (a === "--html-inline") args.htmlInline = argv[++i];
    else if (a.startsWith("--name=")) args.name = a.slice(7);
    else if (a === "--name") args.name = argv[++i];
    else if (a.startsWith("--viewport=")) args.viewport = a.slice(11);
    else if (a === "--viewport") args.viewport = argv[++i];
    else if (a.startsWith("--before=")) args.before = a.slice(9);
    else if (a === "--before") args.before = argv[++i];
  }
  return args;
}

main().catch((err) => {
  console.error(JSON.stringify({ failed: true, reason: "crash", error: err.message }));
  process.exit(4);
});
