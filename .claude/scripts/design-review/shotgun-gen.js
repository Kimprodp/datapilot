#!/usr/bin/env node
/**
 * shotgun-gen.js — 여러 디자인 시안 동시 렌더링 + 비교 HTML 생성
 *
 * AI 가 여러 방향의 HTML 시안을 JSON 으로 주면, Playwright 로 각각 렌더링 → 스크린샷 →
 * 나란히 비교할 수 있는 grid HTML 생성.
 *
 * 입력 JSON 포맷:
 *   {
 *     "topic": "Hero CTA 개선안",
 *     "viewport": "1440x900",
 *     "variants": [
 *       { "name": "minimal", "html": "<html>..." },
 *       { "name": "bold", "html": "<html>..." },
 *       { "name": "glassmorphism", "html": "<html>..." }
 *     ]
 *   }
 *
 * 사용:
 *   node .claude/scripts/design-review/shotgun-gen.js --input <path.json> [--before <orig-screenshot>]
 */

const fs = require("fs");
const path = require("path");
const { ensurePlaywright } = require("../_shared/ensure-playwright");

const DR_DIR = path.resolve(__dirname);
const SHOTGUN_DIR = path.join(DR_DIR, "shotgun");
const CLAUDE_DIR = path.resolve(DR_DIR, "../..");
const PROJECT_ROOT = path.resolve(CLAUDE_DIR, "..");

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.input) {
    console.error(
      "사용: node shotgun-gen.js --input <variants.json> [--before <orig.png>]"
    );
    process.exit(2);
  }

  ensurePlaywright();

  const inputPath = path.isAbsolute(args.input)
    ? args.input
    : path.resolve(process.cwd(), args.input);
  const data = JSON.parse(fs.readFileSync(inputPath, "utf-8"));

  if (!Array.isArray(data.variants) || data.variants.length < 2) {
    console.error(
      JSON.stringify({ failed: true, reason: "need_2_or_more_variants" })
    );
    process.exit(2);
  }

  const [vw, vh] = parseViewport(data.viewport || "1440x900");
  const topic = (data.topic || "shotgun").replace(/[^a-z0-9가-힣_-]/gi, "_");

  const { chromium } = require(path.join(
    CLAUDE_DIR,
    "node_modules/playwright"
  ));
  const browser = await chromium.launch({ headless: true });

  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(SHOTGUN_DIR, `${ts}-${topic}`);
  fs.mkdirSync(runDir, { recursive: true });

  const captures = [];
  for (const variant of data.variants) {
    const name = (variant.name || `variant-${captures.length + 1}`).replace(
      /[^a-z0-9가-힣_-]/gi,
      "_"
    );
    const context = await browser.newContext({
      viewport: { width: vw, height: vh },
    });
    const page = await context.newPage();
    try {
      await page.setContent(variant.html || "<body>empty</body>", {
        waitUntil: "networkidle",
      });
      const shotPath = path.join(runDir, `${name}.png`);
      await page.screenshot({ path: shotPath, fullPage: true });
      captures.push({ name, path: path.basename(shotPath), ok: true });
    } catch (err) {
      captures.push({ name, ok: false, error: err.message });
    } finally {
      await context.close();
    }
  }

  await browser.close();

  // 비교 HTML 생성
  const compareHtml = buildCompareHtml({
    topic: data.topic || "shotgun",
    viewport: `${vw}x${vh}`,
    captures,
    before: args.before,
  });
  const comparePath = path.join(runDir, "compare.html");
  fs.writeFileSync(comparePath, compareHtml, "utf-8");

  // 원본 스크린샷 있으면 같은 폴더에 복사 (상대 경로 호환)
  let beforeCopiedPath = null;
  if (args.before) {
    const absBefore = path.isAbsolute(args.before)
      ? args.before
      : path.resolve(PROJECT_ROOT, args.before);
    if (fs.existsSync(absBefore)) {
      beforeCopiedPath = path.join(runDir, "_before.png");
      fs.copyFileSync(absBefore, beforeCopiedPath);
    }
  }

  console.log(
    JSON.stringify(
      {
        topic: data.topic,
        timestamp: ts,
        runDir: relFromProject(runDir),
        variants: captures,
        compare: relFromProject(comparePath),
        before: beforeCopiedPath ? relFromProject(beforeCopiedPath) : null,
      },
      null,
      2
    )
  );

  const anyFail = captures.some((c) => !c.ok);
  process.exit(anyFail ? 1 : 0);
}

function buildCompareHtml({ topic, viewport, captures, before }) {
  const panelCount = captures.length + (before ? 1 : 0);
  const cols = Math.min(panelCount, 3);

  const panels = [];
  if (before) {
    panels.push(
      `<div class="col"><h2>원본</h2><img src="_before.png" alt="before"></div>`
    );
  }
  for (const c of captures) {
    if (!c.ok) {
      panels.push(
        `<div class="col"><h2>${escapeHtml(c.name)} (실패)</h2><pre>${escapeHtml(
          c.error || "unknown"
        )}</pre></div>`
      );
    } else {
      panels.push(
        `<div class="col"><h2>${escapeHtml(c.name)}</h2><img src="${escapeHtml(
          c.path
        )}" alt="${escapeHtml(c.name)}"></div>`
      );
    }
  }

  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Shotgun — ${escapeHtml(topic)}</title>
<style>
body { margin:0; font-family: system-ui, sans-serif; background:#111; color:#eee; }
header { padding: 16px 24px; background: #000; border-bottom: 1px solid #222; }
header h1 { margin: 0; font-size: 20px; }
header .meta { font-size: 12px; color: #888; margin-top: 4px; }
.grid { display: grid; grid-template-columns: repeat(${cols}, 1fr); gap: 1px; background: #333; }
.col { background: #0a0a0a; padding: 12px; }
.col h2 { font-size: 14px; margin: 0 0 12px 0; color: #aaa; font-weight: 600; }
.col img { width: 100%; display: block; border: 1px solid #1f1f1f; }
.col pre { background: #1a0000; color: #ff8a8a; padding: 8px; font-size: 12px; overflow: auto; }
</style></head>
<body>
<header>
  <h1>Shotgun — ${escapeHtml(topic)}</h1>
  <div class="meta">viewport: ${escapeHtml(viewport)} · variants: ${captures.length}${before ? " · 원본 포함" : ""}</div>
</header>
<div class="grid">${panels.join("")}</div>
</body></html>`;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[c]));
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
    if (a.startsWith("--input=")) args.input = a.slice(8);
    else if (a === "--input") args.input = argv[++i];
    else if (a.startsWith("--before=")) args.before = a.slice(9);
    else if (a === "--before") args.before = argv[++i];
  }
  return args;
}

main().catch((err) => {
  console.error(
    JSON.stringify({ failed: true, reason: "crash", error: err.message })
  );
  process.exit(4);
});
