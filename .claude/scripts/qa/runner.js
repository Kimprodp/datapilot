#!/usr/bin/env node
/**
 * runner.js — /qa Playwright 실행 엔진
 *
 * 입력: 시나리오 JSON 파일 경로 (--scenario)
 * 출력: stdout 으로 JSON 리포트 (step 별 결과 + 콘솔/네트워크 로그 + 스크린샷 경로)
 *
 * 사용:
 *   node .claude/scripts/qa/runner.js --scenario=path/to/scenario.json [--headed] [--mode=standard]
 *
 * 시나리오 JSON 포맷:
 *   {
 *     "name": "login-flow",
 *     "mode": "standard",              // quick | standard | exhaustive
 *     "url": "http://localhost:3000",
 *     "viewport": { "width": 1280, "height": 800 },
 *     "steps": [
 *       { "action": "goto", "path": "/login" },
 *       { "action": "fill", "selector": "#email", "value": "test@example.com" },
 *       { "action": "click", "selector": "button[type=submit]" },
 *       { "action": "waitForURL", "pattern": "**\/dashboard" },
 *       { "action": "expect", "selector": "h1", "contains": "Dashboard" }
 *     ]
 *   }
 */

const fs = require("fs");
const path = require("path");
const { ensurePlaywright } = require("../_shared/ensure-playwright");
const { checkDevServer } = require("../_shared/check-dev-server");

const QA_DIR = path.resolve(__dirname);
const REPORTS_DIR = path.join(QA_DIR, "reports");
const CLAUDE_DIR = path.resolve(QA_DIR, "../..");
const PROJECT_ROOT = path.resolve(CLAUDE_DIR, "..");

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.scenario) {
    console.error(
      "사용: node runner.js --scenario <path.json> [--headed] [--mode=standard]"
    );
    process.exit(2);
  }

  ensurePlaywright();

  const scenarioPath = path.isAbsolute(args.scenario)
    ? args.scenario
    : path.resolve(process.cwd(), args.scenario);
  const scenario = JSON.parse(fs.readFileSync(scenarioPath, "utf-8"));

  const rootUrl = new URL(scenario.url).origin;
  const serverCheck = await checkDevServer(rootUrl);
  if (!serverCheck.ok) {
    console.error(
      JSON.stringify({
        failed: true,
        reason: "dev_server_unreachable",
        url: rootUrl,
        error: serverCheck.error,
      })
    );
    process.exit(3);
  }

  const { chromium } = require(path.join(CLAUDE_DIR, "node_modules/playwright"));
  const browser = await chromium.launch({ headless: !args.headed });
  const context = await browser.newContext({
    viewport: scenario.viewport || { width: 1280, height: 800 },
  });
  const page = await context.newPage();

  const consoleLogs = [];
  page.on("console", (msg) => {
    const type = msg.type();
    if (type === "error" || type === "warning") {
      consoleLogs.push({ type, text: msg.text() });
    }
  });

  const networkFailures = [];
  page.on("requestfailed", (req) =>
    networkFailures.push({
      url: req.url(),
      failure: req.failure()?.errorText || "unknown",
    })
  );

  const results = [];
  let failed = false;
  const ts = new Date()
    .toISOString()
    .replace(/[:.]/g, "-")
    .slice(0, 19);
  const scenarioName = scenario.name || "unnamed";

  if (!fs.existsSync(REPORTS_DIR))
    fs.mkdirSync(REPORTS_DIR, { recursive: true });

  for (let i = 0; i < scenario.steps.length; i++) {
    const step = scenario.steps[i];
    const stepNum = i + 1;
    try {
      await executeStep(page, step, scenario.url);
      results.push({ step: stepNum, action: step.action, status: "pass" });
    } catch (err) {
      failed = true;
      const screenshotPath = path.join(
        REPORTS_DIR,
        `${ts}-${scenarioName}-step-${stepNum}-fail.png`
      );
      try {
        await page.screenshot({ path: screenshotPath, fullPage: true });
      } catch {
        /* screenshot optional */
      }
      results.push({
        step: stepNum,
        action: step.action,
        status: "fail",
        error: err.message,
        screenshot: relFromProject(screenshotPath),
      });
      break;
    }
  }

  const finalPath = path.join(
    REPORTS_DIR,
    `${ts}-${scenarioName}-final.png`
  );
  try {
    await page.screenshot({ path: finalPath, fullPage: true });
  } catch {
    /* ignore */
  }

  await browser.close();

  const report = {
    scenario: scenarioName,
    url: scenario.url,
    mode: scenario.mode || "standard",
    failed,
    steps: results,
    consoleLogs,
    networkFailures,
    finalScreenshot: relFromProject(finalPath),
    timestamp: ts,
  };

  console.log(JSON.stringify(report, null, 2));
  process.exit(failed ? 1 : 0);
}

async function executeStep(page, step, baseUrl) {
  const timeout = step.timeout || 5000;
  switch (step.action) {
    case "goto": {
      const dest = step.path
        ? new URL(step.path, baseUrl).toString()
        : step.url;
      await page.goto(dest, {
        waitUntil: step.waitUntil || "networkidle",
        timeout: step.timeout || 10000,
      });
      return;
    }
    case "click":
      await page.click(step.selector, { timeout });
      return;
    case "fill":
      await page.fill(step.selector, step.value, { timeout });
      return;
    case "press":
      await page.keyboard.press(step.key);
      return;
    case "waitForSelector":
      await page.waitForSelector(step.selector, { timeout });
      return;
    case "waitForURL":
      await page.waitForURL(step.pattern, { timeout });
      return;
    case "wait":
      await page.waitForTimeout(step.ms || 500);
      return;
    case "expect": {
      if (step.selector && step.contains !== undefined) {
        const text = (await page.textContent(step.selector, { timeout })) || "";
        if (!text.includes(step.contains)) {
          throw new Error(
            `expect "${step.contains}" in ${step.selector}, got "${text}"`
          );
        }
        return;
      }
      if (step.urlContains) {
        if (!page.url().includes(step.urlContains)) {
          throw new Error(
            `expect URL contains "${step.urlContains}", got "${page.url()}"`
          );
        }
        return;
      }
      throw new Error(`expect step missing selector/contains or urlContains`);
    }
    default:
      throw new Error(`Unknown action: ${step.action}`);
  }
}

function relFromProject(absPath) {
  return path.relative(PROJECT_ROOT, absPath).replace(/\\/g, "/");
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--headed") args.headed = true;
    else if (a.startsWith("--scenario=")) args.scenario = a.slice(11);
    else if (a === "--scenario") args.scenario = argv[++i];
    else if (a.startsWith("--mode=")) args.mode = a.slice(7);
    else if (a === "--mode") args.mode = argv[++i];
  }
  return args;
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      failed: true,
      reason: "runner_crash",
      error: err.message,
      stack: err.stack,
    })
  );
  process.exit(4);
});
