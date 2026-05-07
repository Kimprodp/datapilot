#!/usr/bin/env node
/**
 * detect.js — 기술 부채 탐지 스크립트
 *
 * Git 추적 파일 대상으로 다음 항목을 탐지:
 *   1. 오래된 TODO/FIXME 주석 (90일 이상 방치)
 *   2. 참조 없는 파일 (import/require 에서 아무도 안 쓰는 JS/TS/Dart)
 *   3. 큰 소스 파일 (500 줄 초과)
 *   4. 빈 폴더
 *
 * 출력: stdout 으로 JSON 리포트. 삭제/수정은 하지 않음 (탐지 전용).
 *
 * 사용:
 *   node .claude/scripts/cleanup/detect.js [--stale-days 90] [--large-threshold 500]
 */

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const PROJECT_ROOT = path.resolve(__dirname, "../../..");
const DEFAULT_STALE_DAYS = 90;
const DEFAULT_LARGE_THRESHOLD = 500;

// ─── Utilities ───
function listTrackedFiles() {
  try {
    const out = execSync("git ls-files", {
      cwd: PROJECT_ROOT,
      stdio: ["pipe", "pipe", "pipe"],
    }).toString();
    return out.split("\n").filter((l) => l.trim());
  } catch {
    return [];
  }
}

function readFileSafe(relPath) {
  try {
    return fs.readFileSync(path.join(PROJECT_ROOT, relPath), "utf-8");
  } catch {
    return null;
  }
}

function gitBlameDate(file, lineNum) {
  try {
    const out = execSync(
      `git blame -L ${lineNum},${lineNum} --date=short -- "${file}"`,
      { cwd: PROJECT_ROOT, stdio: ["pipe", "pipe", "pipe"] }
    ).toString();
    const m = out.match(/(\d{4}-\d{2}-\d{2})/);
    return m ? m[1] : null;
  } catch {
    return null;
  }
}

function daysBetween(a, b) {
  return Math.floor((b - a) / (1000 * 60 * 60 * 24));
}

// ─── 1. 오래된 TODO/FIXME ───
function detectStaleTodos(files, staleDays) {
  const results = [];
  const todoRegex = /(TODO|FIXME|HACK|XXX)(?:\([^)]*\))?:?\s*(.*)/i;
  const today = Date.now();

  for (const file of files) {
    const ext = path.extname(file).toLowerCase();
    if (!/\.(js|ts|jsx|tsx|py|rb|go|rs|java|kt|dart|c|cpp|h|hpp|cs|php|swift|md)$/.test(ext))
      continue;

    const content = readFileSafe(file);
    if (!content) continue;

    const lines = content.split("\n");
    lines.forEach((line, idx) => {
      const m = line.match(todoRegex);
      if (!m) return;
      const dateStr = gitBlameDate(file, idx + 1);
      if (!dateStr) return;
      const ageDays = daysBetween(new Date(dateStr).getTime(), today);
      if (ageDays >= staleDays) {
        results.push({
          file,
          line: idx + 1,
          kind: m[1].toUpperCase(),
          text: m[2].trim().slice(0, 80),
          ageDays,
          commitDate: dateStr,
        });
      }
    });
  }
  return results.sort((a, b) => b.ageDays - a.ageDays);
}

// ─── 2. 참조 없는 파일 (JS/TS/Dart 간이 버전) ───
function detectOrphanFiles(files) {
  const candidates = files.filter((f) =>
    /\.(js|ts|jsx|tsx|dart)$/.test(f) &&
    !/\b(test|spec|\.test\.|\.spec\.|_test\.|mock|fixture|index\.|main\.|entry\.)/i.test(f)
  );

  // 간이 import 스캔: 프로젝트 내 모든 파일의 import/require/from 문 텍스트 추출
  const importContents = files
    .filter((f) => /\.(js|ts|jsx|tsx|dart)$/.test(f))
    .map((f) => ({ file: f, content: readFileSafe(f) || "" }))
    .filter((x) => x.content);

  const orphans = [];
  for (const candidate of candidates) {
    const base = path.basename(candidate, path.extname(candidate));
    // 자신은 제외하고 다른 파일에서 이 basename 이 import 경로에 등장하는지 확인
    const referenced = importContents.some(({ file, content }) => {
      if (file === candidate) return false;
      const patterns = [
        new RegExp(`from\\s+['"][^'"]*\\b${base}(?:\\.[a-z]+)?['"]`, "i"),
        new RegExp(`require\\s*\\(\\s*['"][^'"]*\\b${base}(?:\\.[a-z]+)?['"]`, "i"),
        new RegExp(`import\\s+['"][^'"]*\\b${base}(?:\\.[a-z]+)?['"]`, "i"),
      ];
      return patterns.some((p) => p.test(content));
    });
    if (!referenced) orphans.push(candidate);
  }
  return orphans;
}

// ─── 3. 큰 소스 파일 ───
function detectLargeFiles(files, threshold) {
  const results = [];
  for (const file of files) {
    if (!/\.(js|ts|jsx|tsx|py|rb|go|rs|java|kt|dart|c|cpp|cs|php|swift)$/.test(file))
      continue;
    const content = readFileSafe(file);
    if (!content) continue;
    const lines = content.split("\n").length;
    if (lines > threshold) {
      results.push({ file, lines });
    }
  }
  return results.sort((a, b) => b.lines - a.lines);
}

// ─── 4. 빈 폴더 ───
function detectEmptyDirs() {
  const results = [];
  function walk(dir) {
    const abs = path.join(PROJECT_ROOT, dir);
    let entries;
    try {
      entries = fs.readdirSync(abs, { withFileTypes: true });
    } catch {
      return;
    }
    // 제외: .git, node_modules, .claude/node_modules 등
    const filtered = entries.filter(
      (e) => !/^(\.git|node_modules|\.next|dist|build|target|\.gradle|\.idea|\.vscode)$/.test(e.name)
    );
    if (filtered.length === 0 && dir !== "") {
      results.push(dir.replace(/\\/g, "/"));
      return;
    }
    for (const e of filtered) {
      if (e.isDirectory()) {
        const sub = dir ? `${dir}/${e.name}` : e.name;
        walk(sub);
      }
    }
  }
  walk("");
  return results;
}

// ─── Main ───
function main() {
  const args = parseArgs(process.argv.slice(2));
  const staleDays = args.staleDays || DEFAULT_STALE_DAYS;
  const largeThreshold = args.largeThreshold || DEFAULT_LARGE_THRESHOLD;

  const files = listTrackedFiles();
  if (files.length === 0) {
    console.error(JSON.stringify({ failed: true, reason: "no_git_or_no_tracked_files" }));
    process.exit(1);
  }

  const report = {
    scannedFiles: files.length,
    thresholds: { staleDays, largeThreshold },
    staleTodos: detectStaleTodos(files, staleDays),
    orphanFiles: detectOrphanFiles(files),
    largeFiles: detectLargeFiles(files, largeThreshold),
    emptyDirs: detectEmptyDirs(),
    timestamp: new Date().toISOString(),
  };

  console.log(JSON.stringify(report, null, 2));
  process.exit(0);
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--stale-days=")) args.staleDays = parseInt(a.slice(13), 10);
    else if (a === "--stale-days") args.staleDays = parseInt(argv[++i], 10);
    else if (a.startsWith("--large-threshold=")) args.largeThreshold = parseInt(a.slice(18), 10);
    else if (a === "--large-threshold") args.largeThreshold = parseInt(argv[++i], 10);
  }
  return args;
}

try {
  main();
} catch (err) {
  console.error(JSON.stringify({ failed: true, error: err.message }));
  process.exit(1);
}
