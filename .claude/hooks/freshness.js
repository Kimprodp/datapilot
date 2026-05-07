#!/usr/bin/env node
/**
 * freshness.js — Stop Hook
 *
 * AI 응답 종료 시 이번 세션에서 수정된 `docs/*.md` 및 `CLAUDE.md` 의
 * `Last Updated` 라인을 오늘 날짜로 자동 갱신한다.
 *
 * 동작:
 *   1. stdin 으로 받은 Stop payload 의 transcript_path 에서 이번 세션의
 *      Edit / Write / MultiEdit 도구 호출 대상 파일들을 수집.
 *   2. 대상 파일이 `docs/**.md` 또는 `CLAUDE.md` 인 경우만 필터.
 *   3. 각 파일의 `**Last Updated**: YYYY-MM-DD` 혹은 `Last Updated: YYYY-MM-DD`
 *      라인을 오늘 날짜로 치환 (이미 오늘이면 무시).
 *
 * 설계 결정:
 *   - git 기반 감지 대신 transcript 기반 — "이번 세션에서 AI 가 건드린 파일만"
 *     갱신하기 위함. 사용자 수동 편집분은 제외.
 *   - 외부 의존성 無. Node.js 표준 라이브러리만 사용.
 *   - 실패해도 조용히 종료 (Stop 이벤트가 blocking 이므로 리스크 최소화).
 */

const fs = require("fs");
const path = require("path");

// ─────────────────────────────────────────────────────────
// 1. Payload 파싱
// ─────────────────────────────────────────────────────────
let payload = {};
try {
  const raw = fs.readFileSync(0, "utf-8");
  payload = JSON.parse(raw || "{}");
} catch {
  process.exit(0);
}

const cwd = payload.cwd || process.cwd();
const transcriptPath = payload.transcript_path;

// ─────────────────────────────────────────────────────────
// 2. 이번 세션에서 AI 가 수정한 파일 수집 (transcript 파싱)
// ─────────────────────────────────────────────────────────
const EDIT_TOOLS = new Set(["Edit", "Write", "MultiEdit", "NotebookEdit"]);

function collectModifiedFiles() {
  const files = new Set();
  if (!transcriptPath || !fs.existsSync(transcriptPath)) return files;

  let content;
  try {
    content = fs.readFileSync(transcriptPath, "utf-8");
  } catch {
    return files;
  }

  for (const line of content.split("\n")) {
    if (!line.trim()) continue;
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      continue;
    }

    const contents = msg?.message?.content;
    if (!Array.isArray(contents)) continue;

    for (const item of contents) {
      if (item?.type !== "tool_use") continue;
      if (!EDIT_TOOLS.has(item.name)) continue;
      const fp = item.input?.file_path;
      if (typeof fp === "string" && fp.length > 0) files.add(fp);
    }
  }

  return files;
}

// ─────────────────────────────────────────────────────────
// 3. 갱신 대상 여부 판별
// ─────────────────────────────────────────────────────────
function isTargetFile(absPath) {
  const rel = path.relative(cwd, absPath).replace(/\\/g, "/");
  if (rel.startsWith("..")) return false;
  if (rel === "CLAUDE.md") return true;
  if (rel.startsWith(".claude/templates/")) return false;
  if (rel.startsWith("docs/") && rel.endsWith(".md")) return true;
  return false;
}

// ─────────────────────────────────────────────────────────
// 4. Last Updated 라인 치환
// ─────────────────────────────────────────────────────────
const LAST_UPDATED_REGEX =
  /(\*\*)?Last Updated(\*\*)?:\s*\d{4}-\d{2}-\d{2}/g;

function updateLastUpdated(absPath, today) {
  if (!fs.existsSync(absPath)) return false;

  let content;
  try {
    content = fs.readFileSync(absPath, "utf-8");
  } catch {
    return false;
  }

  if (!LAST_UPDATED_REGEX.test(content)) return false;
  LAST_UPDATED_REGEX.lastIndex = 0;

  const updated = content.replace(LAST_UPDATED_REGEX, (match) =>
    match.replace(/\d{4}-\d{2}-\d{2}/, today)
  );

  if (updated === content) return false;

  try {
    fs.writeFileSync(absPath, updated, "utf-8");
    return true;
  } catch {
    return false;
  }
}

// ─────────────────────────────────────────────────────────
// 5. Main
// ─────────────────────────────────────────────────────────
const today = new Date().toISOString().slice(0, 10);
const modified = collectModifiedFiles();
const updated = [];

for (const fp of modified) {
  const abs = path.isAbsolute(fp) ? fp : path.resolve(cwd, fp);
  if (!isTargetFile(abs)) continue;
  if (updateLastUpdated(abs, today)) {
    updated.push(path.relative(cwd, abs).replace(/\\/g, "/"));
  }
}

if (updated.length > 0) {
  console.log(`Last Updated 자동 갱신: ${updated.join(", ")} -> ${today}`);
}

process.exit(0);
