#!/usr/bin/env node
/**
 * post-edit.example.js — PostToolUse Hook (예시)
 *
 * Claude Code 가 Edit / Write / NotebookEdit 도구 실행 후 이 스크립트를 호출.
 * 이 파일은 **예시** 이며 기본 등록되지 않는다. 사용하려면:
 *
 *   1. 이 파일을 post-edit.js 로 복사
 *   2. .claude/settings.json 의 PostToolUse 섹션 활성화
 *   3. 아래 RUN_COMMAND 영역을 프로젝트에 맞게 수정
 *
 * 입력: stdin 으로 JSON payload
 *   {
 *     session_id: string,
 *     transcript_path: string,
 *     cwd: string,
 *     tool_name: "Edit" | "Write" | "NotebookEdit",
 *     tool_input: { file_path: string, ... },
 *     tool_response: { ... }
 *   }
 *
 * 출력:
 *   - stdout 텍스트 → 경고/안내 (AI 컨텍스트 주입 가능)
 *   - 종료 코드 0 → 정상
 *   - 종료 코드 1+ → 에러 (Claude 에게 경고 표시)
 */

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

// ─────────────────────────────────────────────────────────
// 1. 입력 파싱
// ─────────────────────────────────────────────────────────
let payload = {};
try {
  const raw = fs.readFileSync(0, "utf-8");
  payload = JSON.parse(raw || "{}");
} catch (err) {
  // payload 없어도 조용히 종료 (Hook 이 항상 payload 를 주는 건 아님)
  process.exit(0);
}

const toolName = payload.tool_name;
const filePath = payload.tool_input?.file_path;

if (!filePath) process.exit(0);
if (!["Edit", "Write", "NotebookEdit"].includes(toolName)) process.exit(0);

// ─────────────────────────────────────────────────────────
// 2. 민감 파일 경고 (범용)
// ─────────────────────────────────────────────────────────
const SENSITIVE_PATTERNS = [
  /\.env(\..+)?$/,
  /credentials?\.json$/,
  /secret/i,
  /private[._-]?key/i,
];

if (SENSITIVE_PATTERNS.some((p) => p.test(filePath))) {
  console.log(`⚠️  민감 파일 수정 감지: ${filePath}`);
  console.log(`   .gitignore 확인 / 커밋 전 재검토 권장`);
}

// ─────────────────────────────────────────────────────────
// 3. 린터 / 포맷터 실행 (프로젝트별로 교체)
// ─────────────────────────────────────────────────────────
// 예시들 — 프로젝트에 맞는 것만 남기고 수정:

// --- TypeScript / JavaScript (prettier) ---
// if (/\.(ts|tsx|js|jsx|json|md)$/.test(filePath)) {
//   try {
//     execSync(`npx prettier --write "${filePath}"`, { stdio: "pipe" });
//   } catch (err) {
//     console.error(`prettier 실패: ${err.message}`);
//   }
// }

// --- Python (black) ---
// if (/\.py$/.test(filePath)) {
//   try {
//     execSync(`black "${filePath}"`, { stdio: "pipe" });
//   } catch (err) {
//     console.error(`black 실패: ${err.message}`);
//   }
// }

// --- Flutter / Dart ---
// if (/\.dart$/.test(filePath)) {
//   try {
//     execSync(`dart format "${filePath}"`, { stdio: "pipe" });
//   } catch (err) {
//     console.error(`dart format 실패: ${err.message}`);
//   }
// }

// --- Kotlin / Java (spotless, ktlint 등) ---
// 프로젝트 빌드 도구에 맞춰 추가

process.exit(0);
