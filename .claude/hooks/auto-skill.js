#!/usr/bin/env node
/**
 * auto-skill.js — UserPromptSubmit Hook
 *
 * 사용자 프롬프트 제출 직후 실행. 3가지 일을 수행:
 *   1. skill-rules.json 수동 규칙 매칭 → 힌트 주입 (우선권)
 *   2. .claude/skills/<name>/SKILL.md frontmatter keywords 자동 스캔 -> 힌트 주입 (수동 규칙에 없는 스킬만)
 *   3. 보안/cleanup 주기 초과 체크 → 경고 힌트 주입
 *
 * 출력 규약 (Claude Code):
 *   - stdout 텍스트 → AI 컨텍스트에 system 힌트로 삽입
 *   - exit 0 → 성공. 메시지 그대로 전달
 *   - 실패해도 조용히 exit 0 (Hook 이 사용자 플로우 방해 금지)
 *
 * 설계 원칙:
 *   - 힌트는 권고일 뿐. AI 가 필요 시 사용자에게 전달.
 *   - 한 메시지당 최대 3개 힌트 (과다 주입 방지).
 *   - 외부 의존성 無. Node 내장만 사용.
 *   - 수동 규칙 (skill-rules.json) 이 자동 스캔보다 우선.
 */

const fs = require("fs");
const path = require("path");

const CLAUDE_DIR = path.resolve(__dirname, "..");
const RULES_FILE = path.join(CLAUDE_DIR, "skill-rules.json");
const SKILLS_DIR = path.join(CLAUDE_DIR, "skills");
const SECURITY_STATE_FILE = path.join(
  CLAUDE_DIR,
  "state",
  "security-audit.json"
);
const CLEANUP_STATE_FILE = path.join(CLAUDE_DIR, "state", "cleanup.json");
const SETTINGS_FILE = path.join(CLAUDE_DIR, "settings.json");

const MAX_HINTS = 3;
const DEFAULT_SECURITY_INTERVAL_DAYS = 30;
const DEFAULT_CLEANUP_INTERVAL_DAYS = 45;
const DEFAULT_SENSITIVE_KEYWORDS = [
  "인증", "로그인", "토큰", "세션", "비밀번호", "password",
  "결제", "카드", "환불", "PG", "payment",
  "API 키", "시크릿", "secret", "OAuth",
  "권한", "admin", "관리자",
];

function readJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf-8"));
  } catch {
    return fallback;
  }
}

function getPayload() {
  try {
    const raw = fs.readFileSync(0, "utf-8");
    return JSON.parse(raw || "{}");
  } catch {
    return {};
  }
}

function getReminderSettings() {
  const s = readJson(SETTINGS_FILE, {});
  const r = s.reminders || {};
  return {
    securityInterval: r.security_audit_interval_days || DEFAULT_SECURITY_INTERVAL_DAYS,
    cleanupInterval: r.cleanup_interval_days || DEFAULT_CLEANUP_INTERVAL_DAYS,
    keywords: r.security_sensitive_keywords || DEFAULT_SENSITIVE_KEYWORDS,
  };
}

// ─── Skill frontmatter 자동 스캔 ───
function parseFrontmatter(content) {
  const m = content.match(/^---\s*\n([\s\S]*?)\n---/);
  if (!m) return null;
  const block = m[1];
  const result = {};
  const lines = block.split("\n").map((l) => l.replace(/\r$/, ""));
  let currentKey = null;
  for (const line of lines) {
    // 배열 형식: keywords: ["a", "b", "c"]
    const arrayMatch = line.match(/^(\w+):\s*\[(.*)\]\s*$/);
    if (arrayMatch) {
      const key = arrayMatch[1];
      const items = arrayMatch[2]
        .split(",")
        .map((s) => s.trim().replace(/^["']|["']$/g, ""))
        .filter(Boolean);
      result[key] = items;
      currentKey = null;
      continue;
    }
    // 단순 key: value
    const kvMatch = line.match(/^(\w+):\s*(.+)$/);
    if (kvMatch) {
      result[kvMatch[1]] = kvMatch[2].trim();
      currentKey = kvMatch[1];
      continue;
    }
  }
  return result;
}

function scanSkills() {
  const autoRules = [];
  if (!fs.existsSync(SKILLS_DIR)) return autoRules;

  let entries;
  try {
    entries = fs.readdirSync(SKILLS_DIR, { withFileTypes: true });
  } catch {
    return autoRules;
  }

  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const skillFile = path.join(SKILLS_DIR, e.name, "SKILL.md");
    if (!fs.existsSync(skillFile)) continue;

    let content;
    try {
      content = fs.readFileSync(skillFile, "utf-8");
    } catch {
      continue;
    }

    const fm = parseFrontmatter(content);
    if (!fm || !fm.name) continue;

    // keywords 필드 있으면 사용, 없으면 name 자체를 키워드로
    const keywords = Array.isArray(fm.keywords) && fm.keywords.length > 0
      ? fm.keywords
      : [fm.name, fm.name.replace(/-/g, " ")];

    const descSnippet = (fm.description || "").slice(0, 90);
    autoRules.push({
      keywords,
      hint: `Skill \`${fm.name}\` 관련 — ${descSnippet}`,
      source: "auto-scan",
      skillName: fm.name,
    });
  }
  return autoRules;
}

function matchRules(userText, rules) {
  const hints = [];
  const seenHints = new Set();
  const lower = userText.toLowerCase();
  for (const rule of rules.rules || []) {
    if (!Array.isArray(rule.keywords)) continue;
    const matched = rule.keywords.some((kw) =>
      lower.includes(String(kw).toLowerCase())
    );
    if (matched && rule.hint && !seenHints.has(rule.hint)) {
      hints.push(rule.hint);
      seenHints.add(rule.hint);
      if (hints.length >= MAX_HINTS) break;
    }
  }
  return { hints, seen: seenHints };
}

function matchAutoSkills(userText, autoRules, existingHints, seen) {
  const lower = userText.toLowerCase();
  const hints = [...existingHints];
  for (const rule of autoRules) {
    if (hints.length >= MAX_HINTS) break;
    const matched = rule.keywords.some((kw) =>
      lower.includes(String(kw).toLowerCase())
    );
    if (!matched) continue;
    if (seen.has(rule.hint)) continue;
    // 동일 skillName 의 수동 규칙 힌트가 이미 있으면 중복으로 간주
    const dup = [...seen].some((h) => h.includes(rule.skillName));
    if (dup) continue;
    hints.push(rule.hint);
    seen.add(rule.hint);
  }
  return hints;
}

function checkSecurityAuditInterval(userText, settings) {
  const state = readJson(SECURITY_STATE_FILE, null);
  if (!state || !state.last_audit || !state.last_audit.date) return null;

  const lastDate = new Date(state.last_audit.date);
  const daysSince = Math.floor(
    (Date.now() - lastDate.getTime()) / (1000 * 60 * 60 * 24)
  );
  const interval = settings.securityInterval;

  // 메시지에 민감 키워드 있으면 주기 반 경과만 돼도 알림 (조기 경고)
  const lower = userText.toLowerCase();
  const sensitiveInMessage = settings.keywords.some((kw) =>
    lower.includes(String(kw).toLowerCase())
  );

  if (daysSince > interval) {
    const base = `[보안 감사 주기 초과] 마지막 감사 ${daysSince}일 전 (기준 ${interval}일).`;
    const extra = sensitiveInMessage
      ? " 이번 작업이 민감 영역과 관련 — `/security-audit` 강력 권장."
      : " `/security-audit` 실행 권장.";
    return base + extra;
  }

  if (sensitiveInMessage && daysSince > interval / 2) {
    return `[민감 영역 작업] 마지막 보안 감사 ${daysSince}일 전 (주기 ${interval}일 중반). 작업 후 \`/security-audit\` 권장.`;
  }

  return null;
}

function checkCleanupInterval(settings) {
  const state = readJson(CLEANUP_STATE_FILE, null);
  if (!state || !state.last_cleanup || !state.last_cleanup.date) return null;

  const lastDate = new Date(state.last_cleanup.date);
  const daysSince = Math.floor(
    (Date.now() - lastDate.getTime()) / (1000 * 60 * 60 * 24)
  );
  const interval = settings.cleanupInterval;

  if (daysSince > interval * 2) {
    return `[cleanup 주기 2배 초과] 마지막 정리 ${daysSince}일 전 (기준 ${interval}일). \`/cleanup\` 강력 권장.`;
  }
  if (daysSince > interval) {
    return `[cleanup 주기 초과] 마지막 정리 ${daysSince}일 전 (기준 ${interval}일). \`/cleanup\` 권장.`;
  }
  return null;
}

function main() {
  const payload = getPayload();
  const userText = payload.prompt || "";
  if (!userText.trim()) {
    process.exit(0);
  }

  const rules = readJson(RULES_FILE, { rules: [] });
  const settings = getReminderSettings();

  // 1단계: 수동 규칙 (우선)
  const manualResult = matchRules(userText, rules);
  let hints = manualResult.hints;
  const seen = manualResult.seen;

  // 2단계: 자동 스킬 스캔 (보조)
  if (hints.length < MAX_HINTS) {
    const autoRules = scanSkills();
    hints = matchAutoSkills(userText, autoRules, hints, seen);
  }

  // 3단계: 주기 경고
  const securityHint = checkSecurityAuditInterval(userText, settings);
  if (securityHint && hints.length < MAX_HINTS) {
    hints.push(securityHint);
  }

  const cleanupHint = checkCleanupInterval(settings);
  if (cleanupHint && hints.length < MAX_HINTS) {
    hints.push(cleanupHint);
  }

  if (hints.length === 0) {
    process.exit(0);
  }

  // stdout 으로 출력 → Claude Code 가 AI 컨텍스트에 system 메시지로 삽입
  console.log(
    `[auto-skill hints — 참고용, 사용자에게 노출 선택]\n${hints
      .map((h) => `- ${h}`)
      .join("\n")}`
  );
  process.exit(0);
}

main();
