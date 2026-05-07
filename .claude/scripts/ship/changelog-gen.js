#!/usr/bin/env node
/**
 * changelog-gen.js — 커밋 메시지 기반 CHANGELOG 생성
 *
 * 지정한 ref (기본: 직전 태그) ~ HEAD 사이 커밋을 그룹핑해서 CHANGELOG.md 에 prepend.
 * 프로젝트 루트에 CHANGELOG.md 없으면 새로 생성.
 *
 * 사용:
 *   node .claude/scripts/ship/changelog-gen.js --version 0.3.2 [--from v0.3.1] [--dry-run]
 *
 * 그룹 prefix 매핑:
 *   feat, add     -> 추가
 *   fix           -> 수정
 *   docs          -> 문서
 *   refactor      -> 리팩터
 *   chore, build  -> 기타
 *   (기타)        -> 기타
 */

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const PROJECT_ROOT = path.resolve(__dirname, "../../..");
const CHANGELOG = path.join(PROJECT_ROOT, "CHANGELOG.md");

const GROUP_MAP = [
  { regex: /^(feat|add)(\([^)]+\))?:\s*/i, group: "추가" },
  { regex: /^fix(\([^)]+\))?:\s*/i, group: "수정" },
  { regex: /^docs(\([^)]+\))?:\s*/i, group: "문서" },
  { regex: /^refactor(\([^)]+\))?:\s*/i, group: "리팩터" },
  { regex: /^(chore|build|ci)(\([^)]+\))?:\s*/i, group: "기타" },
];

function lastTag() {
  try {
    return execSync("git describe --tags --abbrev=0", {
      cwd: PROJECT_ROOT,
      stdio: ["pipe", "pipe", "pipe"],
    })
      .toString()
      .trim();
  } catch {
    return null;
  }
}

function listCommits(from) {
  const range = from ? `${from}..HEAD` : "HEAD";
  try {
    const out = execSync(`git log ${range} --pretty=format:%s`, {
      cwd: PROJECT_ROOT,
    }).toString();
    return out.split("\n").filter((l) => l.trim());
  } catch (err) {
    return [];
  }
}

function classify(message) {
  for (const { regex, group } of GROUP_MAP) {
    if (regex.test(message)) {
      return { group, text: message.replace(regex, "").trim() };
    }
  }
  return { group: "기타", text: message.trim() };
}

function buildEntry(version, commits) {
  const today = new Date().toISOString().slice(0, 10);
  const groups = {};
  for (const msg of commits) {
    const { group, text } = classify(msg);
    if (!groups[group]) groups[group] = [];
    groups[group].push(text);
  }
  let out = `## [${version}] - ${today}\n\n`;
  const order = ["추가", "수정", "문서", "리팩터", "기타"];
  for (const g of order) {
    if (!groups[g] || groups[g].length === 0) continue;
    out += `### ${g}\n`;
    for (const t of groups[g]) out += `- ${t}\n`;
    out += `\n`;
  }
  return out;
}

function prependToChangelog(entry, { dryRun = false } = {}) {
  let existing = "";
  if (fs.existsSync(CHANGELOG)) {
    existing = fs.readFileSync(CHANGELOG, "utf-8");
  } else {
    existing = "# Changelog\n\n";
  }
  // 기존 내용 중 `# Changelog` 헤더 뒤에 삽입
  let updated;
  if (/^# Changelog\n/.test(existing)) {
    updated = existing.replace(
      /^(# Changelog\n+)/,
      `$1${entry}`
    );
  } else {
    updated = `# Changelog\n\n${entry}${existing}`;
  }
  if (!dryRun) {
    fs.writeFileSync(CHANGELOG, updated, "utf-8");
  }
  return updated;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.version) {
    console.error("사용: node changelog-gen.js --version <x.y.z> [--from <ref>] [--dry-run]");
    process.exit(2);
  }
  const from = args.from || lastTag();
  const commits = listCommits(from);

  if (commits.length === 0) {
    console.log(
      JSON.stringify({
        version: args.version,
        from,
        commits: [],
        reason: "no_commits",
      })
    );
    process.exit(0);
  }

  const entry = buildEntry(args.version, commits);
  if (args.dryRun) {
    console.log(JSON.stringify({ version: args.version, from, commits: commits.length, entryPreview: entry }));
  } else {
    prependToChangelog(entry);
    console.log(JSON.stringify({ version: args.version, from, commits: commits.length, written: "CHANGELOG.md" }));
  }
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") args.dryRun = true;
    else if (a.startsWith("--version=")) args.version = a.slice(10);
    else if (a === "--version") args.version = argv[++i];
    else if (a.startsWith("--from=")) args.from = a.slice(7);
    else if (a === "--from") args.from = argv[++i];
  }
  return args;
}

try {
  main();
} catch (err) {
  console.error(JSON.stringify({ failed: true, error: err.message }));
  process.exit(1);
}
