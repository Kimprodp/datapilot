#!/usr/bin/env node
/**
 * bump-version.js — 버전 자동 증가
 *
 * 감지된 버전 파일의 버전을 semver bump (patch/minor/major).
 * 지원: VERSION, package.json, pubspec.yaml
 *
 * 사용:
 *   node .claude/scripts/ship/bump-version.js --type patch [--dry-run]
 *   node .claude/scripts/ship/bump-version.js --type minor --target VERSION
 *
 * 출력: JSON { detected, previous, next, bumped_files }
 */

const fs = require("fs");
const path = require("path");

const PROJECT_ROOT = path.resolve(__dirname, "../../..");

function detectVersionFiles() {
  const candidates = [
    { file: "VERSION", type: "plain" },
    { file: "package.json", type: "package.json" },
    { file: "pubspec.yaml", type: "pubspec.yaml" },
  ];
  return candidates.filter((c) =>
    fs.existsSync(path.join(PROJECT_ROOT, c.file))
  );
}

function readVersion({ file, type }) {
  const full = path.join(PROJECT_ROOT, file);
  const raw = fs.readFileSync(full, "utf-8");
  switch (type) {
    case "plain":
      return raw.trim();
    case "package.json": {
      const pkg = JSON.parse(raw);
      return pkg.version || null;
    }
    case "pubspec.yaml": {
      const m = raw.match(/^version:\s*([0-9]+\.[0-9]+\.[0-9]+)(\+\d+)?/m);
      return m ? m[1] : null;
    }
  }
  return null;
}

function writeVersion({ file, type }, newVersion) {
  const full = path.join(PROJECT_ROOT, file);
  const raw = fs.readFileSync(full, "utf-8");
  let updated;
  switch (type) {
    case "plain":
      updated = newVersion + "\n";
      break;
    case "package.json": {
      const pkg = JSON.parse(raw);
      pkg.version = newVersion;
      updated = JSON.stringify(pkg, null, 2) + "\n";
      break;
    }
    case "pubspec.yaml":
      updated = raw.replace(
        /^(version:\s*)([0-9]+\.[0-9]+\.[0-9]+)(\+\d+)?/m,
        (_, prefix, _ver, buildSuffix) =>
          `${prefix}${newVersion}${buildSuffix || ""}`
      );
      break;
  }
  fs.writeFileSync(full, updated, "utf-8");
}

function bumpSemver(version, type) {
  const m = version.match(/^(\d+)\.(\d+)\.(\d+)/);
  if (!m) throw new Error(`invalid semver: ${version}`);
  let [_, major, minor, patch] = m;
  [major, minor, patch] = [major, minor, patch].map(Number);
  switch (type) {
    case "major":
      return `${major + 1}.0.0`;
    case "minor":
      return `${major}.${minor + 1}.0`;
    case "patch":
    default:
      return `${major}.${minor}.${patch + 1}`;
  }
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const bumpType = args.type || "patch";
  const dryRun = Boolean(args.dryRun);

  let files = detectVersionFiles();
  if (args.target) {
    files = files.filter((f) => f.file === args.target);
    if (files.length === 0) {
      console.error(JSON.stringify({ failed: true, reason: "target_not_found", target: args.target }));
      process.exit(2);
    }
  }

  if (files.length === 0) {
    console.log(JSON.stringify({ detected: [], reason: "no_version_file" }));
    process.exit(0);
  }

  const results = [];
  for (const f of files) {
    const prev = readVersion(f);
    if (!prev) {
      results.push({ file: f.file, error: "version_not_found" });
      continue;
    }
    let next;
    try {
      next = bumpSemver(prev, bumpType);
    } catch (err) {
      results.push({ file: f.file, previous: prev, error: err.message });
      continue;
    }
    if (!dryRun) writeVersion(f, next);
    results.push({ file: f.file, previous: prev, next, bumped: !dryRun });
  }

  console.log(JSON.stringify({ bumpType, dryRun, results }, null, 2));
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") args.dryRun = true;
    else if (a.startsWith("--type=")) args.type = a.slice(7);
    else if (a === "--type") args.type = argv[++i];
    else if (a.startsWith("--target=")) args.target = a.slice(9);
    else if (a === "--target") args.target = argv[++i];
  }
  return args;
}

try {
  main();
} catch (err) {
  console.error(JSON.stringify({ failed: true, error: err.message }));
  process.exit(1);
}
