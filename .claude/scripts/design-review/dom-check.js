#!/usr/bin/env node
/**
 * dom-check.js — /design-review rule 기반 빠른 DOM 감지
 *
 * Playwright 로 페이지 로드 후 JS evaluate 로 슬롭 패턴 힌트 수집.
 * Vision 분석(AI) 전 빠른 필터 역할.
 *
 * 감지 항목:
 *   #2 3-column repeating grid
 *   #3 그라데이션 요소 개수
 *   #5 이모지 밀도
 *   #6 font-weight 다양성
 *   #8 CTA 버튼 카운트
 *   #10 저대비 추정 (샘플)
 *
 * 사용:
 *   node .claude/scripts/design-review/dom-check.js --url http://localhost:3000
 */

const path = require("path");
const { ensurePlaywright } = require("../_shared/ensure-playwright");
const { checkDevServer } = require("../_shared/check-dev-server");

const CLAUDE_DIR = path.resolve(__dirname, "../..");

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.url) {
    console.error("사용: node dom-check.js --url <url> [--dark]");
    process.exit(2);
  }

  ensurePlaywright();

  const serverCheck = await checkDevServer(new URL(args.url).origin);
  if (!serverCheck.ok) {
    console.error(JSON.stringify({ failed: true, reason: "dev_server_unreachable" }));
    process.exit(3);
  }

  const { chromium } = require(path.join(CLAUDE_DIR, "node_modules/playwright"));
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    colorScheme: args.dark ? "dark" : "light",
  });
  const page = await context.newPage();

  try {
    await page.goto(args.url, { waitUntil: "networkidle", timeout: 15000 });
    await page.waitForTimeout(500);

    const findings = await page.evaluate(() => {
      const result = {
        ctaCount: 0,
        ctaExamples: [],
        fontWeights: [],
        gradientCount: 0,
        emojiDensity: 0,
        repeatingThreeColumnGrids: 0,
        lowContrastSamples: [],
        designSystem: {
          colors: [],
          fonts: [],
          fontSizes: [],
          spacings: [],
          radii: [],
          shadows: [],
        },
      };

      // CTA: button + a.btn + [role=button]
      const ctas = Array.from(
        document.querySelectorAll(
          'button:not([type="submit"]), a.button, a.btn, [role="button"]'
        )
      ).filter((el) => el.offsetParent !== null);
      // hero 영역 (top 800px) 안의 CTA 만 카운트
      const heroBottom = 800;
      const heroCtas = ctas.filter((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top >= 0 && rect.top < heroBottom;
      });
      result.ctaCount = heroCtas.length;
      result.ctaExamples = heroCtas.slice(0, 5).map((el) =>
        (el.innerText || el.textContent || "").trim().slice(0, 40)
      );

      // font-weights
      const weights = new Set();
      document.querySelectorAll("body *").forEach((el) => {
        if (el.offsetParent === null) return;
        const w = getComputedStyle(el).fontWeight;
        if (w) weights.add(w);
      });
      result.fontWeights = Array.from(weights).sort();

      // gradient count (CSS background-image with linear/radial-gradient)
      let gradientCount = 0;
      document.querySelectorAll("body *").forEach((el) => {
        const bg = getComputedStyle(el).backgroundImage || "";
        if (/linear-gradient|radial-gradient|conic-gradient/.test(bg)) {
          gradientCount++;
        }
      });
      result.gradientCount = gradientCount;

      // emoji density (non-ASCII in text content)
      const bodyText = document.body.innerText || "";
      const emojiRegex =
        /[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\u{1F000}-\u{1F0FF}]/gu;
      const emojis = bodyText.match(emojiRegex) || [];
      result.emojiDensity = emojis.length;

      // 3-column repeating grid
      let threeColGrids = 0;
      document.querySelectorAll("*").forEach((el) => {
        const style = getComputedStyle(el);
        if (style.display !== "grid") return;
        const cols = style.gridTemplateColumns || "";
        const parts = cols.split(" ").filter((s) => s.trim());
        if (parts.length === 3 && new Set(parts).size === 1) {
          threeColGrids++;
        }
      });
      result.repeatingThreeColumnGrids = threeColGrids;

      // low contrast 샘플: 상위 50개 텍스트 요소
      function parseColor(rgb) {
        const m = rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (!m) return null;
        return [parseInt(m[1]), parseInt(m[2]), parseInt(m[3])];
      }
      function luminance([r, g, b]) {
        const a = [r, g, b].map((v) => {
          v /= 255;
          return v <= 0.03928
            ? v / 12.92
            : Math.pow((v + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * a[0] + 0.7152 * a[1] + 0.0722 * a[2];
      }
      function contrast(c1, c2) {
        const l1 = luminance(c1);
        const l2 = luminance(c2);
        return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
      }
      const textEls = Array.from(
        document.querySelectorAll("p, span, a, li, td, h1, h2, h3, h4, h5, h6")
      )
        .filter((el) => el.offsetParent !== null)
        .slice(0, 50);
      const lowContrast = [];
      textEls.forEach((el) => {
        const s = getComputedStyle(el);
        const fg = parseColor(s.color);
        let bgEl = el;
        let bg = null;
        while (bgEl && !bg) {
          const bgStr = getComputedStyle(bgEl).backgroundColor;
          if (bgStr && bgStr !== "rgba(0, 0, 0, 0)" && bgStr !== "transparent") {
            bg = parseColor(bgStr);
            break;
          }
          bgEl = bgEl.parentElement;
        }
        if (!bg) bg = [255, 255, 255];
        if (fg && bg) {
          const ratio = contrast(fg, bg);
          if (ratio < 4.5) {
            lowContrast.push({
              text: (el.innerText || "").trim().slice(0, 40),
              ratio: Math.round(ratio * 100) / 100,
              fg: `rgb(${fg.join(",")})`,
              bg: `rgb(${bg.join(",")})`,
            });
          }
        }
      });
      result.lowContrastSamples = lowContrast.slice(0, 10);

      // ─── Design System Extraction ───
      // 전체 페이지에서 실제 사용된 디자인 토큰 추출
      const colors = new Set();
      const fonts = new Set();
      const fontSizes = new Set();
      const spacings = new Set();
      const radii = new Set();
      const shadows = new Set();

      document.querySelectorAll("body *").forEach((el) => {
        if (el.offsetParent === null) return;
        const s = getComputedStyle(el);

        // colors (color, background-color, border-color 모두)
        [s.color, s.backgroundColor, s.borderColor].forEach((c) => {
          if (!c) return;
          if (c === "rgba(0, 0, 0, 0)" || c === "transparent") return;
          colors.add(c);
        });

        // fonts (첫 fallback 기준)
        if (s.fontFamily) {
          const primary = s.fontFamily.split(",")[0].trim().replace(/^["']|["']$/g, "");
          if (primary) fonts.add(primary);
        }

        // font sizes (px 기준)
        const fs = parseFloat(s.fontSize);
        if (fs && fs > 0) fontSizes.add(Math.round(fs));

        // spacings (margin/padding, 양수만)
        ["marginTop", "marginBottom", "marginLeft", "marginRight",
         "paddingTop", "paddingBottom", "paddingLeft", "paddingRight"].forEach((k) => {
          const v = parseFloat(s[k]);
          if (v && v > 0 && v < 500) spacings.add(Math.round(v));
        });

        // border radius
        const r = parseFloat(s.borderRadius);
        if (r && r > 0) radii.add(Math.round(r));

        // box shadow
        if (s.boxShadow && s.boxShadow !== "none") {
          shadows.add(s.boxShadow);
        }
      });

      result.designSystem = {
        colors: Array.from(colors).slice(0, 30),
        fonts: Array.from(fonts),
        fontSizes: Array.from(fontSizes).sort((a, b) => a - b),
        spacings: Array.from(spacings).sort((a, b) => a - b),
        radii: Array.from(radii).sort((a, b) => a - b),
        shadows: Array.from(shadows).slice(0, 10),
      };

      return result;
    });

    const hints = generateHints(findings);

    console.log(JSON.stringify({
      url: args.url,
      dark: Boolean(args.dark),
      findings,
      hints,
    }, null, 2));
  } catch (err) {
    console.error(JSON.stringify({ failed: true, reason: "dom_check_failed", error: err.message }));
    process.exit(1);
  } finally {
    await browser.close();
  }
}

function generateHints(f) {
  const hints = [];
  if (f.ctaCount > 2) {
    hints.push({ pattern: "#8 CTA 폭주", severity: "high", detail: `hero 에 CTA ${f.ctaCount} 개 (기준 2 초과)` });
  }
  if (f.fontWeights.length > 3) {
    hints.push({ pattern: "#6 폰트 웨이트 카오스", severity: "medium", detail: `font-weight ${f.fontWeights.length} 종 (${f.fontWeights.join(", ")})` });
  }
  if (f.gradientCount >= 3) {
    hints.push({ pattern: "#3 그라데이션 과다", severity: "high", detail: `그라데이션 요소 ${f.gradientCount} 개` });
  }
  if (f.emojiDensity >= 5) {
    hints.push({ pattern: "#5 이모지 인플레이션", severity: "medium", detail: `본문 이모지 ${f.emojiDensity} 개` });
  }
  if (f.repeatingThreeColumnGrids >= 1) {
    hints.push({ pattern: "#2 3-column 카드 그리드", severity: "medium", detail: `동일폭 3열 grid ${f.repeatingThreeColumnGrids} 곳` });
  }
  if (f.lowContrastSamples.length > 0) {
    hints.push({ pattern: "#10 저대비", severity: "high", detail: `WCAG AA 미달 텍스트 ${f.lowContrastSamples.length} 건 샘플` });
  }

  // ─── Design System 지표 기반 힌트 ───
  const ds = f.designSystem || {};
  if (Array.isArray(ds.colors) && ds.colors.length > 12) {
    hints.push({ pattern: "팔레트 카오스", severity: "high", detail: `색상 ${ds.colors.length} 종 (권장 ≤ 8). 토큰 정리 필요.` });
  } else if (Array.isArray(ds.colors) && ds.colors.length > 8) {
    hints.push({ pattern: "팔레트 많음", severity: "medium", detail: `색상 ${ds.colors.length} 종 (권장 ≤ 8).` });
  }
  if (Array.isArray(ds.fonts) && ds.fonts.length > 3) {
    hints.push({ pattern: "폰트 패밀리 과다", severity: "medium", detail: `폰트 ${ds.fonts.length} 종 (${ds.fonts.slice(0, 4).join(", ")}) — 권장 1~2종.` });
  }
  if (Array.isArray(ds.fontSizes) && ds.fontSizes.length > 12) {
    hints.push({ pattern: "타이포 스케일 산만", severity: "medium", detail: `폰트 사이즈 ${ds.fontSizes.length} 종 — modular scale 재정리 권장.` });
  }
  if (Array.isArray(ds.radii) && ds.radii.length === 1 && ds.radii[0] > 0) {
    hints.push({ pattern: "#4 균일 버블 radius", severity: "medium", detail: `모든 요소 radius ${ds.radii[0]}px 단일 — 위계 부재 가능성.` });
  }
  if (Array.isArray(ds.spacings) && ds.spacings.length > 15) {
    const base = ds.spacings[0] || 4;
    const offScale = ds.spacings.filter((v) => base > 0 && v % base !== 0).length;
    if (offScale > ds.spacings.length / 2) {
      hints.push({ pattern: "#9 불균일 spacing", severity: "medium", detail: `spacing ${ds.spacings.length} 종 중 과반이 base ${base}의 배수 아님.` });
    }
  }

  return hints;
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dark") args.dark = true;
    else if (a.startsWith("--url=")) args.url = a.slice(6);
    else if (a === "--url") args.url = argv[++i];
  }
  return args;
}

main().catch((err) => {
  console.error(JSON.stringify({ failed: true, reason: "crash", error: err.message }));
  process.exit(4);
});
