---
description: 스크린샷 + AI Vision 기반 디자인 품질 감사. 10대 AI Slop 패턴 체크 + 타겟 목업 생성.
argument-hint: [URL 또는 자연어 — 예: "랜딩 페이지 http://localhost:3000"]
---

# /design-review — 디자인 품질 감사

당신(메인 AI)은 Playwright 로 스크린샷을 캡처하고, DOM rule 기반 빠른 체크와 **자체 Vision 분석** 을 결합해 디자인 품질을 감사한다.
`design-slop-patterns` 스킬의 10대 AI Slop 패턴이 기준.

**`/qa` 와의 차이**:
- `/qa`: 기능 동작 검증 (클릭/입력/리다이렉트)
- `/design-review`: **시각적 품질 감사** (슬롭 패턴, Design Grade, AI Slop Score)

**`/code-review` 와의 차이**:
- `/code-review`: 코드 정적 품질
- `/design-review`: 렌더된 화면 시각 품질 (코드 안 봄)

---

## 전제 조건

- dev server 가 떠있어야 한다.
- Playwright 미설치 시 자동 설치 (/qa 와 공유).
- `design-slop-patterns` 스킬을 Skill tool 로 로드한다 (10패턴 체크리스트 확보).
- 이 커맨드는 코드를 수정하지 않는다. 수정은 자연 대화 또는 `/task` 로.

---

## Phase 0: 선행 체크

1. `.claude/scripts/design-review/capture.js`, `dom-check.js`, `mockup-gen.js` 존재 확인.
2. `.claude/skills/design-slop-patterns/SKILL.md` 존재 확인. **Skill tool 로 로드**.
3. 대상 URL 결정 (인자 또는 기본 `http://localhost:3000`).

---

## Phase 1: 감사 대상 수집

사용자 인자/대화로 확정:

- **URL/경로**: 구체 페이지 (예: `/`, `/pricing`, `/dashboard`)
- **viewport**: 기본 desktop + mobile. 필요 시 tablet/widescreen 추가.
- **다크모드**: 감지 시 자동 포함, 또는 사용자 명시 (`다크 포함`).
- **특정 interaction state**: hover, modal open 등 필요하면 캡처 전 setup 안내.

**예시 응답**:
```
감사 계획:
- URL: http://localhost:3000/pricing
- Viewport: desktop (1440x900) + mobile (375x812)
- 다크모드: 라이트만 (아직 미구현 가정)

진행할까요?
```

---

## Phase 2: 스크린샷 캡처

Bash 로 `capture.js` 실행:

```bash
node .claude/scripts/design-review/capture.js \
  --url <URL> \
  --name <page-name> \
  [--viewports=desktop,mobile,tablet] \
  [--dark]
```

결과 JSON 에서 캡처된 파일 경로 추출. 경로 예: `.claude/scripts/design-review/reports/<ts>-pricing-desktop.png`

---

## Phase 3: DOM 자동 체크 (rule-based 빠른 필터)

Bash 로 `dom-check.js` 실행:

```bash
node .claude/scripts/design-review/dom-check.js --url <URL> [--dark]
```

결과 JSON 구조:
```json
{
  "findings": {
    "ctaCount": 3,
    "fontWeights": ["300", "400", "600", "800"],
    "gradientCount": 5,
    "emojiDensity": 7,
    "repeatingThreeColumnGrids": 2,
    "lowContrastSamples": [...],
    "designSystem": {
      "colors": ["rgb(10, 10, 10)", "rgb(255, 255, 255)", ...],
      "fonts": ["Inter", "JetBrains Mono"],
      "fontSizes": [12, 14, 16, 20, 28, 40],
      "spacings": [4, 8, 12, 16, 24, 32, 48, 64],
      "radii": [8],
      "shadows": ["0 1px 3px rgba(0,0,0,0.1)", ...]
    }
  },
  "hints": [
    { "pattern": "#8 CTA 폭주", "severity": "high", "detail": "..." },
    { "pattern": "팔레트 많음", "severity": "medium", "detail": "색상 10 종" },
    { "pattern": "#4 균일 버블 radius", "severity": "medium", "detail": "모든 요소 radius 8px 단일" }
  ]
}
```

이 hints 는 Phase 4 Vision 분석에 **사전 정보** 로 활용. 덮어쓰기 X (Vision 이 최종 판단).

---

## Phase 4: AI Vision 분석 (핵심)

### 4-1. 스크린샷 Read
Phase 2 에서 캡처된 PNG 각각을 Read 도구로 로드.

### 4-2. 10패턴 대조
`design-slop-patterns/SKILL.md` 의 10개 패턴을 기준으로 각 스크린샷 분석:
- 각 패턴별로 🔴 High / 🟡 Medium / 🟢 Low 또는 "해당 없음"
- 근거: 스크린샷에서 관찰된 구체적 시각 요소 (색상, 크기, 배치 등)

### 4-3. DOM 힌트 교차 검증
Phase 3 hints 를 Vision 관찰과 교차 검증:
- DOM hint 는 있는데 Vision 에서 안 보임 → 화면 밖 요소 가능성 (무시 OK)
- Vision 에서 보이는데 DOM hint 無 → 수동 검출 (더 정확)
- 둘 다 감지 → 강한 확정 시그널

### 4-4. Design Grade / AI Slop Score 산출
`design-slop-patterns` 의 평가 기준 적용:
- **Design Grade**: A~F (브랜드 identity, typography, 리듬, 우선순위 기반)
- **AI Slop Score**: A~F (슬롭 패턴 가시성 기반)

두 점수는 **독립**. 근거 명시.

---

## Phase 5: 리포트 + 요약

```markdown
# Design Review — <URL>

**스크린샷**
- desktop: .claude/scripts/design-review/reports/<ts>-<name>-desktop.png
- mobile: .../<ts>-<name>-mobile.png
- (dark): .../<ts>-<name>-desktop-dark.png

## 🎯 종합 평가
- **Design Grade**: B (근거: typography 정돈됐으나 CTA 우선순위 불명확)
- **AI Slop Score**: C (근거: 그라데이션 과다 + 3-column 카드)

## 🎨 Design System (자동 추출)

dom-check.js 의 `designSystem` 결과 기반:

- **색상**: N종 (🟢 ≤8 / 🟡 9~12 / 🔴 13+) — 주요 팔레트 5~8개 예시
- **폰트 패밀리**: N종 (🟢 1~2 / 🟡 3 / 🔴 4+) — 각 이름 명시
- **폰트 사이즈**: N종 — 숫자 목록 (예: 12 / 14 / 16 / 20 / 28 / 40). modular scale 비율 감지 시 ✅
- **폰트 웨이트**: N종 (기존 findings.fontWeights 와 동일 소스)
- **Spacing**: N종 — 숫자 목록. 8의 배수 정렬 여부 명시
- **Radius**: N종 — 단일 (⚠️ #4 Slop 가능성) vs 다양 (위계 있음)
- **Shadow**: N종

> 이 섹션은 Vision 판정과 **별개의 정량 데이터**. 10패턴 결론과 교차 검증용.

## 🔴 Critical (High 심각도)

### #3 바퍼웨이브 그라데이션 (보라→핑크)
- **근거**: hero 배경 + "기능" 섹션 배경 + CTA 모두 같은 그라데이션
- **DOM 확인**: gradient 요소 5 개
- **개선**: 단일 브랜드 컬러 + 미묘한 tint 로 교체. 최대 1~2 곳에만 사용.

### #8 CTA 폭주
- **근거**: hero 에 "Get Started" / "Watch Demo" / "Contact Sales" 3개
- **DOM 확인**: ctaCount: 3
- **개선**: primary 1개 ("Get Started") + 링크 스타일 "Watch Demo" → "Contact Sales" 는 섹션 내부로

## 🟡 Warning (Medium)

### #2 3-column 카드 그리드
- 기능 섹션 + 가격 섹션 모두 3-column 동일 패턴
- 개선: 기능 섹션은 비대칭 2열 스토리 흐름으로

### #6 폰트 웨이트 카오스
- 300/400/600/800 4종 사용
- 개선: regular + bold 2 종으로 통일 권장

## 🟢 잘한 부분

- 대비 충족 (샘플 50개 중 WCAG AA 미달 0건)
- 섹션 spacing 일관성 양호 (8의 배수 스케일 감지)
- 실제 제품 스크린샷 사용 (#7 플레이스홀더 없음)

## 💡 추천 다음 행동

1. 🔴 **가장 먼저**: #3 그라데이션 정돈 → 전체 톤 단숨에 개선
2. 🔴 #8 CTA 우선순위 재편 → 전환율 영향 큼
3. 🟡 #2, #6 은 리팩터 할 때 같이 해결
```

### 요약 블록
```
─────────────────────────────
📊 Design Review 요약
- Design Grade: B
- AI Slop Score: C
- 🔴 High: 2건 / 🟡 Medium: 2건 / 🟢 잘한 부분: 3건
─────────────────────────────
```

---

## Phase 6: 타겟 목업 생성 (사용자 요청 시)

"목업 보여줘" / "이렇게 바꾸면 어떨지 목업" 등 요청 시:

### 6-1. 대상 선정
어느 이슈의 목업을 만들지 확인. 예: "#8 CTA 폭주" 수정 목업.

### 6-2. HTML/CSS 작성
해당 섹션의 수정안을 독립된 HTML 파일로 작성 (스타일 포함, 외부 의존성 無):

```html
<!DOCTYPE html>
<html><head><style>
  body { margin:0; font-family: system-ui; }
  .hero { min-height: 60vh; padding: 64px 32px; background: #0A0A0A; color: #fff; display: flex; flex-direction: column; justify-content: center; gap: 24px; max-width: 640px; margin: 0 auto; }
  h1 { font-size: 56px; line-height: 1.1; margin: 0; }
  p { font-size: 18px; color: #a0a0a0; margin: 0; }
  .cta-row { display: flex; gap: 16px; align-items: center; margin-top: 8px; }
  .btn { padding: 12px 24px; border-radius: 8px; font-size: 16px; font-weight: 600; border: none; cursor: pointer; }
  .btn-primary { background: #3B82F6; color: white; }
  .link { color: #a0a0a0; text-decoration: none; }
  .link:hover { color: #fff; }
</style></head>
<body>
<section class="hero">
  <h1>실제 제품 가치 카피</h1>
  <p>서브 설명 한 줄.</p>
  <div class="cta-row">
    <button class="btn btn-primary">Get Started</button>
    <a href="#demo" class="link">Watch Demo →</a>
  </div>
</section>
</body></html>
```

임시 파일로 저장: `.claude/scripts/design-review/mockups/_tmp-<ts>.html`

### 6-3. 렌더링 + 캡처
```bash
node .claude/scripts/design-review/mockup-gen.js \
  --html .claude/scripts/design-review/mockups/_tmp-<ts>.html \
  --name cta-fixed \
  --before .claude/scripts/design-review/reports/<ts>-pricing-desktop.png
```

출력:
- 목업 스크린샷: `mockups/<ts>-cta-fixed.png`
- 비교 HTML: `mockups/<ts>-cta-fixed-compare.html` (원본/목업 나란히)

### 6-4. 사용자에게 제시
```
목업 생성 완료:
- 목업 스크린샷: .claude/scripts/design-review/mockups/<ts>-cta-fixed.png
- 비교 페이지 (브라우저로 열기): mockups/<ts>-cta-fixed-compare.html

이 방향으로 실제 코드 수정할까요?
→ "네" 하면 관련 컴포넌트 파일 찾아서 실제 코드로 반영 (사용자 승인 받으며)
```

---

## Phase 7: 자연 대화 후속

| 사용자 요청 | 메인 AI 행동 |
|---|---|
| "#3 목업 보여줘" | Phase 6 진입 (단일 방향 목업) |
| "방향이 막힘 / 여러 안 볼래" | **`/design-shotgun` 으로 3~5개 다른 스타일 시안 동시 생성 제안** |
| "#8, #3 tasks.md 에 추가" | `#### 디자인 수정` 섹션 추가 |
| "#2 더 자세히" | 해당 패턴 상세 + 개선 방향 확장 |
| "모바일 뷰도 다시 봐줘" | mobile 스크린샷 재분석 |
| "다크모드 체크" | `--dark` 추가 Phase 2 부터 재실행 |
| "실제 코드 수정" | 해당 컴포넌트 파일 찾아서 수정 (사용자 승인) |
| "톤/방향 결정됨" | `/decision` 으로 디자인 방향 ADR 기록 제안 |
| "넘어가" | 종료 |

### 자연 체이닝 패턴
```
발견 (이 페이지에 이런 슬롭 있음)
  → 막힘 (어떻게 고쳐?)
    → /design-shotgun (3~5 시안 탐색)
      → 방향 선택
        → /decision (톤 결정 ADR)
          → mockup-gen 으로 1 방향 다듬기 (Phase 6)
            → 실제 코드 수정 (자연 대화)
```

### tasks.md 추가 형식

```markdown
### 공통 / 디자인
  #### 디자인 수정 (<YYYY-MM-DD> Design Review)
    - [ ] **[D1] #8 CTA 폭주 정리** [S]
      - 근거: hero 에 CTA 3개
      - 개선: primary 1 + link 1 로 축소
      - 목업: mockups/<ts>-cta-fixed.png
```

---

## 핵심 원칙 (위반 금지)

1. **10패턴 모두 평가**: 자의적 축소 금지. "해당 없음" 도 명시.
2. **DOM 과 Vision 교차**: 한쪽만 믿지 않는다. Vision 이 최종 판단자.
3. **근거는 구체적으로**: "폰트가 이상해" X → "h1 900, 본문 300 극단 대비" O
4. **Design Grade ≠ AI Slop Score**: 섞지 말 것. 둘 다 산출.
5. **코드 수정은 별도**: 리뷰 안에서 수정 X. 목업 → 사용자 승인 → 수정 분리.
6. **타겟 목업은 요청 시에만**: 매번 자동 생성 X (Phase 6 진입은 사용자 요청 후).

---

## 실패 시나리오 대응

| 상황 | 대응 |
|---|---|
| dev server 없음 | "서버 켜주세요" 후 중단 |
| capture.js 실패 (페이지 로드 타임아웃) | Phase 2 재시도, waitUntil 완화 |
| dom-check.js 실패 | Phase 4 Vision 만으로 진행 (hints 는 없지만 분석 가능) |
| 캡처는 됐는데 Vision 판단 애매 | "근거 불충분" 플래그 + 사용자에게 수동 확인 요청 |
| 사용자가 요청 모드가 "급속" | Phase 3 건너뛰고 Phase 2 → Phase 4 바로 (Vision only) |
