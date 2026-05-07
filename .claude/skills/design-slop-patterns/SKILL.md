---
name: design-slop-patterns
description: AI 로 생성한 UI 가 흔히 빠지는 10대 "슬롭 패턴" 체크리스트. /design-review 가 스크린샷 분석 시 기준으로 사용.
keywords: ["AI slop", "슬롭 패턴", "디자인 감사", "디자인 품질", "AI 생성 티", "바퍼웨이브"]
---

# AI Slop 10대 패턴 — 디자인 감사 기준

> "프로가 만든 느낌" 과 "AI 가 대충 만든 느낌" 을 가르는 시각적 지표들.
> 각 패턴마다 **감지 힌트 / 심각도 / 개선 방향** 을 명시.

---

## 1. 제네릭 히어로 섹션

**현상**: 풀스크린 그라데이션 배경 + 뻔한 카피 ("Build the future", "Welcome to...", "Get started today") + 센터 정렬 텍스트 + 2~3개 CTA.

**감지 힌트**:
- 상단에 50vh 이상 차지하는 그라데이션
- 초대형 h1 (64px+) 가운데 정렬
- "The future of X", "Reimagine Y", "Your X, simplified" 류 카피
- 버튼 2개 가로 나열 (primary + secondary)

**심각도**: 🟡 Medium (첫인상 직결)
**개선**: 브랜드 구체 카피, 비대칭 레이아웃, 실제 제품 스크린샷 noverlay, primary CTA 1개로 축소.

---

## 2. 3-Column 아이콘 카드 그리드

**현상**: "기능" / "장점" / "Why Choose Us" 섹션마다 정확히 3칸 grid, 각 칸에 아이콘 + 제목 + 2~3줄 설명.

**감지 힌트**:
- `grid-template-columns: repeat(3, 1fr)` (또는 동등)
- 각 카드 동일 크기, 동일 패딩, 동일 radius
- 아이콘이 이모지 또는 스트로크 SVG (비슷한 굵기)

**심각도**: 🟡 Medium (기시감 강함)
**개선**: 스토리텔링 구조 (비대칭 2열 / 수직 flow), 내용 우선순위에 따라 카드 크기 차등, 실물 예시/스크린샷 동반.

---

## 3. 바퍼웨이브 그라데이션

**현상**: 배경/버튼/테두리에 **보라 → 파랑 → 핑크** 그라데이션 남용. 2020년대 AI 아트풍.

**감지 힌트**:
- `linear-gradient(…, #6366f1, #ec4899)` 류 조합
- 한 페이지에 그라데이션 요소 3+ 곳
- hero / CTA / 카드 hover 전부 그라데이션

**심각도**: 🔴 High (가장 눈에 띄는 슬롭 시그널)
**개선**: 단일 브랜드 컬러 + 미묘한 tint 변화, 또는 흑백 대비에 포인트 컬러 1~2 strokes.

---

## 4. 장식적 Blur / Glassmorphism 과잉

**현상**: 반투명 패널 + heavy blur + 아래에 컬러 blob. iOS 위젯 베끼기.

**감지 힌트**:
- `backdrop-filter: blur(...)` + `rgba(255,255,255,0.1)` 배경
- 배경에 `radial-gradient` 색 blob 2~3개
- 반투명 패널이 3+ 요소에 반복

**심각도**: 🟡 Medium
**개선**: solid 배경 + 질감(noise)으로 depth, blur 는 핵심 1곳만.

---

## 5. 이모지 인플레이션

**현상**: 아이콘 대용으로 이모지 남발. ✨ 🚀 💡 ⚡ 🎯 등 "긍정 상징" 군집.

**감지 힌트**:
- 헤딩/카드 제목/CTA 에 이모지 섞임
- 한 페이지 고유 이모지 5+ 종
- "✨ Elegant" / "🚀 Fast" / "💡 Smart" 류 카피

**심각도**: 🟡 Medium
**개선**: 일관된 아이콘 세트 (Lucide, Heroicons, Phosphor 등) 사용, 이모지는 0~2곳만 (축하/성공 확정 표시 등).

---

## 6. 폰트 웨이트 카오스

**현상**: 한 화면에 3+ 종류의 font-weight 혼재 (예: 300/400/600/800).

**감지 힌트**:
- CSS 에서 `font-weight` 4 종 이상
- 본문 light (300) + 강조 extrabold (800) 극단 대비
- 제목마다 서로 다른 weight

**심각도**: 🟢 Low~🟡 Medium (전체 리듬 약화)
**개선**: 2 weight 규칙 (regular + bold) 또는 3 (light/regular/bold). 일관 규칙 수립.

---

## 7. 플레이스홀더 티 남

**현상**: "Lorem ipsum" 잔재, 가짜 로고 ("Logo 1", "Logo 2"), "Client Name" 과 같은 미치환 문자열.

**감지 힌트**:
- "Lorem", "Ipsum", "Placeholder", "TBD", "Coming soon"
- 동일 아바타 이미지 반복 (Unsplash 인물 무작위)
- "Example Customer" / "Your Name Here"

**심각도**: 🔴 High (프로덕션 느낌 완전 파괴)
**개선**: 실제 카피/실사용자 테스트 데이터 / 생략 가능한 섹션 제거.

---

## 8. CTA 폭주

**현상**: 한 섹션에 동일 레벨 버튼 3+ 개. "Get Started / Learn More / Watch Demo / Contact Sales".

**감지 힌트**:
- hero 에 `<button>` 요소 3+
- 같은 visual weight 의 버튼 반복
- 우선순위 불명확 (모두 primary 처럼)

**심각도**: 🔴 High (전환율 직접 타격)
**개선**: primary 1개 + secondary 1개 (ghost/link 스타일). 나머지는 스크롤 유도 또는 섹션 내부로 이동.

---

## 9. 불균일 Spacing

**현상**: 섹션 간 padding 이 제각각. 어떤 섹션은 64px, 다음은 120px, 그다음은 40px. 시각적 리듬 부재.

**감지 힌트**:
- 섹션 `padding-top/bottom` 값이 5+ 종
- 8의 배수 / 스페이싱 스케일 무시
- 카드 내부 padding 도 일관성 없음

**심각도**: 🟡 Medium
**개선**: 스페이싱 스케일 확립 (4, 8, 16, 24, 32, 48, 64, 96, 128). 섹션 padding 2~3 값으로 수렴.

---

## 10. 저대비 색상

**현상**: 연회색 텍스트 (#A0A0A0) on 흰 배경. 또는 다크모드에서 #525252 on #1A1A1A. WCAG AA 미달.

**감지 힌트**:
- 본문 색 `#ccc`, `#999`, `#777` on 흰 배경 (AA 요구 4.5:1 미달)
- "subtle" 의도로 글자 흐림 처리
- 링크 색이 본문과 거의 동일 (감지 불가)

**심각도**: 🔴 High (접근성 법적 이슈 가능)
**개선**: 대비 4.5:1 이상 확보 (#767676 on 흰 = 4.54:1). 강조 부위는 7:1 이상. 자동 체크: axe-core, WebAIM Contrast Checker.

---

## 평가 기준 (Design Grade / AI Slop Score)

### Design Grade (A~F)
- **A**: 브랜드 identity 명확, typography 정제, 섹션 리듬 일관, CTA 우선순위 뚜렷
- **B**: 큰 결함 無, 소소한 불일치
- **C**: 패턴 1~2 위반, 개선 여지 명확
- **D**: 패턴 3+ 위반, 전문성 부족 느낌
- **F**: 전반적 슬롭 — 실제 구현 재고 권장

### AI Slop Score (A~F)
Slop 패턴이 **얼마나 눈에 띄는지**:
- **A**: 슬롭 특징 거의 없음
- **B**: 1~2 개 약한 흔적 (무난)
- **C**: 3~4 개 명확한 흔적
- **D**: 5+ 개, AI 생성 티 강하게 남
- **F**: "AI 가 대충 만든 사이트" 인상 지배적

두 점수는 **독립**. 디자인은 A 지만 AI Slop 티는 남는 경우 있음 (예: 깔끔하게 만든 3-column 카드).

---

## 사용 방법 (/design-review 에서)

1. 스크린샷 Read (Playwright capture 결과)
2. DOM 체크 결과 받기 (dom-check.js 빠른 필터)
3. 위 10패턴 각각 대조 — 🔴/🟡/🟢 마킹
4. Design Grade + AI Slop Score 산출
5. 개선 제안 (해당 패턴의 "개선" 섹션 참조)
6. 사용자 요청 시 타겟 목업 생성

---

## 확장

프로젝트마다 추가 패턴이 있을 수 있다 (예: 브랜드 가이드 위반). 확장하려면 이 파일에 "## 11. ..." 형식으로 append.
