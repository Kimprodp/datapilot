---
description: 같은 디자인 요구에 대해 3~5가지 서로 다른 스타일 시안을 동시 생성. /design-review 의 목업 확장판.
argument-hint: [자연어 주제 — 예: "로그인 페이지", "Hero 섹션 CTA", "가격표"]
---

# /design-shotgun — 디자인 시안 다발 생성

당신(메인 AI)은 사용자가 디자인 방향을 고민할 때 **서로 다른 스타일 3~5개 시안** 을 동시에 생성해 비교 가능하게 만든다.

**`/design-review` 의 `mockup-gen` 과의 차이**:
- `mockup-gen`: **1개 대안** (현재 스크린샷 → 이렇게 바꾸면 이상적)
- `shotgun-gen`: **여러 다른 방향** (minimal / bold / glassmorphism / brutalist ...) 동시 생성 → 탐색용

**언제 쓰나**:
- 디자인 방향이 여러 갈래로 막힘 (어떤 톤으로 갈지 결정 안 됨)
- `/design-review` 에서 이슈 발견 후 "어떻게 고쳐?" 단계에서 탐색 필요
- 새 기능 PRD / tech-spec 중 UI 설계 전에 시안 다발 비교
- 이해관계자 (디자이너 / PM) 에게 옵션 제시

---

## 전제 조건

- Playwright 미설치면 자동 설치 (ensure-playwright).
- dev server 필요 X (순수 HTML 렌더링).
- 생성 시안은 **독립된 HTML 스니펫** 이어야 함 (외부 이미지/JS 의존성 최소).
- 코드 수정 X. 시안 렌더링 → 사용자가 방향 선택 → 이후 별도 대화/`/task` 로 실제 반영.

---

## Phase 0: 선행 체크

1. `.claude/scripts/design-review/shotgun-gen.js` 존재 확인.
2. 사용자 인자에서 주제 파악 (없으면 대화로 수집).

---

## Phase 1: 주제 + 제약 수집

### 1-1. 주제 확정
사용자 인자 또는 대화로 **"무엇에 대한 시안인지"** 확정:
- 주제 (예: "로그인 페이지 hero", "가격표 카드", "랜딩 footer")
- 컨텍스트 (브랜드 톤, 타겟 사용자, 기존 디자인 제약)
- viewport (기본 1440x900, 모바일이면 375x812)

### 1-2. 시안 방향 N 개 제안

AI 가 **4~5개 스타일 방향** 을 먼저 제안:

```
시안 방향 초안 (5개 — 조정하고 싶으면 알려주세요):

1. **Minimal** — 여백 최대, 흑백 + 단일 포인트 컬러, sans-serif regular
2. **Bold Editorial** — 큰 serif 헤드라인, 비대칭 레이아웃, 강한 대비
3. **Warm Friendly** — 라운디드, 파스텔, hand-drawn 아이콘 느낌
4. **Modern Tech** — 모노스페이스 포인트, 그리드 강조, 네이비+시안
5. **Brutalist** — 기본 시스템 폰트, 날것의 border, 비정형 배치

각 시안은 동일한 카피/기능을 담고 스타일만 다릅니다.
진행할까요? (수정 / 추가 / 제거 가능)
```

사용자 승인 또는 조정 후 Phase 2.

### 1-3. 원본 비교 포함 여부
사용자가 **기존 스크린샷 경로** 를 주면 비교 뷰에 "원본" 도 함께 표시. 선택적.
`/design-review` 직후에 /design-shotgun 호출된 경우 → 직전 capture 결과 자동 사용.

---

## Phase 2: HTML 시안 N개 작성

각 방향에 대해 **독립된 완전한 HTML 문서** 를 작성. 규칙:

- `<!DOCTYPE html>` 부터 `</html>` 까지 전체
- `<style>` 인라인 (외부 CSS 참조 X)
- 외부 이미지는 placeholder (`https://via.placeholder.com/...` 또는 CSS gradient block)
- JavaScript 없음 (정적 렌더만)
- 동일한 텍스트 내용 (카피는 모두 같고 스타일만 다름)

### 입력 JSON 구성

작성한 시안들을 다음 JSON 포맷으로 임시 파일에 저장:

```json
{
  "topic": "Hero CTA 개선",
  "viewport": "1440x900",
  "variants": [
    { "name": "minimal", "html": "<!DOCTYPE html><html>...</html>" },
    { "name": "bold-editorial", "html": "<!DOCTYPE html><html>...</html>" },
    { "name": "warm-friendly", "html": "<!DOCTYPE html><html>...</html>" },
    { "name": "modern-tech", "html": "<!DOCTYPE html><html>...</html>" },
    { "name": "brutalist", "html": "<!DOCTYPE html><html>...</html>" }
  ]
}
```

저장 위치: `.claude/scripts/design-review/shotgun/_tmp-<ts>.json`

---

## Phase 3: 렌더링 + 비교 HTML 생성

Bash 로 shotgun-gen 실행:

```bash
node .claude/scripts/design-review/shotgun-gen.js \
  --input .claude/scripts/design-review/shotgun/_tmp-<ts>.json \
  [--before <original-screenshot-path>]
```

결과:
- `.claude/scripts/design-review/shotgun/<ts>-<topic>/<name>.png` N개
- `.claude/scripts/design-review/shotgun/<ts>-<topic>/compare.html` (grid 비교)
- 옵션으로 `_before.png` 포함

---

## Phase 4: 결과 제시 + 자연 대화

```
🎨 /design-shotgun 완료

주제: <topic>
생성된 시안: N개

📁 저장 위치
  - 비교 페이지: .claude/scripts/design-review/shotgun/<ts>-<topic>/compare.html
  - 개별 PNG: 같은 폴더

🖼 비교 페이지 열기:
  브라우저에서 compare.html 을 여세요. grid 로 N개 시안 + (있다면) 원본 나란히.

다음:
- "N번 방향으로 가자"    → 해당 시안을 `/design-review mockup-gen` 으로 단일 발전
- "N번 기반으로 더"      → Phase 1 으로 돌아가 변주 생성
- "실제 코드로 반영"     → 관련 컴포넌트 파일 찾아 HTML/CSS 조정 (사용자 승인 필요)
- "tasks.md 에 결정 기록" → `/decision` 실행 제안
- "다른 주제"            → 종료
```

---

## Phase 5: 선택 후 체이닝

사용자가 방향을 선택하면 자연스러운 다음 단계:

| 사용자 반응 | 메인 AI 행동 |
|---|---|
| "minimal 로 간다" | 해당 HTML 을 `mockup-gen` 용 타겟 목업으로 다듬어 `/design-review` 의 mockup-gen 단계 진입 제안 |
| "bold + minimal 장점 합쳐" | Phase 1~2 재실행, 요구사항 반영한 새 시안 | 
| "실제 코드 수정" | 해당 컴포넌트 찾아 시안 스타일 반영 (사용자 승인 후 Edit) |
| "`/decision` 기록" | 스타일 방향 결정을 ADR 로 기록 (brand tone decision) |
| "넘어가" | 종료. 생성된 파일은 디스크에 남음 (참조용) |

---

## 핵심 원칙 (위반 금지)

1. **최소 2개, 권장 3~5개 시안**: 1개는 /mockup-gen 영역. 6개 초과는 선택 피로.
2. **스타일만 다르고 내용은 동일**: 카피/기능이 달라지면 비교 불가.
3. **HTML 은 자립**: 외부 의존성 없는 독립 문서. 네트워크 필요 X.
4. **이해관계자 친화**: compare.html 은 브라우저에서 즉시 열 수 있어야.
5. **코드 수정 분리**: 시안 선택 → 승인 → 별도 대화로 실제 반영. 자동 수정 X.
6. **결정은 ADR 로**: 최종 방향 선택하면 `/decision` 으로 기록 권장.

---

## 실패 시나리오 대응

| 상황 | 대응 |
|---|---|
| 시안 아이디어 부족 (AI 가 4 종도 못 만듦) | 대신 3개 제시 후 사용자에게 "더 필요?" 재질문 |
| HTML 렌더 실패 (스크립트 에러) | 해당 시안 "실패" 상태로 compare.html 에 표시, 나머지는 정상 진행 |
| 사용자가 "다 별로" | 요구사항 재수집 후 Phase 1~2 재시도 1회. 그래도 별로면 "현실 제약 요구사항 더 필요" 보고 |
| viewport 여러 개 필요 | 1회 실행 = 1 viewport. 모바일도 원하면 2번째 실행 권장 |
| 브랜드 가이드 있음 | Phase 1 에서 가이드 읽기. 각 시안이 가이드 안에서 움직이도록 제약 |

---

## `/design-review` 와의 흐름

```
/design-review  (현재 상태 진단)
    ↓ "#8 CTA 폭주 발견, 고치는 방향 막혔음"
/design-shotgun (3~5 방향 탐색)
    ↓ "minimal + primary 1개 방향 확정"
/decision       (이 디자인 방향 선택 이유 ADR 기록)
    ↓
자연 대화       (실제 컴포넌트 파일 수정)
```

이 체인이 가장 자연스러운 사용 패턴.
