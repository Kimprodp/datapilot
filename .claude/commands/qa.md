---
description: Playwright 로 실제 브라우저에서 기능 동작 검증. 3-tier 모드 (quick/standard/exhaustive).
argument-hint: [자연어 시나리오 설명 — 예: "로그인 플로우" / --quick, --exhaustive, --headed 옵션]
---

# /qa — 브라우저 동작 검증

당신(메인 AI)은 사용자의 요청에 따라 Playwright 로 실제 브라우저에서 기능을 검증한다.
시나리오 JSON 을 생성·저장·실행하고, 결과 리포트를 제시한 후 자연 대화로 후속 조치한다.

**`/code-review`, `/investigate` 와의 차이**:
- `/code-review`: 코드 정적 품질
- `/investigate`: 버그 원인 조사 (실행 X)
- `/qa`: **실제 브라우저로 동작 검증** (동적)

---

## 전제 조건

- 프로젝트 루트에서 실행.
- dev server 가 **떠있어야 한다** (http://localhost:3000 등). 없으면 사용자에게 먼저 켜달라고 안내.
- Playwright 미설치면 **자동 설치** (runner 가 알아서 처리).
- 이 커맨드는 **코드 수정하지 않는다**. 실패 시 `/investigate` 로 제안.

---

## Phase 0: 선행 체크

1. `.claude/scripts/qa/runner.js` 존재 확인.
2. `.claude/package.json` 존재 확인.
3. dev server URL 결정:
   - 사용자 인자에 URL 있으면 사용.
   - 없으면 기본값 `http://localhost:3000` 사용. 다른 포트일 가능성이 높으면 사용자 확인.
4. **Playwright 자동 설치 동의 확인** (첫 실행 시):
   ```
   Playwright 가 아직 설치되지 않았습니다.
   자동 설치할까요? (~1~2분, 약 320MB)
   
   - Y/네 → 자동 설치 후 진행
   - N → 중단
   ```
   * 설치 완료 마커: `.claude/state/tools-installed.json` 에 기록됨.

---

## Phase 1: 검증 대상 수집

### 1-1. 모드 결정

| 인자 | 모드 | 설명 |
|---|---|---|
| `--quick` | **Quick** | 30초 스모크 — 홈 + 상위 5 네비 + 콘솔 에러 감지 |
| (기본) | **Standard** | 핵심 플로우 1~2개 + 콘솔/네트워크 체크 |
| `--exhaustive` | **Exhaustive** | 모든 플로우 + 다수 상호작용 상태 + 반응형 |

인자에 옵션 지정 있으면 해당 모드, 없으면 Standard.

### 1-2. 시나리오 요구사항 수집

사용자 인자 + 대화로 수집:

- **목적**: 무엇을 확인하고 싶은가? (예: "로그인 플로우", "결제 완료", "랜딩 반응형")
- **URL/경로**: dev server 주소 + 시작 페이지
- **전제 조건**: 로그인 필요? 시드 데이터?
- **성공 기준**: 뭐가 되면 통과인가? (리다이렉트 성공, 특정 텍스트 표시, 콘솔 에러 없음)

**예시**:
```
알려주세요:
1. URL (기본: http://localhost:3000)
2. 검증 목적 (인자 "로그인 플로우" 반영)
3. 테스트 계정 (로그인 필요 시)
4. 성공 기준 (어디 도달하면 통과?)
```

---

## Phase 2: 시나리오 생성 + 사용자 승인

### 2-1. 시나리오 JSON 생성

Phase 1 정보로 `.claude/scripts/qa/scenarios/<name>.json` 작성:

**Standard 예시** (로그인):
```json
{
  "name": "login-flow",
  "mode": "standard",
  "url": "http://localhost:3000",
  "viewport": { "width": 1280, "height": 800 },
  "steps": [
    { "action": "goto", "path": "/login" },
    { "action": "fill", "selector": "#email", "value": "test@example.com" },
    { "action": "fill", "selector": "#password", "value": "password123" },
    { "action": "click", "selector": "button[type=submit]" },
    { "action": "waitForURL", "pattern": "**/dashboard", "timeout": 5000 },
    { "action": "expect", "selector": "h1", "contains": "Dashboard" }
  ]
}
```

**Quick 예시** (스모크):
```json
{
  "name": "smoke-test",
  "mode": "quick",
  "url": "http://localhost:3000",
  "steps": [
    { "action": "goto", "path": "/" },
    { "action": "goto", "path": "/about" },
    { "action": "goto", "path": "/pricing" },
    { "action": "goto", "path": "/dashboard" },
    { "action": "goto", "path": "/login" }
  ]
}
```

### 2-2. 지원 action 목록

| action | 필드 | 설명 |
|---|---|---|
| `goto` | `path` 또는 `url`, `waitUntil`, `timeout` | 페이지 이동 |
| `click` | `selector`, `timeout` | 요소 클릭 |
| `fill` | `selector`, `value`, `timeout` | input 입력 |
| `press` | `key` | 키보드 키 입력 (Enter, Tab 등) |
| `waitForSelector` | `selector`, `timeout` | 요소 나타날 때까지 대기 |
| `waitForURL` | `pattern`, `timeout` | URL 매칭 대기 |
| `wait` | `ms` | 고정 대기 |
| `expect` | `selector` + `contains`, 또는 `urlContains` | 어설션 |

### 2-3. 사용자 승인

생성한 시나리오를 **먼저 보여주고 승인 받는다**. 허락 없이 브라우저 실행 금지:

```
아래 시나리오로 진행합니다:

  1. /login 로드
  2. #email 에 "test@example.com" 입력
  3. #password 에 "password123" 입력
  4. 로그인 버튼 클릭
  5. /dashboard 로 리다이렉트 대기
  6. h1 에 "Dashboard" 포함 검증

저장 위치: .claude/scripts/qa/scenarios/login-flow.json
실행 모드: standard (headless)

진행할까요? (수정 필요하면 알려주세요)
```

---

## Phase 3: 실행 (runner)

사용자 승인 시 Bash 도구로 실행:

```bash
node .claude/scripts/qa/runner.js --scenario .claude/scripts/qa/scenarios/<name>.json
```

옵션:
- `--headed` : 브라우저 창 보이게 (기본은 headless)
- `--mode=quick|standard|exhaustive` : 모드 명시 (시나리오 JSON 에도 들어가지만 override 가능)

runner 는 stdout 으로 JSON 리포트 출력. 종료 코드:
- `0` : 모든 step 통과
- `1` : step 실패 (리포트에 상세)
- `2` : 잘못된 인자
- `3` : dev server 응답 없음
- `4` : runner 내부 크래시

---

## Phase 4: 결과 제시 + 요약

### 4-1. 리포트 파싱 + 사용자 출력

runner stdout JSON 을 파싱해서 보기 좋게 포맷:

```
📊 QA Report — <timestamp>

시나리오: login-flow (mode: standard)
URL: http://localhost:3000

Steps:
  ✅ 1. goto        /login
  ✅ 2. fill        #email
  ✅ 3. fill        #password
  ✅ 4. click       button[type=submit]
  🔴 5. waitForURL  **/dashboard
     -> Timeout 5000ms exceeded
     -> 스크린샷: .claude/scripts/qa/reports/<ts>-login-flow-step-5-fail.png

콘솔 경고/에러:
  [error] Failed to load resource: /api/auth (404)

네트워크 실패:
  POST /api/auth -> ERR_CONNECTION_REFUSED

최종 스크린샷: .claude/scripts/qa/reports/<ts>-login-flow-final.png
```

### 4-2. 종합 판정

```
─────────────────────────────
판정: 🔴 실패 (step 5/6)
- 통과: 4 step
- 실패: 1 step
- 콘솔 에러: 1건
- 네트워크 실패: 1건
─────────────────────────────
```

---

## Phase 5: 자연 대화 후속

결과에 따라 자연 대화로 이어간다.

### 실패 시
```
실패 원인을 조사할까요?

• "/investigate 로 원인 조사"     → 버그 디버깅
• "스크린샷 보여줘"              → 파일 경로 안내
• "시나리오 고쳐서 재실행"        → Phase 2 로 돌아가 수정
• "일단 넘어가"                  → 종료
• "tasks.md 에 버그 추가"         → 기록
```

### 성공 시
```
✅ 모든 step 통과.

• "tasks.md 체크오프"             → 관련 todo 완료 처리 제안
• "exhaustive 로도 돌려볼래"      → 모드 업그레이드 재실행
• "다른 시나리오"                 → Phase 1 로 돌아감
• 자연 대화로 이어가기
```

### 후속 처리 매핑

| 사용자 요청 | 메인 AI 행동 |
|---|---|
| "/investigate ..." | `/investigate` 커맨드로 전환. 실패 step 과 에러를 증상으로 전달. |
| "tasks.md 에 버그 추가" | 해당 기능 섹션 `#### 버그 수정` 에 todo 추가 |
| "재실행" | 시나리오 경로 기억해뒀다 runner 바로 재호출 |
| "시나리오 수정" | 기존 JSON 읽어서 변경점 논의 → 덮어쓰기 → 재승인 → 실행 |
| "보안도 확인해" | `/security-audit` 으로 전환 (QA 는 보안 감사 대체 아님) |

---

## 핵심 원칙 (위반 금지)

1. **사용자 승인 없이 브라우저 실행 금지**: Phase 2 에서 시나리오 확정 전 runner 호출 X.
2. **코드 수정 금지**: `/qa` 안에서 파일 수정 X. 수정은 `/investigate` → `/task` 흐름으로.
3. **시나리오는 저장**: 일회성 실행도 `scenarios/` 에 남긴다 (재사용/회귀 테스트 기반).
4. **실패는 명확히**: 통과처럼 보이게 뭉뚱그리지 말 것. 🔴 로 구분.
5. **콘솔/네트워크 경고 무시 금지**: step 이 통과해도 경고 있으면 요약에 명시.
6. **자동 Playwright 설치는 사용자 승인 후만**: 약 320MB 다운로드 = 명시적 승인 필요.

---

## 실패 시나리오 대응

| 상황 | 대응 |
|---|---|
| dev server 응답 없음 | "서버를 먼저 켜주세요 (기본 http://localhost:3000)" 후 중단 |
| Playwright 설치 사용자 거부 | 중단. "설치 없이는 `/qa` 실행 불가" 안내 |
| 시나리오 요구사항 모호 | Phase 1 에서 한 번 재질문. 그래도 모호하면 임시 가정 명시 후 진행 |
| 선택자 틀림 (step 1~2 부터 실패) | 사용자에게 올바른 selector 물어봄. 페이지 구조 읽기 요청도 가능. |
| 간헐 실패 (flaky) | 한 번 재실행 제안. 그래도 실패 시 `/investigate` 로 전환 권장. |
| runner 크래시 (code=4) | stderr 의 stack 분석. Node 버전 / Playwright 버전 문제 의심. |
