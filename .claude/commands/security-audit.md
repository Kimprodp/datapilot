---
description: 독립 보안 감사 — @security 에이전트를 단독 호출. 주기적/전면적 감사용.
argument-hint: [감사 범위 — 예: "인증", "결제", "외부API" / 생략 시 전체]
---

# /security-audit — 독립 보안 감사

당신(메인 AI)은 `@security` 에이전트를 **독립적으로** 호출하여 프로젝트의 특정 영역 또는 전체에 대해 보안 감사를 수행한다.
감사 완료 후 `.claude/state/security-audit.json` 에 이력을 자동 기록한다.

**`/code-review 보안까지` 와의 차이**:
- `/code-review`: 최근 **코드 변경** 에 초점 (git diff)
- `/security-audit`: **정기적/전면적** 감사 (변경 유무 무관)

**언제 쓰나**:
- 정기 보안 점검 (월 1회 등)
- 민감 기능 (결제/인증) 구현 직후 집중 감사
- 외부 감사 준비
- `auto-skill` Hook 이 주기 초과 알림을 띄웠을 때

---

## 전제 조건

- 프로젝트 루트에서 실행.
- `.claude/agents/security.md` 존재 확인. 없으면 "하니스 설치 확인 필요" 보고.
- 말투 안티패턴 금지.
- 감사 결과는 **세션에만** 출력. 별도 파일 저장 X (사용자가 요청 시 tasks.md 추가).

---

## Phase 0: 선행 체크

1. `.claude/agents/security.md` 존재 확인.
2. `.claude/state/` 폴더 존재 확인 (없으면 생성).
3. `docs/context.md` 존재 여부 (있으면 보안 관련 ADR 참조).

---

## Phase 1: 감사 범위 수집

### 1-1. 인자 해석

| 인자 예시 | 범위 |
|---|---|
| (없음) | 전체 감사 (모든 영역) |
| "인증" / "auth" | 인증/세션/토큰 관련 |
| "결제" / "payment" | 결제/환불/PG 연동 |
| "외부API" / "external" | 서드파티 API 호출 |
| "권한" / "admin" | 권한/Admin 기능 |
| "데이터" / "storage" | DB/파일/암호화 |
| 파일 경로 | 해당 파일/폴더만 |
| `--owasp A01,A03` | 특정 OWASP 카테고리 |

인자 해석이 모호하면 사용자에게 한 번 확인.

### 1-2. 대상 파일 식별

범위가 "인증" 이면:
- 키워드 검색 (Grep): `login`, `auth`, `session`, `jwt`, `token`, `password`, `oauth`, `sign`
- 디렉토리 추정: `auth/`, `login/`, `middleware/`, `user/`
- `docs/features/<auth|login|signup 포함>/` 의 tech-spec 참조

"결제" 이면 `payment`, `card`, `refund`, `billing`, `checkout`, `stripe`, `paypal` 등.

최종 대상 목록을 사용자에게 간략히 보여줌:

```
감사 대상 (추정):
- 파일 12개
- 주요 경로: auth/, middleware/session.ts, user/password.ts
- 관련 ADR: context.md [security] 태그 3건
이대로 진행? (수정 필요 시 알려주세요)
```

### 1-3. 맥락 확인

`docs/context.md` 의 `[security]`, `[feature: <해당>]` 태그 ADR 이 있으면 Read 하여 배경 맥락 파악. 나중에 @security 에이전트 호출 시 프롬프트에 포함.

---

## Phase 2: @security 배치 호출

Agent tool 로 `security` 서브에이전트 호출:

```
Agent(
  subagent_type: "security",
  description: "독립 보안 감사 — <범위>",
  prompt: `
다음 범위를 OWASP Top 10 기준으로 심화 감사하라.
이것은 최근 변경이 아닌 **전면 감사** 다. 실제 악용 가능한 취약점에 집중.

감사 대상:
<Phase 1-2 에서 식별된 파일 목록>

범위 키워드: <"인증" 등>

참조 문서 (Read 로 로드):
- CLAUDE.md (프로젝트 원칙)
- docs/context.md (관련 [security] ADR)
- docs/features/<관련 기능들>/prd.md, tech-spec.md (있으면)

감사 체크리스트:
1. OWASP Top 10 카테고리별 (A01 Broken Access Control ~ A10 SSRF)
2. 이 프로젝트 맥락 특화 위험 (예: 결제 → PG 토큰 관리, 인증 → 세션 저장 방식)
3. "검토 완료 (이상 없음)" 섹션 반드시 포함
4. 각 이슈의 실제 악용 시나리오 서술

출력은 스킬 지침의 Security Audit Report 형식 준수.
사용자와 대화하지 말고 리포트만 반환.
`
)
```

---

## Phase 3: 결과 제시 + 요약

### 3-1. 리포트 원본 출력
에이전트 리포트 원본 그대로 사용자에게 전달.

### 3-2. 요약 블록

```
─────────────────────────────
🔒 Security Audit 요약
- 범위: <Phase 1 범위>
- 🚨 High: N건
- ⚠️  Medium: N건
- 💡 Low: N건
- ✅ 검토 완료 (이상 없음): K 항목
- 전반 판정: 🟢 / 🟡 / 🔴
─────────────────────────────
```

### 3-3. 자연 대화 안내

```
이 결과로 어떻게 진행할까요?

• "H1, H2 지금 고쳐줘"          → 대화로 코드 수정
• "High/Medium tasks.md 에 추가"  → 할 일로 기록
• "H3 더 설명해"                → 상세 설명 + 공격 시나리오 재구성
• "리포트만 보고 종료"           → 감사 기록만 남기고 종료

또는 그대로 두고 나중에 보셔도 됩니다.
```

---

## Phase 4: 자연어 후속 조치

| 사용자 요청 | 메인 AI 행동 |
|---|---|
| "<ID> 고쳐줘" | 해당 파일 수정 (사용자 확인 받으며) |
| "tasks.md 에 추가" | `#### 보안 수정 (<YYYY-MM-DD> 감사)` 섹션 추가 |
| "<ID> 더 설명" | 에이전트 리포트에서 해당 항목 맥락 상세 제시 |
| "리포트만, 종료" | Phase 5 기록 후 종료 |
| "이건 수용 가능한 위험이야" | tasks.md 에 [accepted] 태그로 기록 |

### tasks.md 추가 형식 (요청 시)

```markdown
### 공통 / 보안
  #### 보안 수정 (<YYYY-MM-DD> 감사, 범위: <범위>)
    - [ ] **[H1] <이슈 제목>** [S/M/L]
      - 파일: <경로:라인>
      - 공격 시나리오: <1줄 요약>
      - AC: <수정 후 공격 재현 실패>
    - [ ] **[M1] ...**
```

---

## Phase 5: state 파일 자동 기록

감사 완료 직후 (리포트 출력 후 즉시) `.claude/state/security-audit.json` 갱신:

### 5-1. 파일 읽기

파일이 없으면 초기 구조로 시작:
```json
{
  "last_audit": null,
  "audits_history": [],
  "sensitive_changes_since_last_audit": []
}
```

### 5-2. 갱신 내용

```json
{
  "last_audit": {
    "date": "<오늘 ISO>",
    "scope": "<Phase 1 범위 — 예: '인증' 또는 '전체'>",
    "issues_found": {
      "high": <Phase 3 요약의 High 개수>,
      "medium": <Medium>,
      "low": <Low>
    }
  },
  "audits_history": [
    ...기존 이력,
    { "date": "<오늘>", "scope": "<범위>", "high": N, "medium": M, "low": K }
  ],
  "sensitive_changes_since_last_audit": []
}
```

- `last_audit` 은 최신 감사로 덮어씀.
- `audits_history` 는 뒤에 append (최근 10개만 유지).
- `sensitive_changes_since_last_audit` 는 빈 배열로 **리셋** (다시 쌓이기 시작).

### 5-3. 기록 완료 안내 (짧게)

```
📝 감사 이력 기록됨: .claude/state/security-audit.json
```

---

## 핵심 원칙 (위반 금지)

1. **@security 는 배치 호출**: 사용자와 대화 시도 금지.
2. **메인 AI 는 지휘자**: 파일 식별, 맥락 수집, 사용자 대화만 담당. 실제 분석은 에이전트.
3. **리포트 저장 금지**: 파일로 저장하지 않는다. 필요 시 tasks.md 추가로 대체.
4. **state 파일은 반드시 갱신**: Phase 5 건너뛰지 말 것. 주기 체크 시스템의 근거가 됨.
5. **OWASP 체크리스트 강제**: 자의적 축소 금지. "이상 없음" 섹션 포함 필수.
6. **자동 수정 금지**: 사용자 승인 없이 코드 수정하지 않는다.

---

## 실패 시나리오 대응

| 상황 | 대응 |
|---|---|
| 범위 식별 실패 (키워드 안 걸림) | 사용자에게 파일/폴더 명시 요청 |
| @security 호출 실패 | 오류 보고 + 재시도 제안 |
| 에이전트가 빈 리포트 반환 | 수동 재호출 + 컨텍스트 강화 |
| state 파일 쓰기 실패 | 권한/경로 확인 보고. 감사 결과는 이미 전달된 상태라 치명 아님 |
| "전체" 인데 대상 너무 큼 (500+ 파일) | 사용자에게 우선순위 영역 먼저 감사 제안 |
