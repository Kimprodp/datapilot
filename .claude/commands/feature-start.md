---
description: 기능 단위 기획 (PRD 작성) — idea-validation 축약 + product-spec 축약 + @plan-reviewer 검토
argument-hint: <기능명>
---

# /feature-start — 기능 PRD 작성

당신(메인 AI)은 새 기능의 PRD를 작성한다.
`idea-validation`과 `product-spec` 스킬의 **축약 버전**을 순차 실행하여 PRD를 만든 후,
`@plan-reviewer`에게 검토를 맡기고 사용자 컨펌을 받아 `docs/features/<기능명>/prd.md`로 확정한다.

**인자**: 기능명 (slug 형식: 소문자, 하이픈. 예: `signup`, `payment`, `booking-list`)

---

## 전제 조건

- **프로젝트 루트에서 실행**.
- `docs/plan.md`, `CLAUDE.md` 존재 필수 (없으면 `/kickoff` 먼저 안내하고 중단).
- 사용자와 **직접 대화** (서브에이전트로 인터뷰 금지).
- 말투는 직설적·간결. 안티패턴 표현 금지.

---

## Phase 0: 선행 체크

### 0-1. 맥락 확인
- `docs/plan.md`, `docs/context.md`, `CLAUDE.md` 를 Read로 로드.
- 기능명 slug 검증 (소문자, 하이픈, 특수문자 없음).

### 0-2. 기능 디렉토리 상태

- `docs/features/<기능명>/prd.md` 존재?
  → 사용자에게: **"이미 PRD가 있다. 어떻게 할까?"**
    - A. 덮어쓰기 (기존은 `docs/features/<기능명>/prd.backup-<timestamp>.md`로 이동)
    - B. 취소
    - C. 기존 PRD 수정 대화 (섹션별 수정 — 이 커맨드 범위 밖, 사용자가 수동 편집 권장)

- `.draft/features/<기능명>/feature-start-progress.md` 존재?
  → 이어서 진행 여부 확인.

### 0-3. 작업 공간 준비

- `.draft/features/<기능명>/` 생성.
- `feature-start-progress.md` 초기화.

```markdown
# Feature Start Progress — <기능명>
- **Started**: YYYY-MM-DD HH:MM
- [ ] Phase 1 — Idea (축약)
- [ ] Phase 2 — Spec (축약)
- [ ] Phase 3 — PRD 생성
- [ ] Phase 4 — 검토 + 확정
```

---

## Phase 1: Idea Validation (기능 단위 축약)

### 실행
1. **Skill tool로 `idea-validation` 스킬 로드**.
2. **축약 버전 수행** — 기능 단위이므로 6질문 중 **핵심 3개만**:
   - **Q1 (수요 현실)**: 이 기능을 원한다는 증거는? (기존 사용자 요청/유사 기능 사용 데이터/외부 레퍼런스)
   - **Q3 (구체성)**: 이 기능이 가장 필요한 사용자 1명 구체 묘사 (직업/상황/고통)
   - **Q4 (최소 버전)**: 이 기능의 "MVP of MVP" — 이번 주 내 릴리즈 가능한 최소 형태
3. Q2/Q5/Q6 는 프로덕트 레벨에서 이미 검증됨 → 생략. 단 **프로덕트 방향과 충돌**한다고 판단되면 사용자에게 물어 재검증.
4. 모호한 답변 2회 push.

### 산출
- `.draft/features/<기능명>/idea-<YYYY-MM-DD>.md` 저장.
- Progress 업데이트.

---

## Phase 2: Product Spec (기능 단위 축약)

### 실행
1. `.draft/features/<기능명>/idea-*.md` 를 Read.
2. **Skill tool로 `product-spec` 스킬 로드**.
3. **축약 버전 수행** — 7섹션 중 **핵심 5개**:
   - 섹션 1 (가치 제안)
   - 섹션 2 (핵심 기능 — 이 기능의 서브 기능들)
   - 섹션 3 (차별점) — 프로덕트 레벨 차별점에 기여하는 방식
   - 섹션 4 (지표) — 기능 성공/실패 판단
   - 섹션 5 (범위 경계)
4. 섹션 6 (접근 비교), 섹션 7 (전제) 는 기능 단위에선 보통 단순 → 필요 시에만 포함.

### 산출
- `.draft/features/<기능명>/spec-<YYYY-MM-DD>.md` 저장.
- Progress 업데이트.

---

## Phase 3: PRD 초안 생성

### 실행
1. `.draft/features/<기능명>/idea-*.md` + `spec-*.md` 통합.
2. `.claude/templates/feature-prd-template.md` 를 로드하여 빈칸 채움.
3. `.draft/features/<기능명>/prd-draft.md` 에 저장.

### 매핑 가이드

| prd-template 섹션 | 소스 |
|---|---|
| 1. 한 줄 요약 | spec §1 (가치 제안) |
| 2. 배경 / 문제 정의 | idea Q1, Q2 + spec §1 |
| 3. 타겟 사용자 | idea Q3 |
| 4. 사용자 스토리 | spec §1, §2 기반 3~5개 도출 |
| 5. 기능 범위 | spec §5 |
| 6. 성공 지표 | spec §4 |
| 7. 플로우 / UX 요구사항 | spec §2 + 사용자 스토리 기반 |
| 8. 비기능 요구사항 | plan.md §6 기술 스택 + 이 기능 특성 |
| 9. 의존성 / 전제 조건 | plan.md 기반 도출 + idea/spec 대화에서 나온 제약 |
| 10. 리스크 + 대응책 | spec §4 실패 신호 기반 도출 |

채우지 못한 섹션은 `<TBD: 이유>` 로 명시하고 사용자에게 확인 요청.

---

## Phase 4: @plan-reviewer 검토

Agent tool로 `plan-reviewer` 서브에이전트 호출:

```
Agent(
  subagent_type: "plan-reviewer",
  description: "기능 PRD 배치 검토",
  prompt: `
다음 파일을 Feature PRD 문서 종류로 검토하고 Plan Review Report를 반환하라.
사용자와 대화하지 말고 리포트만 반환하라.

- .draft/features/<기능명>/prd-draft.md

참조할 맥락:
- docs/plan.md (프로덕트 전체)
- docs/context.md (결정 기록)
- CLAUDE.md (프로젝트 원칙)

체크 포인트:
1. 스킬 지침의 Feature PRD 체크리스트
2. 프로덕트 plan과의 정합성 (차별점/타겟/로드맵)
3. 비기능/의존성/리스크 빠진 것 없는지
`
)
```

---

## Phase 5: 컨펌 + 확정

### 5-1. 사용자 컨펌 루프

1. 리뷰 리포트를 사용자에게 제시.
2. 선택지:
   - **A. 그대로 확정**: 5-2로 진행
   - **B. 특정 섹션 수정**: 해당 섹션 대화로 수정 → prd-draft 갱신 → Phase 4 재실행
   - **C. Phase 재실행**: Phase 1 또는 2부터 재시작
   - **D. 취소**: draft 유지, 중단
3. A 선택까지 루프.

### 5-2. 확정 (draft → docs 이동)

1. `docs/features/<기능명>/` 디렉토리 생성 (없으면).
2. `.draft/features/<기능명>/prd-draft.md` → `docs/features/<기능명>/prd.md` 이동.
3. `docs/tasks.md` 업데이트:
   - **"진행 상태 요약"** 아래에 **"현재 작업 중 기능: <기능명>"** 추가 (또는 업데이트).
   - **기능이 속할 Phase를 먼저 판단**:
     - `docs/plan.md` §7 로드맵에서 해당 기능이 언급된 Phase 식별 (예: "Phase 1 MVP", "Phase 2")
     - 모호하거나 명시 없으면 사용자에게 확인: **"이 기능은 어느 Phase에 속하나요? (1/2/3 또는 백로그)"**
   - 식별된 Phase 아래에 `### 기능: <기능명>` 서브섹션 추가. 비워둠 (Task 분해는 `/feature-plan`에서).
4. Progress 파일에 완료 스탬프.
5. 사용자에게 완료 요약:

```
✅ Feature PRD 완료 — <기능명>

생성: docs/features/<기능명>/prd.md
참조: .draft/features/<기능명>/ (idea + spec 원본 보존)

이어서 기술 설계도 진행하시겠어요?
- "네" / "Y" / "진행" → /feature-plan <기능명> 바로 실행
- "나중에" / "N" → 종료 (나중에 /feature-plan <기능명> 으로 재개)

또는 다른 선택:
- 다른 기능 기획: /feature-start <다른기능명>
- 상황 확인: /project-status
```

### 5-3. 자동 연속 호출 메커니즘

사용자가 "네/Y/진행" 응답 시:
1. `.claude/commands/feature-plan.md` 를 Read 도구로 로드.
2. 그 파일의 Phase 0~5 지시를 **현재 세션 내에서 이어서 수행**한다.
3. `<기능명>` 플레이스홀더는 현재 기능명으로 치환.
4. 사용자는 재입력 불필요.

> 만약 동작이 이상하면 사용자가 직접 `/feature-plan <기능명>` 을 입력해도 된다.

---

## 중단 / 재시작

- 중간 중단 시 `.draft/features/<기능명>/` 에 현재까지 산출물 저장 + progress 업데이트.
- 재실행 시 progress 읽고 이어서.

---

## 핵심 원칙 (위반 금지)

1. **docs/features/<기능명>/** 는 Phase 5-2 확정 시에만 쓰기.
2. Skill은 Skill tool로 로드.
3. @plan-reviewer는 배치.
4. 사용자 최종 결정권자.
5. 프로덕트 plan/context와 충돌하면 중단하고 사용자 확인.
