---
description: 기능 기술 설계 (tech-spec.md 작성) — tech-spec 스킬 + @plan-reviewer 검토 + tasks.md Task 분해
argument-hint: <기능명>
---

# /feature-plan — 기능 기술 설계

당신(메인 AI)은 PRD가 작성된 기능의 **기술 설계 문서**(tech-spec.md)를 만든다.
`tech-spec` 스킬을 따라 4개 섹션을 순차 수행하고, `@plan-reviewer`에게 검토 후 사용자 컨펌을 받아 `docs/features/<기능명>/tech-spec.md`로 확정한다.
마지막으로 `docs/tasks.md`에 구현 Task를 분해하여 추가한다.

**인자**: 기능명 (PRD가 이미 있어야 함)

---

## 전제 조건

- **프로젝트 루트에서 실행**.
- `docs/features/<기능명>/prd.md` 존재 필수 (없으면 `/feature-start <기능명>` 먼저 안내하고 중단).
- `docs/plan.md`, `docs/context.md`, `CLAUDE.md` 존재 필수 (맥락 필요).
- 사용자와 **직접 대화** (서브에이전트로 인터뷰 금지).
- 말투 안티패턴 금지.

---

## Phase 0: 선행 체크 + 맥락 로드

### 0-1. 맥락 로드
- Read: `docs/features/<기능명>/prd.md` (필수)
- Read: `docs/plan.md`, `docs/context.md`, `CLAUDE.md`
- PRD를 요약하여 사용자에게 1~2문장으로 "이 기능 설계를 시작한다"는 확인 출력.

### 0-2. 기존 design 체크

- `docs/features/<기능명>/tech-spec.md` 존재?
  → 사용자에게: **"design이 이미 있다. 어떻게 할까?"**
    - A. 덮어쓰기 (기존은 `design.backup-<timestamp>.md`로)
    - B. 취소
- `.draft/features/<기능명>/feature-plan-progress.md` 존재? → 이어서 진행 여부 확인.

### 0-3. 작업 공간 준비

- `.draft/features/<기능명>/` 보장.
- Progress 파일 초기화:

```markdown
# Feature Plan Progress — <기능명>
- **Started**: YYYY-MM-DD HH:MM
- [ ] Step 0 — Scope Challenge
- [ ] Section 1 — Architecture
- [ ] Section 2 — Code Quality
- [ ] Section 3 — Tests (Coverage Diagram)
- [ ] Section 4 — Performance
- [ ] Outside Voice (선택)
- [ ] Design 생성
- [ ] 검토 + 확정
- [ ] tasks.md 분해 추가
```

---

## Phase 1: tech-spec 스킬 수행

### 1-0. Skill 로드
**Skill tool로 `tech-spec` 스킬 로드**.

### 1-1. Step 0 — Scope Challenge (필수)

- 복잡도 체크 (파일 8+ 또는 새 클래스/서비스 2+ 이면 축소 제안).
- 기존 코드 재활용 가능성 감사.
- 복잡도 초과 시 사용자에게 **A. 진행 / B. 축소 / C. 단계적 도입** 선택지 제시.

### 1-2. Section 1 — Architecture Review

평가 항목은 tech-spec 스킬 지침 참조. 필수 산출물:
- 비자명 플로우 ASCII 다이어그램
- 새 코드 경로당 **실제 프로덕션 실패 1개** 식별 + 처리 여부

**섹션 끝**: 발견 이슈를 **개별적으로** 사용자 확인 (일괄 질문 금지). 모두 해결 후 Section 2.

### 1-3. Section 2 — Code Quality Review

- DRY 공격적 플래그.
- 오래된 ASCII 다이어그램 업데이트 확인.
- 섹션 끝 개별 확인.

### 1-4. Section 3 — Test Review (가장 깊음)

**Coverage Diagram 필수 생성**:
- 3-1 실행 추적 (분기/에러 경로 전부 ASCII로)
- 3-2 사용자 플로우 & 인터랙션 엣지 케이스
- 3-3 커버리지 점수 (★★★/★★/★)
- 3-4 갭 식별 (unit/integration/E2E/eval)
- 3-5 테스트 계획 산출

**회귀(regression) 테스트는 기본 포함**. AskUserQuestion 없이 자동 추가.

### 1-5. Section 4 — Performance Review

N+1 / 메모리 / 캐싱 / 느린 경로 / 인덱스 / 예상 트래픽.

### 1-6. Outside Voice (선택)

섹션 4까지 통과 후 사용자에게 제시:
```
추천: A (독립 2차 의견은 구조적 맹점을 잡는다).
A. Outside Voice 받기 (Codex 또는 Claude 서브에이전트로 독립 챌린지)
B. 건너뛰기
```

A 선택 시:
- 가능하면 독립 모델로 동일 PRD + 초안 design을 챌린지.
- 결과를 원본 그대로 제시.
- Cross-model tension이 있으면 **개별 AskUserQuestion**으로 해결.
- Outside Voice 의견 **자동 반영 금지**.

---

## Phase 2: tech-spec.md 초안 생성

### 실행
1. Phase 1에서 수집된 모든 정보 통합.
2. `.claude/templates/feature-tech-spec-template.md` 로드하여 빈칸 채움.
3. tech-spec 스킬 고유 섹션 추가:
   - Scope Challenge 결과
   - Coverage Diagram (ASCII)
   - Failure Modes Registry (표)
   - Parallelization Strategy (표)
   - Outside Voice 결과 (했을 경우)
   - Completion Summary (섹션별 발견 건수)
4. `.draft/features/<기능명>/tech-spec-draft.md` 에 저장.

### 필수 산출물 섹션 (design 초안에 모두 포함)

1. **NOT in scope** — 명시 제외 + 근거
2. **What already exists** — 재활용 / 불필요 재구현 코드
3. **Test coverage diagram** — ASCII
4. **Failure modes** — 새 경로당 실패 + 처리/테스트/가시성
5. **Parallelization strategy** — 의존성 테이블 + 레인 + 실행 순서
6. **Completion summary**

---

## Phase 3: @plan-reviewer 검토

Agent tool 호출:

```
Agent(
  subagent_type: "plan-reviewer",
  description: "기능 기술 설계 배치 검토",
  prompt: `
다음 파일을 Feature Design 문서 종류로 검토하고 Plan Review Report를 반환하라.
사용자와 대화하지 말고 리포트만 반환하라.

- .draft/features/<기능명>/tech-spec-draft.md

참조 맥락:
- docs/features/<기능명>/prd.md (범위 일관성 확인용)
- docs/plan.md §6 (기술 스택 일관성)
- docs/context.md (ADR과 충돌 확인)

체크 포인트:
1. 스킬 지침의 Feature Design 체크리스트
2. PRD 범위를 벗어나지 않는가
3. 필수 6개 산출물 섹션 완비
4. Coverage Diagram 실제로 경로를 다루는가
5. Failure Modes가 "실제 프로덕션"에서 일어날 일인가
`
)
```

---

## Phase 4: 컨펌 + 확정

### 4-1. 사용자 컨펌 루프

1. 리뷰 리포트 제시.
2. 선택지:
   - **A. 그대로 확정**: 4-2로 진행
   - **B. 특정 섹션 수정**: 대화로 수정 → design-draft 갱신 → Phase 3 재실행
   - **C. 특정 Section 재실행**: Phase 1의 Section 1~4 중 하나를 재시작
   - **D. 취소**: draft 유지, 중단
3. A 선택까지 루프.

### 4-2. 확정 (draft → docs 이동)

1. `.draft/features/<기능명>/tech-spec-draft.md` → `docs/features/<기능명>/tech-spec.md`.
2. Progress 파일에 완료 스탬프.

---

## Phase 5: tasks.md 에 Task 분해 추가

### 실행
1. `tech-spec.md` 의 "Parallelization Strategy" + "Section 3 테스트 계획" 을 기반으로 구체 Task 도출.
2. 각 Task에 **Effort (S/M/L/XL) + AC (Acceptance Criteria)** 부여.
3. `docs/tasks.md` 의 Phase 1 아래 해당 기능 서브섹션에 추가:

### 형식

```markdown
### 기능: <기능명>

#### 구현
- [ ] **<작업 제목>** [Effort]
  - AC: <측정 가능한 완료 기준>
  - AC: <...>
  - 의존: <선행 작업 / 외부 서비스> (있으면)

#### 테스트
- [ ] **<테스트 작성 작업>** [Effort]
  - AC: <tech-spec의 테스트 계획에 적힌 커버리지 달성>

#### 문서
- [ ] **<문서 갱신 작업>** [S]
  - AC: context.md ADR 업데이트 / 회고 노트 등
```

### 산출
사용자에게 완료 요약:

```
✅ Feature Design 완료 — <기능명>

생성:
- docs/features/<기능명>/tech-spec.md
- docs/tasks.md에 <N>개 Task 추가 (Phase 1 > 기능: <기능명>)

참조: .draft/features/<기능명>/ (진행 과정 원본 보존)

이어서 구현을 시작하시겠어요?
- "네" / "Y" / "진행" → /task 실행 (첫 미완료 task 맥락 로드)
- "나중에" / "N" → 종료 (나중에 /task 로 재개)

또는 다른 선택:
- 구현 완료 후: /code-review
  → 인증/결제/외부 API 관련이면 "보안도 같이 봐줘"라고 말하거나
    `/code-review 보안까지` 로 실행 (보안 심화 감사 포함)
- 상황 확인: /project-status
```

### 자동 연속 호출 메커니즘

사용자가 "네/Y/진행" 응답 시:
1. `.claude/commands/task.md` 를 Read 도구로 로드.
2. 그 파일의 Phase 지시를 **현재 세션 내에서 이어서 수행**한다.
3. 사용자는 재입력 불필요.

> 만약 동작이 이상하면 사용자가 직접 `/task` 를 입력해도 된다.

---

## 중단 / 재시작

- 섹션 중간 중단 시 `.draft/features/<기능명>/` 에 부분 산출물 저장 + progress 업데이트.
- Section 3 (Tests) 는 특히 길다 — Coverage Diagram 작성 중 중단되면 현재까지의 Diagram을 반드시 저장.
- 재실행 시 progress 읽고 이어서.

---

## 핵심 원칙 (위반 금지)

1. **Sequential + STOP Gates**: Section 1~4는 순차. 이전 섹션 이슈 해결 전 다음 섹션 진행 금지.
2. **개별 AskUserQuestion**: 섹션 내 이슈는 **하나씩** 사용자 확인. 배치 질문 금지.
3. **회귀 테스트는 기본 포함**: 사용자 확인 없이 자동 추가.
4. **Outside Voice는 선택, 자동 반영 금지**.
5. **docs/features/<기능명>/tech-spec.md** 는 Phase 4-2 확정 시에만 쓰기.
6. **tasks.md Phase 5에서만 추가**. 중간에 건드리지 않기.
7. **PRD 범위 벗어난 설계 금지**. 벗어나면 사용자에게 PRD 수정 여부 확인.
