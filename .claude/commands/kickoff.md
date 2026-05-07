---
description: 프로덕트 킥오프 — 5단계 인터뷰로 docs/ 3문서 + CLAUDE.md 생성
---

# /kickoff — 프로덕트 킥오프

당신(메인 AI)은 사용자의 새 프로젝트 킥오프를 진행한다.
5개의 스킬을 순차적으로 로드하고, 각 Phase가 끝날 때마다 산출물을 `.draft/`에 저장한다.
최종적으로 `@plan-reviewer`에게 검토를 맡긴 후, 사용자 컨펌을 받고 `docs/`에 확정한다.

---

## 전제 조건

- **반드시 프로젝트 루트에서 실행**. 루트가 아니면 중단하고 이유 보고.
- 사용자와 **직접 대화**한다. 서브에이전트로 인터뷰 시도 금지 (서브에이전트는 인터랙티브 불가).
- 모든 중간 파일은 `.draft/` 아래에. `docs/`는 **Phase 5의 최종 확정 시에만** 쓰기.
- 말투는 직설적·간결하게. "delve", "crucial", "robust" 같은 기업/학술 표현과 과도한 감탄 금지.

---

## Phase 0: 선행 체크

### 0-1. 프로젝트 루트 확인
- `.git` 디렉토리 존재 여부 확인.
  - 없으면 사용자에게: **"Git 저장소가 초기화되지 않았습니다. 지금 `git init` 을 실행할까요? (권장)"**
  - 승인 시 Bash로 `git init` 실행 후 진행.
  - 거절 시 경고: "이후 `/code-review`, `/project-status` 등은 git 기반이라 제한될 수 있습니다." 보고 후 진행.
- 쓰기 권한 확인.

### 0-2. 기존 상태 감지

- `docs/plan.md` 존재?
  → 사용자에게: **"이미 킥오프가 완료된 프로젝트로 보입니다. 어떻게 할까요?"**
    - A. 덮어쓰기 (기존 docs/ 백업: `docs.backup-<timestamp>/`)
    - B. 취소
    - C. 일부 Phase만 재실행 (어느 Phase?)

- `.draft/kickoff-progress.md` 존재?
  → 파일 읽고 마지막 완료 Phase 확인 후: **"Phase X까지 완료되어 있습니다. Phase X+1부터 이어서 진행할까요? (Y/N)"**

### 0-3. 작업 공간 준비

- `.draft/` 폴더 생성 (없으면).
- `.draft/kickoff-progress.md` 초기화 또는 기존 파일 업데이트.

### `.draft/kickoff-progress.md` 형식

```markdown
# Kickoff Progress

- **Started**: YYYY-MM-DD HH:MM
- **Last Updated**: YYYY-MM-DD HH:MM

## Phases

- [ ] Phase 1 — Idea Validation
- [ ] Phase 2 — Product Spec
- [ ] Phase 3 — Scope Review
- [ ] Phase 4 — Tech Stack Decision
- [ ] Phase 5 — Docs Generation

## Artifacts

- (비어있음. 각 Phase 완료 시 추가)
```

각 Phase 시작 시 해당 항목에 `⏳`, 완료 시 `[x] ✅ <timestamp>` 로 업데이트.
Artifacts 섹션에 생성된 draft 파일 경로 추가.

---

## Phase 1: Idea Validation (허점 발굴 인터뷰)

### 실행
1. **Skill tool로 `idea-validation` 스킬 로드**.
2. 스킬 지침에 따라 사용자와 **6가지 질문** 순차 진행.
3. 모호한 답변엔 **2회 push** (스킬 안티패턴 준수).
4. "The Assignment" (이번 주 실행 과제) 도출.

### 산출
- `.draft/idea-validation-<YYYY-MM-DD>.md` 저장 (스킬의 산출물 형식 따름).
- `kickoff-progress.md` 업데이트:
  - Phase 1 체크
  - Artifacts에 파일 경로 추가

### 종료
사용자에게: **"Phase 1 완료. Phase 2 (Product Spec)로 진행할까요? 중단하고 나중에 이어서도 됩니다."**

---

## Phase 2: Product Spec (솔루션 구체화)

### 실행
1. `.draft/idea-validation-*.md` 를 Read로 읽기 (맥락 확보).
2. **Skill tool로 `product-spec` 스킬 로드**.
3. 7개 섹션 순차 인터뷰:
   - 핵심 가치 제안 / 핵심 기능 / 차별점 / 지표 / 범위 / 접근 비교 / 전제(Premises)
4. 각 섹션 완료 시 사용자 요약 확인.

### 산출
- `.draft/product-spec-<YYYY-MM-DD>.md` 저장.
- Progress 업데이트.

---

## Phase 3: Scope Review (범위 조정)

### 실행
1. `.draft/idea-validation-*.md`, `.draft/product-spec-*.md` 를 Read.
2. **Skill tool로 `scope-review` 스킬 로드**.
3. **사용자에게 모드 선택 요청**:
   - EXPANSION (크게 꿈꾸기)
   - SELECTIVE EXPANSION (기본 유지 + cherry-pick)
   - HOLD (범위 유지, 품질 극대화)
   - REDUCTION (최소로 축소)
4. 선택된 모드로 일관 수행 (모드 드리프트 금지).
5. **Long-Term Trajectory 섹션 필수** (모드 무관).

### 산출
- `.draft/scope-review-<YYYY-MM-DD>.md` 저장.
- Progress 업데이트.

---

## Phase 4: Tech Stack Decision (기술 기반 결정)

### 실행
1. 이전 draft 3개 모두 Read.
2. **Skill tool로 `tech-stack-decision` 스킬 로드**.
3. Step 0 Scope Challenge 먼저 수행 (복잡도 임계 초과 시 축소 제안).
4. 4개 영역 순차 결정:
   - 언어/프레임워크 / 데이터 저장 / 아키텍처 / 배포·운영
5. 외부 의존성 감사 + 리스크+대응책 수립.

### 산출
- `.draft/tech-stack-decision-<YYYY-MM-DD>.md` 저장.
- Progress 업데이트.

---

## Phase 5: 문서 생성 + 검토 + 확정

### 5-1. 초안 생성

4개 draft 산출물을 통합하여 `.claude/templates/` 아래 템플릿 기반으로 다음 4개 파일을 `.draft/final/` 에 생성:

| 생성 파일 | 템플릿 | 채울 내용 소스 |
|---|---|---|
| `.draft/final/plan.md` | `.claude/templates/plan-template.md` | 4개 draft 통합 (§1~§7) |
| `.draft/final/context.md` | `.claude/templates/context-template.md` | product-spec 전제 + scope-review Long-Term + tech-stack ADR + 리스크 |
| `.draft/final/tasks.md` | `.claude/templates/tasks-template.md` | Phase 로드맵 + idea-validation의 "The Assignment" 를 최상단 **[이번 주 실행]** 섹션에 |
| `.draft/final/CLAUDE.md` | `.claude/templates/CLAUDE.md.template` | 프로젝트 개요 + 기술 스택 + 워크플로우 + 원칙 |

### 5-2. @plan-reviewer 검토 (배치)

Agent tool로 `plan-reviewer` 서브에이전트 호출:

```
Agent(
  subagent_type: "plan-reviewer",
  description: "프로덕트 킥오프 문서 배치 검토",
  prompt: `
다음 4개 파일을 Product Kickoff 문서 종류로 검토하고 Plan Review Report를 반환하라.
사용자와 대화하지 말고 리포트만 반환하라.

- .draft/final/plan.md
- .draft/final/context.md
- .draft/final/tasks.md
- .draft/final/CLAUDE.md

체크리스트:
1. 각 문서 개별 검토 (제 스킬 지침의 체크리스트 준수)
2. 교차 검증 (Phase/로드맵/지표/기술 일관성)
3. 발견사항을 🚨/⚠️/💡/✅ 우선순위로 정리
4. 종합 평가 (통과 / 조건부 통과 / 재작성 권장)
`
)
```

### 5-3. 사용자 컨펌 루프

1. 리뷰 리포트를 그대로 사용자에게 제시.
2. 사용자 선택지:
   - **A. 그대로 확정**: 5-4로 진행
   - **B. 특정 섹션 수정**: 해당 draft 파일의 섹션을 사용자 대화로 수정 → 5-1 재생성 (해당 파일만) → 5-2 재실행 (부분)
   - **C. 특정 Phase 재실행**: Phase 1~4 중 하나를 재시작
   - **D. 전면 취소**: `.draft/` 유지 + Kickoff 중단
3. 사용자가 승인(A)할 때까지 루프.

**중요**: 리뷰 의견을 **자동 반영하지 않는다**. 사용자 명시 승인만.

### 5-4. 확정 (draft → docs 이동)

1. Move/Copy:
   - `.draft/final/plan.md` → `docs/plan.md`
   - `.draft/final/context.md` → `docs/context.md`
   - `.draft/final/tasks.md` → `docs/tasks.md`
   - `.draft/final/CLAUDE.md` → `CLAUDE.md` (프로젝트 루트)
2. `kickoff-progress.md`에 **완료 스탬프** 추가.
3. `.draft/` 중간 산출물은 **삭제하지 않고 유지**.
4. 사용자에게 완료 요약:

```
✅ Kickoff 완료 (YYYY-MM-DD HH:MM)

생성된 파일:
- docs/plan.md
- docs/context.md
- docs/tasks.md
- CLAUDE.md (프로젝트 루트)

중간 산출물 (참조용 보존):
- .draft/idea-validation-*.md
- .draft/product-spec-*.md
- .draft/scope-review-*.md
- .draft/tech-stack-decision-*.md

다음 단계:
- 첫 기능 시작: /feature-start <기능명>
- 현재 상황 확인: /project-status
- 3문서 갱신: /update-docs
```

---

## 중단 / 재시작 프로토콜

사용자가 중단을 원하거나 세션이 휴지되면:

1. 현재 Phase의 부분 산출물이 있으면 `.draft/`에 **반드시** 저장.
2. `kickoff-progress.md`에 마지막 상태 기록 (Phase N 진행 중, 완료된 질문 N/6 등 상세).
3. 사용자에게: **"진행 상황이 저장되었다. 언제든 `/kickoff`를 다시 실행하면 이 지점부터 이어진다."**

---

## 핵심 원칙 (위반 금지)

1. **draft 우선**: `docs/`는 Phase 5-4 확정 전까지 건드리지 않는다.
2. **각 Phase 완료 직후 저장**: 중간에 끊겨도 복구 가능.
3. **Skill은 Skill tool로 로드**: 스킬 내용을 수동 복붙 금지.
4. **@plan-reviewer는 배치**: 인터랙티브 대화 시도 금지.
5. **사용자가 최종 결정권자**: 리뷰 의견 자동 반영 금지. 제시 → 승인만 반영.
6. **말투 일관성**: 안티패턴 표현 사용 금지.

---

## 실패 시나리오 대응

| 상황 | 대응 |
|---|---|
| 사용자가 중간에 말 없이 중단 | draft 자동 저장 + progress 업데이트 후 안내 |
| 특정 Phase 답변이 모호함 | 스킬 안티패턴 규칙에 따라 2회 push, 그래도 모호하면 "불명확" 플래그 달고 다음 진행 |
| 도중에 아이디어/범위 크게 변경 | 해당 Phase부터 재실행 제안 |
| @plan-reviewer가 🚨 치명 이슈 발견 | 5-3 컨펌 루프에서 해당 섹션 수정 유도 |
| 템플릿 경로를 찾지 못함 | `.claude/templates/` 디렉토리 확인, 없으면 중단 후 "하니스 설치가 올바른지 확인 요청" |
| `.draft/final/` 생성 실패 | 권한/경로 확인 후 사용자에게 보고 |
