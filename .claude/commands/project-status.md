---
description: 프로젝트 현재 상태 요약 — 3문서 + features/ + 최근 git 활동 기반
---

# /project-status — 프로젝트 상태 요약

당신(메인 AI)은 현재 프로젝트 상태를 **한눈에 파악 가능한 구조화된 요약**으로 출력한다.

**언제 쓰나**:
- 오랜만에 돌아와서 "어디까지 왔지?" 빠르게 파악할 때
- 새 대화 세션 시작 시 맥락 잡기
- 회고 / 리뷰 전 준비
- 블로커·미결 항목 확인

---

## 전제 조건

- 프로젝트 루트에서 실행.
- `docs/plan.md` 존재 필수 (없으면 "`/kickoff`가 먼저 필요하다"고 안내 후 중단).
- 파일을 **수정하지 않는다**. 읽기 전용 리포트만 생성.

---

## Phase 0: 선행 체크

1. `docs/plan.md` 존재 확인. 없으면 중단 + `/kickoff` 안내.
2. `docs/context.md`, `docs/tasks.md`, `CLAUDE.md` 존재 확인. 빠진 것은 리포트에 경고로 표시.
3. `.git` 존재 확인. 없으면 git 관련 섹션은 생략.

---

## Phase 1: 정보 수집 (병렬 Read)

다음 파일들을 Read로 로드:
- `docs/plan.md` — 프로덕트 전체 전략
- `docs/context.md` — 최근 ADR, Implementation Snapshot
- `docs/tasks.md` — Phase 진행 상태, 진행 중 항목
- `docs/features/*/prd.md` — 활성 기능들의 PRD (Glob으로 탐색)
- `CLAUDE.md` — 프로젝트 원칙 (필요 시)

추가 (선택):
- `.draft/kickoff-progress.md` — 킥오프 중단 상태 있으면 감지
- `.draft/features/*/feature-*-progress.md` — 기능 작업 중단 상태
- `.claude/state/security-audit.json` — 보안 감사 이력 (있으면)
- `.claude/state/cleanup.json` — cleanup 이력 (있으면)
- `.claude/settings.json` — `reminders.*_interval_days` 기준 로드

---

## Phase 2: git 활동 수집

`.git` 존재 시 Bash로:

```bash
git log --oneline -10                    # 최근 커밋 10개
git status --short                        # 변경사항 요약
git branch --show-current                 # 현재 브랜치
git diff --stat HEAD~5..HEAD 2>/dev/null  # 최근 5커밋 변경 규모
```

없으면 해당 섹션 생략.

---

## Phase 3: 요약 출력

다음 형식으로 구조화해서 출력:

```markdown
# 📊 Project Status — <YYYY-MM-DD HH:MM>

## 📌 프로젝트
- **이름**: <plan.md §1>
- **한 줄 설명**: <plan.md §1>
- **현재 Phase**: <plan.md §1 현재 Phase>
- **마지막 업데이트**: <context.md Last Updated>

## 🎯 현재 초점

### 진행 중 작업
<tasks.md "현재 Phase" + "이번 주 목표" + 진행 중 ⏳ 항목>

### 활성 기능
- **<기능명>** [PRD 완료 / tech-spec 완료 / 구현 중 / 완료]
  <prd.md §1 한 줄 요약>

## 📝 최근 결정 (ADR — 최근 3~5개)

- **<날짜> [태그]** <결정 요약>
  <context.md에서 최근 ADR 추출>

## ⚠️ 블로커 / 미결

### Blockers
<context.md Implementation Snapshot 의 현재 블로커>

### 미결 항목
<tasks.md 백로그 중 우선순위 높은 것 3~5개>

## 📈 최근 활동 (git)

### 최근 커밋
<git log 10개>

### 변경 파일 요약
<git status --short>

### 현재 브랜치
<git branch>

## 🔄 중단된 작업 (있으면)

<.draft/kickoff-progress.md 또는 features/*/feature-*-progress.md 감지 시>
- **<작업 유형>**: <마지막 완료 Phase> — 재개: `/<커맨드>` 실행

## 🧭 추천 다음 행동

상황 기반으로 1~3개 제안:
- <예: "`/feature-plan signup` 으로 기술 설계 진행">
- <예: "tasks.md 의 리뷰 발견사항 C1 수정">
- <예: "`/update-docs` 로 Implementation Snapshot 갱신 (마지막 업데이트 후 N일 경과)">

## 🔒 보안 감사 상태

`.claude/state/security-audit.json` 있으면:
- 마지막 감사: <date> (N일 전) <🟢 / 🟡 / 🔴>
- 권장 주기: <settings.json reminders.security_audit_interval_days 또는 30>일
- 최근 감사 범위: <scope>
- 최근 발견: High N / Medium M / Low K

민감 기능 변경 이력 (감사 이후, `sensitive_changes_since_last_audit`):
- <YYYY-MM-DD> - 기능 <X>
- <YYYY-MM-DD> - 기능 <Y>

없으면:
- "아직 보안 감사 이력이 없습니다. `/security-audit` 로 첫 감사를 진행하세요."

<보안 감사 신선도 기준>
- 🟢 주기 내 / 민감 변경 없음
- 🟡 주기 초과 또는 민감 변경 1~2건 있지만 주기 내
- 🔴 주기 초과 + 민감 변경 있음 → `/security-audit <영역>` 강력 권장

## 🧹 기술 부채 상태 (cleanup)

`.claude/state/cleanup.json` 있으면:
- 마지막 cleanup: <date> (N일 전) <🟢 / 🟡 / 🔴>
- 권장 주기: <settings.json reminders.cleanup_interval_days 또는 45>일
- 직전 발견: stale TODO M / orphan K / large files L / empty dirs P
- 직전 처리: 삭제 X개, tasks 등록 Y개, 남은 항목 Z개

없으면:
- "cleanup 이력 없음. `/cleanup` 으로 첫 탐지 실행 권장."

<cleanup 신선도 기준>
- 🟢 주기 내
- 🟡 주기 초과 ~ 2배 이내 (`/cleanup` 권장)
- 🔴 2배 초과 (강력 권장 / `/ship --strict` 에서 차단)

## 🩺 문서 신선도

- plan.md Last Updated: <날짜> (N일 전)
- context.md Last Updated: <날짜> (N일 전)
- tasks.md Last Updated: <날짜> (N일 전)

<문서 신선도 기준>
- 🟢 7일 이내: 최신
- 🟡 8~30일: 갱신 권장
- 🔴 30일 초과: 대대적 갱신 필요 (`/update-docs` 실행 강력 권장)
```

---

## 후속 대화

리포트 출력 후 자연 대화로 이어진다. 예:
- "<기능명> 더 자세히" → 해당 기능 PRD/tech-spec 요약 추가 제시
- "<ADR> 더 설명해" → context.md 해당 결정 상세 전달
- "tasks.md 전체 보여줘" → 파일 원본 출력
- "/update-docs" → 문서 갱신으로 전환

---

## 핵심 원칙

1. **읽기 전용**: 어떤 파일도 수정하지 않는다.
2. **구조 일관**: 위 출력 형식을 지켜 예측 가능하게.
3. **정보 농축**: 원문 복붙 X, 요약 / 발췌. 전체 원문 필요 시 사용자가 후속 요청.
4. **경고 명시**: 빠진 문서 / 오래된 문서 / 중단된 작업은 눈에 띄게 표시.
5. **추천은 구체적**: "뭔가 하세요" X, "X 커맨드로 Y를 해보세요" ✅

---

## 실패 시나리오

| 상황 | 대응 |
|---|---|
| docs/ 전체 없음 | `/kickoff` 안내 후 종료 |
| plan.md는 있는데 context/tasks 없음 | 있는 것만으로 부분 리포트 + 누락 경고 |
| .git 없음 | git 섹션 생략, 나머지 출력 |
| features/ 비어있음 | "활성 기능 없음" 표시 |
| context.md에 Last Updated 없음 | "날짜 정보 없음 — `/update-docs` 권장" |
