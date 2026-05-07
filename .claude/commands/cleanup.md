---
description: 기술 부채 탐지 — 오래된 TODO / 참조 없는 파일 / 큰 파일 / 빈 폴더 스캔 후 정리 제안 (삭제는 승인 후)
argument-hint: [옵션 — --stale-days=N (기본 90), --large-threshold=N (기본 500 줄)]
---

# /cleanup — 기술 부채 탐지 + 정리 제안

당신(메인 AI)은 `.claude/scripts/cleanup/detect.js` 로 프로젝트를 스캔해 **제거·리팩터 후보** 를 찾고, 사용자 승인 후 선별 정리한다.

**언제 쓰나**:
- 정기 정리 (월 1회 / 스프린트 말)
- `/ship` Phase 2 에서 "cleanup 주기 초과" 경고 받았을 때
- 프로젝트가 커지면서 "뭔가 어수선해짐" 느낌
- PR 올리기 전 한번 털어내고 싶을 때

---

## 전제 조건

- `.git` 존재 필수 (git 추적 파일 대상).
- Node.js 필요.
- 코드 수정 X — **탐지 + 제안 + 승인 후 제거** 순서.
- 삭제는 사용자 명시 승인 후. 자동 삭제 절대 금지.

---

## Phase 0: 선행 체크

1. `.claude/scripts/cleanup/detect.js` 존재 확인.
2. `.git` 존재 확인.
3. 인자 파싱 (`--stale-days`, `--large-threshold`).

---

## Phase 1: 탐지 실행

```bash
node .claude/scripts/cleanup/detect.js \
  [--stale-days <기본 90>] \
  [--large-threshold <기본 500>]
```

결과 JSON:
```json
{
  "scannedFiles": 127,
  "thresholds": { "staleDays": 90, "largeThreshold": 500 },
  "staleTodos": [
    { "file": "src/auth.ts", "line": 42, "kind": "TODO", "text": "handle expired token", "ageDays": 124, "commitDate": "2026-01-10" }
  ],
  "orphanFiles": ["src/utils/legacy-helper.ts"],
  "largeFiles": [{ "file": "src/components/Dashboard.tsx", "lines": 812 }],
  "emptyDirs": ["tests/old"]
}
```

---

## Phase 2: 리포트 제시

탐지 결과를 사람이 읽기 좋게 포맷:

```markdown
# 🧹 Cleanup Report — <타임스탬프>

**스캔 범위**: git 추적 파일 N개
**임계**: stale 90일+, large 500줄+

## 1. 오래된 TODO / FIXME (N건)

### 🔴 기각/재평가 대상 (180일 이상)
- `src/auth.ts:42` [TODO 180일] "handle expired token"
- `api/billing.go:98` [FIXME 210일] "rate limiting 미적용"

### 🟡 검토 필요 (90~180일)
- `components/Modal.tsx:15` [TODO 124일] "a11y: focus trap 필요"

→ 제안: 각 항목에 대해 **해결 / task 로 전환 / 주석 삭제** 중 선택

## 2. 참조 없는 파일 후보 (N개)

⚠️ 간이 정적 분석이라 **false positive 가능**. 삭제 전 사용자 확인 필수.

- `src/utils/legacy-helper.ts` — 어떤 import 에도 등장 안 함
- `src/old/deprecated-client.ts` — 주석에 "deprecated" 포함

→ 제안: 실제 미사용 확인 후 삭제. Entry points / CLI / dynamic import 는 탐지 못 함.

## 3. 큰 소스 파일 (500줄 초과, N개)

- `src/components/Dashboard.tsx` — 812 줄
- `src/services/PaymentService.ts` — 634 줄

→ 제안: 파일 쪼개기 / 책임 분리 리팩터 task 추가 고려

## 4. 빈 폴더 (N개)

- `tests/old/`
- `src/legacy/unused/`

→ 제안: 폴더 삭제

---

## 요약
- 🔴 High: N건
- 🟡 Medium: N건
- 💡 정보: N건
- 예상 작업 시간: 약 N분
```

---

## Phase 3: 사용자 선택적 정리

리포트 제시 후 자연 대화로:

```
어떻게 진행할까요?

**일괄 옵션**
- "전부 tasks.md 에 추가"  → 각 항목을 todo 로 등록 (자동 삭제 X)
- "오래된 TODO 만 처리"    → 해당 카테고리만 대화로 하나씩 처리
- "빈 폴더만 지워"         → 위험 낮음, 일괄 삭제 승인

**개별 처리**
- "src/utils/legacy-helper.ts 삭제 확정"  → 해당 파일 삭제 (승인 기반)
- "Dashboard.tsx 리팩터 task 추가"       → tasks.md 항목 생성
- "TODO#1 주석 지워"                    → 해당 라인 주석만 제거

**종료**
- "일단 리포트만 보고 종료"  → 아무것도 안 함
```

### 처리 유형별 대응

| 요청 | 메인 AI 행동 |
|---|---|
| 파일 삭제 확정 | 해당 파일 최종 확인 후 `fs.unlinkSync` (Edit 대신 rm). 커밋은 사용자가 별도 수행 |
| tasks.md 추가 | 해당 Phase 공통 섹션 아래 `#### 정리 (YYYY-MM-DD)` 에 todo 추가 |
| TODO 주석 삭제 | 해당 라인을 Edit 로 제거 (주의: 라인 번호 재확인 필수) |
| 리팩터 task 추가 | "파일 쪼개기: Dashboard.tsx → N개로" 식 todo |

### 삭제 안전 장치

- **여러 파일 일괄 삭제 요청 시** 한 번 더 확인 ("정말 N개 파일 삭제?")
- **.git, node_modules, dist, build** 안의 파일은 탐지 단계에서 이미 제외되지만, 혹시 감지되면 무조건 거부
- 삭제 전 `git status` 로 uncommitted 변경 확인. 있으면 사용자 확인 1회

---

## Phase 4: state 파일 갱신

정리 완료 후 `.claude/state/cleanup.json` 갱신:

```json
{
  "last_cleanup": {
    "date": "<YYYY-MM-DD>",
    "issues_found": {
      "staleTodos": 5,
      "orphanFiles": 2,
      "largeFiles": 3,
      "emptyDirs": 1
    },
    "issues_resolved": {
      "deleted_files": 1,
      "removed_todos": 2,
      "added_to_tasks": 4
    }
  },
  "history": [
    { "date": "<prev>", "found": 10, "resolved": 7 }
  ]
}
```

`auto-skill` Hook 과 `/project-status` 가 이 파일을 참조해 주기 초과 알림·대시보드 표시.

기록 완료 보고:
```
📝 Cleanup 이력 기록됨: .claude/state/cleanup.json

처리 요약:
- 삭제: N개 파일, K개 주석
- tasks.md 등록: M건
- 남은 항목: J건 (다음 cleanup 때 재검토)
```

---

## Phase 5: 후속 제안 (선택)

리팩터 큰 항목이 있으면:
```
💡 큰 파일 리팩터 감지:
   src/components/Dashboard.tsx (812줄)

`/feature-plan refactor-dashboard` 로 정식 리팩터 계획 수립하시겠어요?
```

---

## 핵심 원칙 (위반 금지)

1. **탐지 ≠ 삭제**: Phase 1~2 는 읽기 전용. 삭제는 Phase 3 에서 사용자 명시 승인 후.
2. **False positive 전제**: 참조 없는 파일 탐지는 간이 분석 — 실제 사용 가능성 있다고 명시.
3. **일괄 자동 정리 금지**: "전부 삭제" 는 여러 단계 승인 필수.
4. **.git / node_modules 등 보호**: 탐지에서 자동 제외. 수동 요청이어도 거부.
5. **Git 상태 안전**: uncommitted 변경 있으면 삭제 전 확인.
6. **state 기록 필수**: Phase 4 건너뛰지 말 것 (주기 상기 근거).

---

## 실패 시나리오 대응

| 상황 | 대응 |
|---|---|
| `.git` 없음 | "git 저장소가 아닙니다. cleanup 중단" |
| 스캔 결과 0건 | "클린 상태입니다. 정리할 것 없음." 보고 후 state 만 갱신 |
| 탐지 스크립트 실패 | 에러 보고 + 스킵 (전체 중단 X) |
| 사용자가 대량 삭제 요청 | 파일별 최종 확인 1회 더 |
| false positive 삭제 위험 | "정말 미사용인지 `git grep <파일명>` 해보세요" 제안 |
| 대용량 프로젝트 (수천 파일) 느림 | `git ls-files` 기반이라 정상 — 첫 실행만 오래 걸림 |

---

## `/ship`, `/project-status`, auto-skill 과의 관계

- `/ship` Phase 2-7 에서 cleanup 주기 초과 감지 시 **경고** (차단은 `--strict` 에서만)
- `/project-status` 가 state/cleanup.json 읽어 **🧹 기술 부채** 섹션 표시
- `auto-skill` Hook 이 "청소", "정리", "리팩터" 키워드 또는 주기 초과 시 힌트 주입
