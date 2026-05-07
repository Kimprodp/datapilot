# CLAUDE.md

이 파일은 Claude Code에게 프로젝트 맥락을 전달합니다.
**모든 작업 시작 전 반드시 읽고 준수하세요.**

---

## 📌 프로젝트 개요

- **이름**: DataPilot
- **한 줄 설명**: 게임 PM 에게 KPI 이상 감지 → 원인 분석 → 액션 결정 사이클을 1~3일 → 몇 분으로 단축해주는 AI 멀티 에이전트 진단 코파일럿
- **현재 Phase**: Beta (Phase 1~10 완료, 배포됨)

> 상세 배경/전략: [`docs/plan.md`](docs/plan.md)

---

## 🗂️ 문서 구조

### 프로덕트 레벨 3대 문서 (필수 참조)

| 문서 | 역할 | 경로 |
|---|---|---|
| **계획서** | 왜/무엇을 (설계도) | [`docs/plan.md`](docs/plan.md) |
| **맥락 노트** | 결정의 이유 (시방서) | [`docs/context.md`](docs/context.md) |
| **체크리스트** | 지금 할 일 (공정표) | [`docs/tasks.md`](docs/tasks.md) |

### 기능별 문서

각 기능은 `docs/features/<기능명>/` 아래에 2개 파일.

| 파일 | 역할 | 생성 시점 |
|---|---|---|
| `prd.md` | 기획 (무엇/왜) | `/feature-start` 실행 시 |
| `tech-spec.md` | 기술 설계 (어떻게) | `/feature-plan` 실행 시 |

### DataPilot 도메인 스펙 (현역, 코드와 함께 갱신)

| 파일 | 연결된 코드 |
|---|---|
| [`docs/mock-data-schema.md`](docs/mock-data-schema.md) | `scripts/seed_mock_data.py`, `data/datapilot_mock.db` |
| [`docs/screen-spec.md`](docs/screen-spec.md) | `app.py` |
| [`docs/wireframe.html`](docs/wireframe.html) | `app.py` (시각적 참조) |
| `docs/agents/01-06-*.md` | `datapilot/agents/*.py` (6종) |
| `docs/agents/07-reviewer.md` | (Phase 2 구현 예정) |

### 이전 환경 이력 (Harness 도입 전)

`docs/archive/` 하위에 5개 파일 보관. **더 이상 갱신 X, 참조용**.
신규 결정은 `docs/context.md` ADR 섹션에 기록.

### 문서 정책

- **`docs/` 폴더는 git 추적하지 않는다** (노션이 PM 기획 SoT). Harness 3축 문서 포함 모든 `docs/*` 는 로컬 워킹 카피.

---

## 🛠️ 기술 스택

- **언어**: Python 3.13 + uv
- **UI**: Streamlit
- **배포**: Streamlit Community Cloud — https://datapilot-ops.streamlit.app
- **LLM**: Claude Sonnet 4 + Opus 4 (에이전트별 분배)
- **AI 프레임워크**: LangChain (`ChatAnthropic` + `ChatPromptTemplate` + `bind_tools`)
- **데이터베이스**: DuckDB (내장, 데모) / BigQuery (Adapter 스텁, 운영 가정)
- **아키텍처**: Port/Adapter

---

## 🚦 작업 워크플로우

### 새 기능 개발

```
/feature-start <기능명>    → docs/features/<name>/prd.md 생성
   ↓
/feature-plan <기능명>     → docs/features/<name>/tech-spec.md 생성
                              + tasks.md 에 Task 분해 추가
   ↓
/task                      → 다음 task 진입 (맥락 자동 로드)
   ↓ [구현 대화]
   ↓ 완료 선언 → tasks.md 체크오프
   ↓ 다음 /task 반복
   ↓
/code-review               → 기능 전체 리뷰 (@reviewer / @security 자동 호출)
   ↓
/update-docs               → ADR 추가 + Implementation Snapshot 갱신
```

### 유지관리

```
/project-status  → 지금 어디까지 왔는지 확인
/update-docs     → 3축 문서 최신화
```

### 서브에이전트

| 에이전트 | 역할 | 호출 시점 |
|---|---|---|
| `@plan-reviewer` | 기획/설계 문서 검토 | `/kickoff`, `/feature-start`, `/feature-plan` 내부 자동 |
| `@reviewer` | 코드 리뷰 | `/code-review` 내부 자동 |
| `@security` | 보안 심화 감사 | `/code-review` 보안 옵션 또는 LLM/Tool Use 코드 감지 시 |
| `@tester` | 단위/통합 테스트 작성 (도메인 보존) | 구현 완료 후 (Edit 권한, 포그라운드) |
| `@ux` | 화면 설계 + UX 리뷰 (도메인 보존) | 설계 단계, UI 구현 후 |
| `@infra` | 환경/배포 (도메인 보존) | 환경/배포 작업 시 (Edit 권한, 포그라운드) |

호출 규칙:
- 독립 작업은 한 메시지에 병렬 호출
- 서브에이전트는 자기 역할 범위를 벗어나지 않음
- Edit/Write 쓰는 에이전트(`@tester`, `@infra`)는 포그라운드 실행 (diff 확인)

---

## ⚠️ 핵심 원칙

### Git 워크플로우

- **push 는 사용자만 수동.** 에이전트 절대 push 금지.
- 커밋 메시지 작성 전 반드시 `git status` + `git diff --stat` 으로 워크트리 확인
- 커밋 메시지에 **Phase 번호(Phase n) 사용 금지** — 기획문서 전용. 이슈/브랜치/커밋 메시지에서 X
- 작업분이 많으면 논리 단위로 커밋 분리
- 메시지는 사용자 승인 후 실행

### 작성 원칙

- 담백한 톤, AI 티 나는 과장 금지
- PM 용어 통일 (PD/PO 혼용 X)
- 중요한 결정은 `docs/context.md` 에 ADR 형식으로 기록
- 완료된 작업은 `docs/tasks.md` 에서 `[x]` 체크 + "완료된 기능" 섹션으로 이동
- 각 작업 항목은 **Effort (S/M/L/XL) + Acceptance Criteria** 반드시 포함
- 긴 작업 종료 시 `/update-docs` 로 `context.md` 의 Implementation Snapshot 갱신

### 에이전트 구현 원칙 (DataPilot 6종 에이전트 전용)

#### 판단은 전적으로 AI

- 코드는 raw 데이터 조회만 담당
- 변화율 계산, 임계값 필터링, 패턴 식별은 프롬프트로 AI 에게 위임
- 코드가 사전 필터링하면 "AI 를 반만 쓰는 것"

#### Data Validator 전용 규칙

- **3 상태 검증**: supported / rejected / unverified 분리
- unverified 는 본문 X, "추가 검토 필요" 로 분리
- **Tool Use 4 중 방어**: SELECT only / 위험 키워드 블랙리스트 / 테이블 화이트리스트 / 읽기 전용 연결

#### Root Cause Reasoner 전용 규칙

- 각 인과 단계에 근거 SQL 인용
- supported 0 개면 억지 체인 X → "원인 불명" 선언
- unverified 는 인과 체인에서 제외

---

## 📖 도메인 용어

| 용어 | 정의 |
|---|---|
| KPI | DAU/MAU/ARPPU/매출/리텐션/세션수/결제성공률 등 |
| segmentable | 세그먼트 차원(platform/country/user_type/device)으로 분해 가능한 이상 지표 |
| anomaly | 시계열에서 정상 변동성을 벗어난 이상 지표 (severity: high/medium/low) |
| supported / rejected / unverified | Data Validator 의 3 상태 검증 결과 |
| 인과 체인 | Root Cause Reasoner 가 supported 가설들을 X → Y → Z 로 묶은 결과 |
| Tool Use | LangChain `bind_tools` 패턴 (Data Validator 만 사용) |
| 4 중 방어 | LLM-generated SQL 안전성 보장 메커니즘 (Data Validator 전용) |

> 전체 용어집: [`docs/context.md`](docs/context.md) "도메인 용어집" 섹션

---

## 🔗 참고

- **노션 SoT**: https://www.notion.so/Harness-AI-349be1faf6028141b14bcaa8a00796db
- **배포 URL**: https://datapilot-ops.streamlit.app
- **GitHub**: https://github.com/Kimprodp/datapilot
- **하니스 기반**: 로컬 `~/harness/`
