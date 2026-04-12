# CLAUDE.md - DataPilot

## 프로젝트 개요

DataPilot은 게임 PM용 멀티 에이전트 KPI 진단 코파일럿이다.

## 기술 스택

- Python 3.13 + uv
- LangChain (ChatAnthropic + ChatPromptTemplate + bind_tools)
- Claude Sonnet 4 + Opus 4
- DuckDB (내장 Mock DB)
- Streamlit + Streamlit Community Cloud

## 문서 인덱스

| 목적 | 파일 |
|---|---|
| 프로덕트 기획서 | `docs/datapilot-planning.md` |
| Mock 데이터 계약 | `docs/mock-data-schema.md` |
| 화면 설계 (시각) | `docs/wireframe.html` |
| 화면 설계 (명세) | `docs/screen-spec.md` |
| 에이전트 구현 명세 | `docs/agents/01-07-*.md` |
| 리뷰 이력 | `docs/review-log.md` |

현재 Phase와 진행 상황은 `datapilot-planning.md` 하단 "진행 관리" 섹션 참조.

---

## 공통 규칙

### 코드 작업

- 모든 파일 생성/수정/삭제는 Edit 도구 사용 (IDE diff 확인)
- Python 3.13 문법 기준

### Git 워크플로우

- **push는 사용자만 수동으로**. 에이전트 절대 push 금지.
- 작업 단위로 커밋, 메시지는 사용자 승인 후 실행

### 작성 원칙

- 담백한 톤
- PM 용어 통일 (PD/PO 혼용 X)
- 문서 수정 시 노션 ↔ 기획문서 동기화

---

## 서브 에이전트

| 에이전트 | 역할 | 호출 시점 |
|---|---|---|
| @ux | 화면 설계 + UX 리뷰 | 설계 단계, UI 구현 후 |
| @reviewer | CTO 관점 코드 리뷰 | 구현 완료 후 |
| @tester | 유닛/통합 테스트 | 구현 완료 후 |
| @security | 보안 검토 | LLM 코드, Tool Use 시 |
| @infra | 환경/배포 | 환경/배포 작업 시 |

### 호출 규칙

- 독립 작업은 병렬 호출
- 서브에이전트는 자기 역할 범위를 벗어나지 않음
- Edit/Write 쓰는 에이전트(@doc, @tester, @infra)는 포그라운드 실행 (diff 확인 필요)

---

## 작업 사이클

각 Phase 구현 완료 후 공통으로 따르는 순서.

1. 구현 (Edit/Write)
2. @tester 호출 → 단위/통합 테스트 작성 + pytest 초록불 확인
3. @reviewer + @security 호출 (reviewer는 항상, security는 조건부 · 병렬 가능)
   - @security 호출 조건 (아래 중 하나라도 해당 시)
     - LLM 호출 코드
     - Tool Use / LLM 생성 SQL 실행
     - 시크릿 / 환경변수 로딩
     - 사용자 입력 파싱
4. 리뷰/보안 피드백 Critical·Warning 반영
5. 커밋
6. `datapilot-planning.md` Phase 체크박스 업데이트

Phase별 상세 작업 사이클은 `docs/datapilot-planning.md` 의 "진행 관리" 섹션 참조.

주의:
- push는 사용자 수동 (공통 규칙 참조)
- @ux / @infra는 Phase 전용이므로 이 사이클과 별도

---

## 에이전트 구현 원칙 (Phase 6)

DataPilot 6종 에이전트 구현 시 반드시 따라야 할 원칙.

### 판단은 전적으로 AI

- 코드는 raw 데이터 조회만 담당
- 변화율 계산, 임계값 필터링, 패턴 식별은 프롬프트로 AI에게 위임
- 코드가 사전 필터링하면 "AI를 반만 쓰는 것"

### Data Validator 전용 규칙

- 3상태 검증: supported / rejected / unverified 분리
- unverified는 본문 아닌 "추가 검토 필요"로
- Tool Use 4중 방어: SELECT only / 위험 키워드 블랙리스트 / 테이블 화이트리스트 / 읽기 전용 연결

### Root Cause Reasoner 전용 규칙

- 각 인과 단계에 근거 SQL 인용
- supported 0개면 억지 체인 X → "원인 불명" 선언
- unverified는 인과 체인에서 제외

---

## 다음 세션 시작 가이드

1. 이 CLAUDE.md 확인
2. `docs/datapilot-planning.md` 하단 Phase 체크리스트 확인
3. ⏳ 인 Phase부터 진행
4. 완료 시 해당 서브에이전트 호출로 품질 검증
