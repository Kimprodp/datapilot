---
name: tech-spec
description: 기능 단위의 상세 기술 설계 스킬. 아키텍처/품질/테스트/성능 4영역을 체계적으로 검토하며, 테스트 커버리지 다이어그램과 실패 모드 레지스트리를 산출한다. /feature-plan에서 사용한다.
keywords: ["기술 설계", "tech spec", "아키텍처 설계", "기능 설계", "failure mode"]
---

# Tech Spec — 기능 단위 기술 설계

> **원본**: [gstack/plan-eng-review](https://github.com/garrytan/gstack) (MIT) — 기능 단위 전체 포팅
> **관련**: 프로덕트 레벨은 `tech-stack-decision` 스킬

## 역할

기능의 PRD(`docs/features/<name>/prd.md`)를 기반으로 **상세 기술 설계**를 작성한다.

**"플랜 모드"**: 구현 전 설계만 리뷰한다. 코드 diff가 아니라 설계 문서를 다룬다.

---

## 핵심 원칙

### 1. Sequential + STOP Gates
4개 섹션을 **순차적**으로 진행하며, 각 섹션 끝에서 사용자 확인 후 다음 섹션.
각 이슈는 **개별** 확인 (배치 질문 금지).

### 2. 완결성 > 단축
AI가 구현 시간을 압축하므로 "해피 패스만" 단축 대신 **전체 커버리지** 선호.

### 3. 회귀(regression)는 필수
기존 코드가 이 변경으로 망가질 가능성 있으면 **반드시 테스트 추가** (선택 사항 아님).

### 4. 다이어그램은 필수
비자명한 플로우는 ASCII 다이어그램 필수.
- 시스템 아키텍처
- 데이터 플로우 (with shadow paths: nil/empty/error)
- 상태 머신
- 에러 플로우
- 배포 순서 / 롤백

---

## Step 0: Scope Challenge

본 섹션 시작 전 범위 검증:

- [ ] **기존 코드 재활용**: 이 문제를 이미 해결하는 코드가 있는가?
- [ ] **최소 변경 식별**: 핵심 목표와 nice-to-have 분리
- [ ] **복잡도 임계값**: 파일 8개 이상 OR 새 클래스/서비스 2개 이상이면 축소 제안
- [ ] **TODOS 교차 참조**: 차단된 의존성 또는 미결 항목 확인
- [ ] **완결성 감사**: "완전한 버전"이 거의 무료라면 단축보다 완결 권장

복잡도 초과 시:
```
이 변경은 <파일 N개 / 새 서비스 M개>로 복잡도가 높다.
A. 그대로 진행 (근거 명시)
B. 범위 축소 (구체 제안)
```

---

## 4개 리뷰 섹션 (순차 + STOP)

### 섹션 1. 아키텍처 리뷰

평가 항목:
- **시스템 설계 / 컴포넌트 경계**
- **의존성 그래프 / 결합도**
- **데이터 플로우 패턴 / 병목**
- **확장 특성 / 단일 실패 지점**
- **보안 아키텍처** (인증, 데이터 접근, API 경계)
- **새 코드 경로별 실제 프로덕션 실패 시나리오**
- **배포 아키텍처** (새 아티팩트 도입 시)

**필수 산출물**:
- 비자명 플로우 ASCII 다이어그램
- **새 경로당 실제 프로덕션 실패 1개** 식별 + 처리 여부

섹션 끝: 발견된 이슈를 **개별적으로** 사용자 확인. 모두 해결 후 다음 섹션.

---

### 섹션 2. 코드 품질 리뷰

평가 항목:
- 코드 조직 / 모듈 구조
- **DRY 위반** (공격적으로 플래그)
- 에러 처리 / 누락된 엣지 케이스
- 기술 부채 핫스팟
- 오버/언더 엔지니어링
- **기존 파일의 오래된 ASCII 다이어그램** (변경의 일부로 업데이트 필수)

섹션 끝: 이슈 개별 확인.

---

### 섹션 3. 테스트 리뷰 (가장 깊음)

**Coverage Diagram 생성 필수.**

#### 3-1. 실행 추적 (Execution Trace)

각 새 기능에 대해 진입점(라우트/함수/이벤트)부터 모든 경로 추적:
- 추가/수정된 모든 함수/메서드
- 모든 조건 분기 (if/else, switch, guard clauses, early returns)
- 모든 에러 경로 (try/catch, rescue, fallback)
- 다른 함수 호출 (재귀적으로 추적)

**ASCII 다이어그램으로 표시**.

#### 3-2. 사용자 플로우 & 인터랙션

- 엔드투엔드 여정 (signup → verify → login)
- 인터랙션 엣지 케이스:
  - 더블 클릭
  - 작업 중 페이지 이탈
  - 오래된 데이터
  - 느린 연결
  - 동시 액션
- 사용자가 볼 수 있는 에러 상태
- 경계 조건 (0 결과, 최대 길이 입력)

#### 3-3. 커버리지 점수

각 분기/플로우에 등급:
- **★★★**: 엣지 케이스 AND 에러 경로 테스트 완료 (unit + integration)
- **★★**: 올바른 동작, 해피 패스만
- **★**: 스모크 테스트 / 존재 확인만

#### 3-4. 갭 식별

각 미테스트 분기마다:
- **unit / integration / E2E / eval** 중 어느 것?
- E2E 판단 기준: 3개 이상 컴포넌트 플로우 / 인증·결제 경로 / 모킹이 실제 실패를 숨기는 경우
- **회귀(regression)** 가능성 체크 (기존 코드가 깨질 수 있는가)

**중요 규칙**: 회귀는 **필수 테스트 추가**. 건너뛰지 않는다. AskUserQuestion 없이 기본값으로 포함.

#### 3-5. 테스트 계획 산출

`docs/features/<name>/tech-spec.md` §테스트 계획에 기록:
- 단위 테스트 목록
- 통합 테스트 목록
- E2E 테스트 목록
- 수동 QA 체크리스트

---

### 섹션 4. 성능 리뷰

평가 항목:
- N+1 쿼리 / DB 액세스 패턴
- 메모리 사용 우려
- 캐싱 기회
- 느린 / 고복잡도 코드 경로
- 인덱스 필요성
- 예상 트래픽 vs 용량

---

## Outside Voice (선택적)

모든 섹션 통과 후 **독립 AI에게 동일 계획을 챌린지** 시키는 옵션:

```
추천: A (독립 2차 의견은 구조적 맹점을 잡는다).
두 다른 AI가 동의하는 계획이 한 AI의 철저한 리뷰보다 강한 시그널.
완결성: A=9/10, B=7/10.

A. Outside Voice 받기
B. 건너뛰기
```

사용자 선택:
- A: 가능하면 다른 모델(Codex 등) 또는 Claude 서브에이전트로 챌린지
- 결과는 원본 그대로 제시
- **Cross-model tension** 확인: 원본 리뷰와 Outside Voice가 다를 때 개별 확인
- Outside Voice 의견을 **자동 적용하지 않는다**. 사용자 명시 승인 필요.

---

## 필수 산출물 섹션

모든 리뷰는 아래 섹션을 반드시 포함:

1. **NOT in scope** — 명시적으로 제외한 작업 + 근거
2. **What already exists** — 재활용하거나 불필요하게 재구현하는 기존 코드
3. **Test coverage diagram** — 모든 경로/갭의 ASCII 시각화
4. **Failure modes** — 새 코드 경로별 실제 프로덕션 실패 + 처리/테스트/가시성 여부
5. **Parallelization strategy** — 모듈별 의존성 테이블, 병렬 작업 레인, 실행 순서
6. **Completion summary** — 섹션별 발견 건수 테이블

### 선택 산출물
- **TODOS 업데이트** — 각 미루는 작업 개별 AskUserQuestion
- **Worktree 병렬화** — 독립 작업 흐름 여럿일 때

---

## 완료 상태 프로토콜

- **DONE** — 모든 단계 완료, 각 주장에 근거 있음
- **DONE_WITH_CONCERNS** — 완료되었으나 사용자가 알아야 할 이슈 있음
- **BLOCKED** — 진행 불가, 차단 요소 명시
- **NEEDS_CONTEXT** — 누락된 정보 요구

**에스컬레이션 규칙**: 한 작업에 3회 실패 시 중단 후 에스컬레이션. 보안 민감 변경 또는 검증 범위 초과 작업은 반드시 에스컬레이션.

---

## 안티패턴 / Voice

**피해야 할 표현**:
- "delve", "crucial", "robust"
- "here's the thing", "plot twist", "furthermore"
- 모호한 "best practices"

**지향하는 톤**:
- 직설적, 구체적, 날카로움, 격려적
- "빌더가 빌더에게" 말하는 톤
- 포인트 먼저, 다음에 메커니즘/트레이드오프/선택
- 공예를 존중, 느슨한 소프트웨어 정상화 X
- 모든 이슈를 **사용자 결과 또는 프로덕션 영향**에 연결

---

## 실행 절차

1. `docs/features/<name>/prd.md` 읽기
2. `docs/plan.md`, `docs/context.md` 읽기 (프로덕트 맥락)
3. Step 0 Scope Challenge
4. 섹션 1~4 순차 실행 (각 섹션 끝 STOP gate)
5. Outside Voice 옵션 제시
6. 산출물을 `docs/features/<name>/tech-spec.md`에 저장
7. 구현 순서를 **`tasks.md` Phase별로 분해**해서 추가 (Effort + AC 포함)

---

## 산출물 템플릿

`docs/features/<name>/tech-spec.md`에 저장. 기존 `feature-tech-spec-template.md`를 채우는 형태.
추가로 이 스킬 고유 섹션:

```markdown
## Scope Challenge 결과
- <복잡도 평가 + 결정>

## Coverage Diagram
<ASCII 다이어그램>

## Failure Modes Registry
| 코드 경로 | 실패 모드 | 처리 여부 | 테스트 커버 | 사용자 가시성 |
|---|---|---|---|---|

## Parallelization Strategy
| 모듈 | 의존 | 병렬 가능 | 실행 순서 |
|---|---|---|---|

## Outside Voice (선택)
- 실행 여부:
- 주요 tension:
- 사용자 결정:

## Completion Summary
| 섹션 | 발견 건수 | 해결 |
|---|---|---|
| Architecture | | |
| Code Quality | | |
| Tests | | |
| Performance | | |

**상태**: DONE / DONE_WITH_CONCERNS / BLOCKED / NEEDS_CONTEXT
```
