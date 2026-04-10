# Mock 데이터 스키마

> Pizza Ready 30일치 KPI Mock 데이터. DuckDB 내장 파일(`datapilot_mock.db`)에 저장.
> 3개 이상 시나리오가 매립되어 있으며, 에이전트 파이프라인이 이를 탐지·검증·추론하는 데 사용된다.

---

## 게임 정보

| 항목 | 값 |
|---|---|
| game_id | `pizza_ready` |
| 데이터 기간 | 30일 (2026-03-02 ~ 2026-03-31) |
| DAU 규모 | 약 900 (데모 축소, 유저 3000명 기준) |
| 이상 지표 | 3개 동시 발생 (서로 다른 원인) |

---

## 이상 시나리오 3개

| # | 이상 지표 | 세그먼트 | 숨겨진 원인 | 흔적 시점 | 관련 테이블 |
|---|---|---|---|---|---|
| 1 | 일 평균 인앱결제 매출이 D-3부터 약 11% 하락 | Android | 앱 업데이트로 상점 UI 진열 순서 변경 | 3일 전 (D-3) | users, payments, products, shop_impressions, releases |
| 2 | D7 리텐션이 D-14부터 약 4%p 하락 | 기존 유저 | 시즌 이벤트 종료 후 컨텐츠 공백 | 2주 전 (D-14) | users, events, sessions, content_releases |
| 3 | 결제 성공률이 D-0 당일 약 5%p 하락 | 브라질 | 지역 PG사(PagSeguro) 장애. 브라질에서 결제 실패율 약 56% | D-0 당일 | users, payment_attempts, payment_errors, gateways |

---

## 테이블 스키마 (12개)

### 집계

#### daily_kpi (일별 KPI 집계)

> ① Bottleneck Detector의 입력 소스. raw 테이블에서 매일 집계된 summary 테이블.
> 이상 3개가 이 테이블의 수치에 반영되어 있어야 한다.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| date | DATE | PK |
| dau | INTEGER | 일간 활성 유저 수 |
| mau | INTEGER | 30일 윈도우 활성 유저 수 |
| revenue | DECIMAL | 인앱결제 매출 합계 (status=success) |
| arppu | DECIMAL | 결제 유저당 평균 매출 |
| d1_retention | DECIMAL | D1 리텐션 (0~1) |
| d7_retention | DECIMAL | D7 리텐션 (0~1) |
| sessions | INTEGER | 총 세션 수 |
| avg_session_sec | INTEGER | 평균 세션 길이 (초) |
| payment_success_rate | DECIMAL | 결제 성공률 (0~1) |
| new_installs | INTEGER | 신규 설치 수 |

**이상 반영 규칙**:
- 매출: D-3부터 약 11% 감소 (Android premium 노출 감소 → 구매 감소)
- D7 리텐션: D-14부터 약 4% 감소 (D7 코호트만 복귀율 하락). DAU는 정상 유지 (-0.6%)
- 결제 성공률: D-0 당일 약 5% 하락 (브라질/PagSeguro 장애, 실패율 약 56%)
- MAU, DAU, 신규 설치 등은 정상 범위 유지

### 공통

#### users (유저 마스터)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| user_id | VARCHAR | PK |
| platform | VARCHAR | `android` / `ios` |
| country | VARCHAR | `brazil`, `usa`, `korea`, `japan`, `india`, ... |
| user_type | VARCHAR | `new` (설치 7일 이내) / `existing` |
| install_date | DATE | 설치일 |
| device_model | VARCHAR | `low` / `mid` / `high` (성능 구간) |

---

### 시나리오 1 — 매출 약 -11% (Android UI 변경)

#### payments (결제 이벤트)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| payment_id | VARCHAR | PK |
| user_id | VARCHAR | FK → users |
| product_id | VARCHAR | FK → products |
| amount | DECIMAL | 결제 금액 |
| timestamp | TIMESTAMP | 결제 시각 |
| status | VARCHAR | `success` / `failed` / `refunded` |

#### products (상품 마스터)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| product_id | VARCHAR | PK |
| name | VARCHAR | 상품명 |
| price_tier | VARCHAR | `low` (1000원 이하) / `mid` / `premium` (5000원 이상) |
| category | VARCHAR | `consumable` / `package` / `subscription` |

#### shop_impressions (상점 노출 로그)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| impression_id | VARCHAR | PK |
| user_id | VARCHAR | FK → users |
| product_id | VARCHAR | FK → products |
| impression_time | TIMESTAMP | 노출 시각 |
| slot_order | INTEGER | 진열 위치 (1=최상단). UI 변경 후 프리미엄 패키지가 하단으로 이동 |

#### releases (빌드 배포 이력)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| release_id | VARCHAR | PK |
| version | VARCHAR | `v1.2.3` |
| platform | VARCHAR | `android` / `ios` / `both` |
| released_at | TIMESTAMP | 배포 시각 |
| build_notes | VARCHAR | 배포 노트 (예: "Shop UI refresh (featured slot reordering)") |

---

### 시나리오 2 — D7 리텐션 약 -4%p (이벤트 종료)

#### events (인게임 이벤트 로그)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| event_id | VARCHAR | PK |
| user_id | VARCHAR | FK → users |
| event_type | VARCHAR | `login` / `stage_clear` / `purchase` / `event_participate` |
| event_time | TIMESTAMP | 이벤트 발생 시각 |
| metadata | VARCHAR | JSON 문자열. 이벤트별 부가 정보 |

#### sessions (세션 로그)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| session_id | VARCHAR | PK |
| user_id | VARCHAR | FK → users |
| session_start | TIMESTAMP | 세션 시작 |
| session_end | TIMESTAMP | 세션 종료 |
| platform | VARCHAR | `android` / `ios` |

#### content_releases (컨텐츠/이벤트 스케줄)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| content_id | VARCHAR | PK |
| content_type | VARCHAR | `season_event` / `update` / `promotion` |
| name | VARCHAR | 이벤트명 (예: "Pizza Festival Season 3") |
| start_date | DATE | 시작일 |
| end_date | DATE | 종료일. 이벤트 종료 후 컨텐츠 공백 발생 |

---

### 시나리오 3 — 결제 성공률 약 -5%p (PG 장애)

#### payment_attempts (결제 시도 로그)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| attempt_id | VARCHAR | PK |
| user_id | VARCHAR | FK → users |
| gateway | VARCHAR | FK → gateways.gateway_id. `google_play` / `apple_pay` / `pagseguro` / ... |
| attempt_time | TIMESTAMP | 시도 시각 |
| status | VARCHAR | `success` / `failed` |
| error_code | VARCHAR | 실패 시 에러 코드. 성공이면 NULL |

#### payment_errors (결제 에러 상세)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| error_code | VARCHAR | PK |
| error_message | VARCHAR | 에러 메시지 (예: "Gateway timeout") |
| gateway | VARCHAR | 어느 PG사에서 발생하는 에러인지 |
| first_seen | TIMESTAMP | 최초 발생 시각 |

#### gateways (PG사 정보)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| gateway_id | VARCHAR | PK. `google_play` / `apple_pay` / `pagseguro` / `stripe` |
| name | VARCHAR | 표시명 |
| region | VARCHAR | 주 사용 지역 (`global` / `brazil` / ...) |
| status | VARCHAR | `active` / `degraded` / `down` |

---

## 데이터 매립 규칙

### 시나리오 1 (D-3: Android UI 변경)
- `releases`에 D-3 시점 Android 배포 기록 추가. build_notes에 "Shop UI refresh (featured slot reordering)"
- D-3 이후 `shop_impressions`에서 Android + premium 상품의 `slot_order`가 1~3 → 4~6으로 이동 (상단 → 중하단)
- 유저별 스크롤 깊이(3~8)에 의해 premium 노출이 자연스럽게 감소
- `payments`는 노출된 상품 중에서만 구매 발생 → premium 구매도 자연 감소
- iOS는 변화 없음

### 시나리오 2 (D-14: 시즌 이벤트 종료)
- `content_releases`에 시즌 이벤트(Pizza Festival Season 3) 종료일 = D-14
- D-14 이후 `events`에서 `event_participate` 타입 이벤트 0건
- D-14 이후 설치 7일차(D7 코호트) 유저만 복귀 확률 감소 (30% → 25%) → D7 리텐션 하락
- DAU 전체에는 영향 거의 없음 (D7 코호트만 영향)
- 신규 유저는 영향 없음

### 시나리오 3 (D-0: PG 장애)
- `gateways`에서 pagseguro의 status = `degraded`
- D-0 당일 `payment_attempts`에서 gateway=pagseguro인 건의 실패율 60%로 급등
- `payment_errors`에 pagseguro 관련 에러 코드(gateway_timeout 등) 집중 발생
- 다른 PG사(google_play, apple_pay, stripe)는 정상
- 브라질 외 국가는 pagseguro 사용 안 하므로 영향 없음

---

## 실측 수치 (seed=42, NUM_USERS=3000)

| 시나리오 | 지표 | 실측 |
|---|---|---|
| 1. 매출 | 전체 매출 변화 (D-3 전후) | **-11.1%** |
| 1. 매출 | DAU | 변화 없음 |
| 2. 리텐션 | DAU 변화 | **-0.6%** (정상 범위) |
| 2. 리텐션 | D7 리텐션 변화 | **-3.9pp** (0.279 → 0.240) |
| 2. 리텐션 | event_participate (D-14 이후) | **0건** |
| 3. 결제 | 전체 결제 성공률 변화 (D-0) | **-5.4pp** (0.978 → 0.924) |
| 3. 결제 | PagSeguro D-0 실패율 | **56%** (16건 중 9건) |
| 3. 결제 | 다른 PG사 | 정상 (2% 실패) |

---

## 구현 시 주의사항

### 1. D7 리텐션 타이밍 역산
D7 리텐션 = "설치 후 7일째에 복귀한 비율". 시나리오 2의 흔적 시점이 D-14이므로:
- D-14에 이벤트 종료 → 기존 유저 복귀 감소
- `daily_kpi.d7_retention`이 하락하려면 **D-21 전후에 설치한 유저**의 D7 시점(D-14 이후)부터 복귀율이 떨어져야 함
- Mock 데이터에서 `users.install_date`가 D-21 근처인 유저들의 `sessions` 7일차 세션을 줄여야 함
- 즉, `daily_kpi` 값만 조작하는 게 아니라 **raw 테이블도 타이밍에 맞게** 세팅해야 ④ Data Validator의 SQL 검증이 통과

### 2. daily_kpi와 raw 테이블의 정합성
`daily_kpi`는 summary 테이블이지만, ④ Data Validator가 raw 테이블에서 직접 SQL을 돌려서 검증한다. 두 테이블 간 수치가 불일치하면:
- ① Bottleneck Detector는 이상을 탐지하지만
- ④가 raw 테이블에서 증거를 못 찾는 상황 발생
- **반드시 `daily_kpi` 수치 = raw 테이블 집계 결과**가 일치하도록 생성

### 3. 세그먼트 차원 컬럼 확인
② Segmentation Analyzer는 `users` 테이블의 세그먼트 컬럼으로 GROUP BY 한다.
- 현재 4개 차원: `platform`, `country`, `user_type`, `device_model`
- 스키마 자동 탐지 방식이므로 이 컬럼들이 `users` 테이블에 존재하면 자동으로 분석 대상에 포함
- 확장성 검증용 보너스 차원(예: `os_version`)을 하나 추가하면 "스키마 자동 탐지가 실제로 동작"하는 것을 시연 가능 (선택)

### 4. 브라질 유저의 PG 매핑
시나리오 3에서 "브라질 외 국가는 pagseguro 사용 안 함"이 핵심:
- `payment_attempts.gateway`와 `users.country`를 JOIN했을 때, `country='brazil'`인 유저만 `gateway='pagseguro'`를 사용해야 함
- 브라질 유저도 일부는 `google_play`나 `stripe`를 쓸 수 있지만, pagseguro 비중이 높아야 시나리오 성립
- 권장: 브라질 유저의 결제 시도 중 70%+ 가 pagseguro

### 5. `events.metadata` 타입
DuckDB는 `JSON` 타입을 네이티브 지원하지만, 프로토타입에서 이 필드를 파싱할 일이 적으면 `VARCHAR`로도 충분. 지금 스키마의 VARCHAR 그대로 진행해도 무방.

### 6. 정상 지표의 안정성
Bottleneck Detector가 "정상"으로 분류해야 하는 지표들(DAU, MAU, 세션 수, 신규 설치)은 **30일간 자연스러운 변동 범위(±3% 이내)**를 유지해야 함. 주말 효과 등 자연 패턴이 있으면 더 현실적이지만, 과도하면 거짓 양성(false positive) 발생 위험.

