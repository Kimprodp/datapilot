"""데모 모드 — API 호출 없이 파이프라인 시뮬레이션.

``docs/analysis-result-backup.md`` 의 정답 분석 결과(2026-04-13 실행)를
Pydantic 모델 상수로 고정해두고, 진행 콜백을 실제 파이프라인과 동일한
순서·구조로 발화한다. 영상 촬영·빠른 데모 용도.

시나리오 (4개 지표):
  - 결제 성공률 (HIGH, segmentable)
  - 인앱결제 매출 (LOW, segmentable)
  - D7 리텐션 (MEDIUM, segmentable)
  - 유저당 평균 결제액 (MEDIUM, 세부 분석 미지원)
"""

from __future__ import annotations

import time

from datapilot.agents.action_recommender import Action, ActionPlan
from datapilot.agents.bottleneck_detector import AnomalyItem
from datapilot.agents.data_validator import ValidationResult
from datapilot.agents.hypothesis_generator import Hypothesis, HypothesisList
from datapilot.agents.root_cause_reasoner import (
    CausalStep,
    RootCause,
    RootCauseReport,
)
from datapilot.agents.segmentation_analyzer import (
    SegmentConcentration,
    SegmentationReport,
)
from datapilot.pipeline import (
    AnomalyAnalysis,
    OnStepCallback,
    PipelineReport,
    PipelineStep,
    UnanalyzedAnomaly,
)

# ------------------------------------------------------------------
# 타이밍 (초) — 영상 분량을 고려해 실제 동작과 유사하게 조정
# ------------------------------------------------------------------

_BOTTLENECK_ACTIVE_SEC = 2.0
_STEP_DELAYS = {
    "segmentation": 1.5,
    "hypothesis": 2.0,
    "validation": 3.0,
    "root_cause": 2.0,
    "action": 1.5,
}

# ------------------------------------------------------------------
# 이상 지표 라벨 상수 (각 데모 함수 내 중복 문자열 방지)
# ------------------------------------------------------------------

_LABEL_PAYMENT = "결제 성공률"
_LABEL_REVENUE = "인앱결제 매출"
_LABEL_D7_RETENTION = "D7 리텐션"
_LABEL_ARPPU = "유저당 평균 결제액"


def _notify(
    on_step: OnStepCallback,
    agent: str,
    status: str,
    summary: str = "",
    metric: str = "",
) -> None:
    if on_step:
        on_step(PipelineStep(
            agent=agent, status=status, summary=summary, metric=metric,
        ))


# ------------------------------------------------------------------
# 이상 지표 1: 결제 성공률 (HIGH, segmentable)
# ------------------------------------------------------------------


def _demo_payment_success_rate() -> AnomalyAnalysis:
    anomaly = AnomalyItem(
        metric="payment_success_rate",
        metric_label="결제 성공률 (payment_success_rate)",
        change=-0.090,
        change_display="97.6% → 88.6% (-9.0%p)",
        comparison_detail=(
            "-9.0%p (3/31 단 하루 만에 97.6%→88.6% 급락, 30일 구간 최저치)"
        ),
        severity="HIGH",
        reasoning=(
            "3/31 단 하루 만에 결제 성공률이 88.6%로 급락해 30일 구간 최저치를 "
            "기록했다. 안정성 지표 특성상 2%p 이상 이탈은 이상 신호이며, 동일 시점 "
            "DAU·세션·매출은 전일 대비 큰 변동이 없어 결제 인프라 또는 PG 연동 "
            "문제로 추정된다."
        ),
    )
    segmentation = SegmentationReport(
        anomaly=_LABEL_PAYMENT,
        concentration=SegmentConcentration(
            dimension="country", focus="brazil", change=-46.3,
        ),
        breakdown={
            "platform": {"android": -11.0, "ios": -8.5},
            "country": {
                "brazil": -46.3, "india": 0.0, "japan": -1.4,
                "korea": -5.7, "others": 3.1, "usa": -5.3,
            },
            "user_type": {"existing": -11.7, "new": -4.9},
            "device_model": {"high": -17.2, "low": -12.9, "mid": -4.1},
        },
        summary="결제 성공률 감소가 브라질에 집중 (-46.3%, 나머지 국가는 -5.7%~+3.1%)",
        spread_type="concentrated",
    )
    hypotheses = HypothesisList(
        anomaly=_LABEL_PAYMENT,
        hypotheses=[
            Hypothesis(
                hypothesis="브라질 담당 PG사(게이트웨이) 장애 또는 성능 저하",
                reasoning="브라질만 -46.3% 집중, 타 국가 정상 → 브라질 전용 PG 의심",
                required_tables=["payment_attempts", "gateways"],
            ),
            Hypothesis(
                hypothesis="브라질 현지 결제 수단(PIX/Boleto 등) 연동 오류 또는 정책 변경",
                reasoning="현지 결제 수단 연동 단절 시 브라질 단독 급락 가능",
                required_tables=["payment_attempts", "payment_errors"],
            ),
            Hypothesis(
                hypothesis="브라질 대상 앱 업데이트 또는 결제 모듈 변경 배포",
                reasoning="3/28 Android 배포 시점과 시기 근접",
                required_tables=["payment_attempts", "content_releases"],
            ),
            Hypothesis(
                hypothesis="브라질 헤알(BRL) 환율 급변 또는 현지 통화 처리 로직 오류",
                reasoning="환율 변동으로 결제 승인 거부 증가 가능",
                required_tables=["payment_attempts"],
            ),
        ],
    )
    validation_results = [
        ValidationResult(
            hypothesis="브라질 담당 PG사(게이트웨이) 장애 또는 성능 저하",
            status="supported",
            evidence=(
                "PagSeguro의 status가 'degraded'로 확인되었으며, 3/31 성공률이 "
                "전일 95.0%에서 29.2%로 -65.8%p 폭락했다. 동일 시점 타 게이트웨이"
                "(apple_pay·google_play·stripe)는 95.7~100% 성공률을 유지해 "
                "브라질 단독 PG사 장애임이 명확하다."
            ),
        ),
        ValidationResult(
            hypothesis="브라질 현지 결제 수단(PIX/Boleto 등) 연동 오류 또는 정책 변경",
            status="supported",
            evidence=(
                "에러 코드 E001(Gateway timeout)·E002(Connection refused)·"
                "E003(Service unavailable)이 모두 first_seen = 2026-03-31로 "
                "3/31에 최초 출현했다. 총 17건의 실패가 해당 에러 코드로 집중되어 "
                "PagSeguro 연동 자체의 인프라/API 단절이 명확히 확인된다."
            ),
        ),
        ValidationResult(
            hypothesis="브라질 대상 앱 업데이트 또는 결제 모듈 변경 배포",
            status="rejected",
            evidence=(
                "content_releases에서 3/28 Android v1.2.3 배포 이후 3/28~3/30 "
                "브라질 결제 성공률은 94~96% 정상 범위를 유지. 3/31 급락은 "
                "배포와 무관한 PG사 장애 시점과 일치한다."
            ),
        ),
        ValidationResult(
            hypothesis="브라질 헤알(BRL) 환율 급변 또는 현지 통화 처리 로직 오류",
            status="rejected",
            evidence=(
                "payment_attempts의 BRL 결제 실패 사유가 모두 E001~E003 "
                "(Gateway/Connection/Service 장애)으로 통화 변환 오류 코드는 0건. "
                "환율 이슈가 아닌 PG 인프라 장애로 판정된다."
            ),
        ),
    ]
    root_cause = RootCauseReport(
        anomaly=_LABEL_PAYMENT,
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step="브라질 전담 PG사 PagSeguro에서 인프라 장애 발생 (상태: degraded)",
                    evidence="gateways 테이블에서 PagSeguro만 status='degraded' 확인",
                ),
                CausalStep(
                    step="3/31 18시부터 PagSeguro 전용 신규 에러 코드 3종이 연쇄 출현",
                    evidence="E001·E002·E003 first_seen = 2026-03-31, 총 17건 실패",
                ),
                CausalStep(
                    step="PagSeguro 경유 결제 시도 24건 중 17건이 해당 에러로 실패하여 성공률 29.2%로 폭락",
                    evidence="PagSeguro 3/31 성공 7/24건, 타 게이트웨이 95.7~100% 정상",
                ),
                CausalStep(
                    step="PagSeguro 실패가 브라질 국가 결제 성공률을 -46.3%p 끌어내림",
                    evidence="country 차원 concentrated, 브라질만 -46.3% 집중",
                ),
                CausalStep(
                    step="브라질 결제 실패 집중으로 전체 결제 성공률이 97.6%→88.6%(-9.0%p) 급락",
                    evidence="전체 성공률 -9.0%p, 타 국가 -5.7%~+3.1% 정상 범위",
                ),
            ],
            summary=(
                "브라질 전담 PG사 PagSeguro의 인프라 장애(degraded)로 3/31 해당 "
                "게이트웨이 성공률이 95.0%→29.2%로 폭락하면서 전체 결제 성공률이 "
                "97.6%→88.6%로 -9.0%p 급락했다."
            ),
        ),
    )
    action_plan = ActionPlan(
        anomaly=_LABEL_PAYMENT,
        actions=[
            Action(
                priority="urgent",
                title="브라질 결제 PagSeguro 우회 처리",
                effect="실패 집중 구간 성공률을 5분 이내 90%대 복원",
                effort="백엔드 1명, 2~4시간",
            ),
            Action(
                priority="urgent",
                title="PagSeguro 3종 에러 실시간 알럿",
                effect="재발 시 평균 탐지 시간을 18시간→10분으로 단축",
                effort="SRE 1명, 반나절",
                related_cause_step="3/31 18시부터 PagSeguro 전용 신규 에러 코드 3종이 연쇄 출현",
            ),
            Action(
                priority="short_term",
                title="브라질 결제 자동 폴백 구현",
                effect="단일 PG 장애 시 손실 규모 80% 이상 축소",
                effort="백엔드 2명, 3~5일",
            ),
            Action(
                priority="short_term",
                title="PagSeguro SLA 위반 확인·보상 청구",
                effect="장애로 인한 매출 손실분 일부 회수",
                effort="BizDev 1명, 1주",
            ),
            Action(
                priority="mid_term",
                title="게이트웨이 상태 기반 자동 라우팅 구축",
                effect="PG 장애 시 자동 트래픽 전환으로 가용성 99%대 유지",
                effort="백엔드 3명, 4~6주",
            ),
        ],
    )
    return AnomalyAnalysis(
        anomaly=anomaly,
        segmentation=segmentation,
        hypotheses=hypotheses,
        validation_results=validation_results,
        root_cause=root_cause,
        action_plan=action_plan,
    )


# ------------------------------------------------------------------
# 이상 지표 2: 인앱결제 매출 (LOW, segmentable)
# ------------------------------------------------------------------


def _demo_revenue() -> AnomalyAnalysis:
    anomaly = AnomalyItem(
        metric="revenue",
        metric_label="인앱결제 매출 (revenue)",
        change=-0.178,
        change_display="-17.8%",
        comparison_detail=(
            "-17.8% (3/25~3/31 평균 966,571원 vs 3/14~3/24 평균 1,048,636원, "
            "3/31 단일 850,700원은 30일 최저)"
        ),
        severity="LOW",
        reasoning=(
            "3/29(856,900원)~3/31(850,700원) 구간에서 매출이 30일 기간 내 "
            "최저치를 연이어 경신했다. 그러나 ARPPU 하락 및 결제 성공률 급락과 "
            "연동된 후행 결과로, 독립적 원인보다 선행 이상(payment_success_rate "
            "급락, arppu 하락)의 영향으로 판단한다."
        ),
    )
    segmentation = SegmentationReport(
        anomaly=_LABEL_REVENUE,
        concentration=SegmentConcentration(
            dimension="country", focus="brazil", change=-14.2,
        ),
        breakdown={
            "platform": {"android": -8.5, "ios": -5.9},
            "country": {
                "brazil": -14.2, "india": -1.8, "japan": -5.3,
                "korea": 4.2, "others": -8.7, "usa": -11.3,
            },
            "user_type": {"existing": -5.5, "new": -13.6},
            "device_model": {"high": -2.7, "low": -5.1, "mid": -13.8},
        },
        summary="인앱결제 매출 감소가 특정 세그먼트에 집중되지 않음 (전반적 하락)",
        spread_type="spread",
    )
    hypotheses = HypothesisList(
        anomaly=_LABEL_REVENUE,
        hypotheses=[
            Hypothesis(
                hypothesis="결제 게이트웨이(PG사) 장애 또는 성능 저하로 결제 성공률 급락 → 매출 하락",
                reasoning="3/31 결제 성공률 폭락과 동일 시점에서 매출 최저치 발생",
                required_tables=["payment_attempts", "gateways"],
            ),
            Hypothesis(
                hypothesis="최근 앱 업데이트에서 상점 UI 또는 결제 흐름 변경으로 구매 전환율 저하",
                reasoning="3/28 Android 배포 이후 매출 연속 최저",
                required_tables=["content_releases", "shop_impressions"],
            ),
            Hypothesis(
                hypothesis="고가 상품 구매 비중 감소(상품 믹스 변화)로 ARPPU 하락 → 매출 감소",
                reasoning="ARPPU -11.8%와 결제 건수 변화가 매출 하락 선행 요인일 수 있음",
                required_tables=["payment_attempts"],
            ),
            Hypothesis(
                hypothesis="이벤트/프로모션 종료로 할인 상품·구매 유인이 사라져 ARPPU 및 매출 동반 하락",
                reasoning="이벤트 종료 후 프로모션 상품 구매가 줄어들 수 있음",
                required_tables=["content_releases", "event_participate"],
            ),
            Hypothesis(
                hypothesis="브라질·미국 등 주요 시장의 환율 변동 또는 스토어 가격 정책 변경",
                reasoning="국가별 매출 분포가 고르게 하락하는 패턴과 연관 가능",
                required_tables=["payment_attempts"],
            ),
        ],
    )
    validation_results = [
        ValidationResult(
            hypothesis="결제 게이트웨이(PG사) 장애 또는 성능 저하로 결제 성공률 급락 → 매출 하락",
            status="supported",
            evidence=(
                "3/31 PagSeguro 성공률 29.2%로 폭락해 브라질 매출 -14.2% 하락에 "
                "직접 기여. 전체 결제 성공률 97.6%→88.6% 연동."
            ),
        ),
        ValidationResult(
            hypothesis="최근 앱 업데이트에서 상점 UI 또는 결제 흐름 변경으로 구매 전환율 저하",
            status="supported",
            evidence=(
                "3/28 Android v1.2.3 업데이트('Shop UI refresh') 이후 슬롯 1~3의 "
                "premium 상품 노출이 3/27 2,250건에서 867~918건으로 59~61% 급감함."
            ),
        ),
        ValidationResult(
            hypothesis="고가 상품 구매 비중 감소(상품 믹스 변화)로 ARPPU 하락 → 매출 감소",
            status="supported",
            evidence=(
                "premium 결제 건수 3/26 134건 → 3/31 99건(-26%), "
                "mid 결제 건수 22건 → 47건(+114%)로 상품 믹스 저가 전환 확인."
            ),
        ),
        ValidationResult(
            hypothesis="이벤트/프로모션 종료로 할인 상품·구매 유인이 사라져 ARPPU 및 매출 동반 하락",
            status="rejected",
            evidence=(
                "Pizza Festival Season 3 종료(3/17) 이후 event_participate가 "
                "0건이나 매출은 3/18~3/24 평균 1,048,636원으로 유지됨. 매출 급락은 "
                "3/25 이후 별개 요인(UI·PG)과 일치."
            ),
        ),
        ValidationResult(
            hypothesis="브라질·미국 등 주요 시장의 환율 변동 또는 스토어 가격 정책 변경",
            status="rejected",
            evidence=(
                "payment_attempts에서 BRL·USD 결제의 단가는 기존 범위를 유지하며 "
                "환율/가격 변경 흔적 없음. 매출 하락은 건수 감소에 의한 것."
            ),
        ),
    ]
    root_cause = RootCauseReport(
        anomaly=_LABEL_REVENUE,
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step="3/28 Android v1.2.3 업데이트에서 상점 UI 추천 슬롯 순서가 변경됨",
                    evidence="content_releases에 'Shop UI refresh' 노트 기록",
                ),
                CausalStep(
                    step="상점 상위 슬롯(1~3위)의 프리미엄 상품 노출이 60% 급감함",
                    evidence="3/27 2,250건 → 3/28~3/31 867~918건 (-59~61%)",
                ),
                CausalStep(
                    step="프리미엄 상품 결제 건수가 감소하고 중·저가 상품 비중이 증가함",
                    evidence="premium 134→99건 (-26%), mid 22→47건 (+114%)",
                ),
                CausalStep(
                    step="결제 건당 평균 금액 및 ARPPU가 하락함",
                    evidence="ARPPU 직전 7일 5,167원 → 최근 7일 4,637원 (-11.8%)",
                ),
                CausalStep(
                    step="3/31 PagSeguro 결제 게이트웨이에 장애가 발생하여 브라질 결제 성공률이 폭락함",
                    evidence="PagSeguro status='degraded', 성공률 95.0%→29.2%",
                ),
                CausalStep(
                    step="전체 결제 성공률이 급락하여 성공 결제 건수가 추가 감소함",
                    evidence="전체 성공률 97.6%→88.6% (-9.0%p)",
                ),
                CausalStep(
                    step="ARPPU 하락과 결제 성공률 급락이 복합 작용하여 인앱결제 매출이 30일 최저치를 기록함",
                    evidence="3/31 단일 매출 850,700원 (30일 최저)",
                ),
            ],
            summary=(
                "3/28 Android 상점 UI 업데이트로 프리미엄 상품 노출이 60% 급감하여 "
                "고가 상품 결제 비중이 하락(ARPPU -11.8%)했고, 동시에 3/31 PagSeguro "
                "결제 게이트웨이 장애(성공률 29.2%)가 겹치면서 결제 성공률이 88.6%로 "
                "급락해 인앱결제 매출이 -17.8% 하락했다."
            ),
        ),
    )
    action_plan = ActionPlan(
        anomaly=_LABEL_REVENUE,
        actions=[
            Action(
                priority="urgent",
                title="Android v1.2.3 상점 슬롯 롤백",
                effect="프리미엄 노출 복구로 ARPPU 3일 이내 정상 범위 회귀",
                effort="Android 개발자 1명, 0.5일",
            ),
            Action(
                priority="urgent",
                title="PagSeguro 장애 대응 — 브라질 결제 게이트웨이 전환",
                effect="당일 매출 손실 차단",
                effort="백엔드 1명, 2~4시간",
            ),
            Action(
                priority="short_term",
                title="상점 슬롯 A/B 테스트 재검증",
                effect="프리미엄 노출 최적 순서 확정",
                effort="PM 1명 + 데이터 1명, 1~2주",
            ),
            Action(
                priority="short_term",
                title="결제 게이트웨이 자동 페일오버 구현",
                effect="PG 장애 시 자동 우회로 매출 손실 최소화",
                effort="백엔드 2명, 3~5일",
            ),
            Action(
                priority="mid_term",
                title="상점 UI 변경 매출 영향 사전 검증 프로세스 수립",
                effect="UI 개편 관련 매출 리스크 구조적 차단",
                effort="PM + 개발자 + 데이터, 2~3주",
            ),
        ],
    )
    return AnomalyAnalysis(
        anomaly=anomaly,
        segmentation=segmentation,
        hypotheses=hypotheses,
        validation_results=validation_results,
        root_cause=root_cause,
        action_plan=action_plan,
    )


# ------------------------------------------------------------------
# 이상 지표 3: D7 리텐션 (MEDIUM, segmentable)
# ------------------------------------------------------------------


def _demo_d7_retention() -> AnomalyAnalysis:
    anomaly = AnomalyItem(
        metric="d7_retention",
        metric_label="D7 리텐션 (d7_retention)",
        change=-0.119,
        change_display="28.6% → 16.7% (-11.9%p)",
        comparison_detail=(
            "-11.9%p (3/21(13.3%)부터 시작된 하락이 3/25 이후 가속되어 3/31 16.7%)"
        ),
        severity="MEDIUM",
        reasoning=(
            "3/21(13.3%)부터 시작된 D7 리텐션 하락이 3/25 이후 더욱 가속되어 "
            "3/31 16.7%까지 떨어졌다. 리텐션 이상 기준(3%p)을 크게 초과하며 약 "
            "11.9%p 하락이다. 같은 시점 신규 설치가 급증한 점을 고려하면 유입된 "
            "신규 유저의 7일 재방문율이 낮아 전체 D7 리텐션을 끌어내린 연동 효과가 "
            "주원인으로 추정된다."
        ),
    )
    segmentation = SegmentationReport(
        anomaly=_LABEL_D7_RETENTION,
        concentration=SegmentConcentration(
            dimension="platform", focus="android", change=-44.0,
        ),
        breakdown={
            "platform": {"android": -44.0, "ios": -6.5},
            "country": {
                "brazil": -22.5, "india": -34.7, "japan": 8.5,
                "korea": -28.6, "others": -16.8, "usa": -21.3,
            },
            "user_type": {"existing": -35.8},
            "device_model": {"high": -38.2, "low": -2.1, "mid": -22.5},
        },
        summary="D7 리텐션 감소가 Android에 집중 (-44.0%, iOS는 -6.5%)",
        spread_type="concentrated",
    )
    hypotheses = HypothesisList(
        anomaly=_LABEL_D7_RETENTION,
        hypotheses=[
            Hypothesis(
                hypothesis="Android 대상 UA 캠페인으로 저품질 신규 유저 대량 유입 → D7 리텐션 희석",
                reasoning="신규 설치 급증과 Android 집중 현상이 동시에 관측됨",
                required_tables=["users", "sessions"],
            ),
            Hypothesis(
                hypothesis="3월 중순 이벤트/콘텐츠 종료로 7일 후 복귀 동기 소멸",
                reasoning="복귀 유인 부재 시 D7 재방문율 하락",
                required_tables=["content_releases", "event_participate"],
            ),
            Hypothesis(
                hypothesis="Android 전용 앱 업데이트로 인한 성능·UX 악화",
                reasoning="Android 집중 하락은 플랫폼 고유 이슈 가능성",
                required_tables=["content_releases", "sessions"],
            ),
            Hypothesis(
                hypothesis="Android 버전 업데이트 시 초기 콘텐츠/온보딩 플로우 변경",
                reasoning="온보딩 개편으로 신규 유저 7일 복귀율 저하 가능",
                required_tables=["content_releases", "sessions"],
            ),
        ],
    )
    validation_results = [
        ValidationResult(
            hypothesis="Android 대상 UA 캠페인으로 저품질 신규 유저 대량 유입 → D7 리텐션 희석",
            status="supported",
            evidence=(
                "3/25부터 Android 중심으로 신규 설치가 25건→128건 수준으로 5~6배 "
                "급증. 해당 코호트의 D7 재방문율이 기존 대비 현저히 낮아 전체 "
                "지표를 희석하는 효과 확인."
            ),
        ),
        ValidationResult(
            hypothesis="3월 중순 이벤트/콘텐츠 종료로 7일 후 복귀 동기 소멸",
            status="supported",
            evidence=(
                "'Pizza Festival Season 3'가 3/17 종료, 이후 event_participate "
                "이벤트 로그가 완전히 0건으로 소멸."
            ),
        ),
        ValidationResult(
            hypothesis="Android 전용 앱 업데이트로 인한 성능·UX 악화",
            status="rejected",
            evidence=(
                "3/28 Android v1.2.3 배포 이후 crash rate·세션 길이·로딩 시간 "
                "모두 기존 범위 유지. 성능·UX 악화 흔적 없음."
            ),
        ),
        ValidationResult(
            hypothesis="Android 버전 업데이트 시 초기 콘텐츠/온보딩 플로우 변경",
            status="rejected",
            evidence=(
                "content_releases에 온보딩 플로우 변경 기록 없음. 3/28 업데이트는 "
                "Shop UI 변경으로 한정됨."
            ),
        ),
    ]
    root_cause = RootCauseReport(
        anomaly=_LABEL_D7_RETENTION,
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step="시즌 이벤트(Pizza Festival Season 3)가 3/17 종료되어 복귀 동기가 소멸됨",
                    evidence="content_releases 종료일 2026-03-17, event_participate 0건",
                ),
                CausalStep(
                    step="이벤트 종료 코호트부터 7일차 재방문 유인이 약해져 D7 리텐션이 선행 하락 시작",
                    evidence="3/21부터 D7 리텐션 13.3% 진입, 기존 평균 28.6% 대비 이탈",
                ),
                CausalStep(
                    step="3/25부터 양 플랫폼 UA 캠페인이 시작되어 신규 설치가 약 5~6배 급증함(Android 중심)",
                    evidence="직전 21일 평균 25.2건 → 3/25~3/31 평균 128.1건 (+408%)",
                ),
                CausalStep(
                    step="대량 유입된 신규 유저의 낮은 7일 재방문율이 전체 D7 리텐션을 추가 희석시킴",
                    evidence="Android 코호트 D7 -44.0% 집중, iOS -6.5%와 큰 격차",
                ),
                CausalStep(
                    step="두 요인이 복합 작용하여 D7 리텐션이 3/31 최저 16.7%까지 급락",
                    evidence="28.6% → 16.7% (-11.9%p)",
                ),
            ],
            summary=(
                "3/17 시즌 이벤트 종료로 7일차 복귀 유인이 소멸되어 D7 리텐션이 "
                "선행 하락하던 중, 3/25부터 UA 캠페인으로 저품질 신규 유저가 5~6배 "
                "대량 유입(특히 Android 집중)되면서 D7 리텐션 희석이 가속되어 "
                "16.7%까지 급락했다."
            ),
        ),
    )
    action_plan = ActionPlan(
        anomaly=_LABEL_D7_RETENTION,
        actions=[
            Action(
                priority="urgent",
                title="Android UA 캠페인 즉시 일시 중단",
                effect="저품질 유입 차단으로 D7 희석 효과 제거",
                effort="마케터 1명, 즉시",
            ),
            Action(
                priority="urgent",
                title="신규 이벤트 콘텐츠 긴급 투입 일정 확정",
                effect="복귀 동기 회복으로 D7 선행 반등",
                effort="콘텐츠 기획자 1명, 1~3일",
            ),
            Action(
                priority="short_term",
                title="Android 온보딩 D7 집중 개선",
                effect="신규 코호트 7일 복귀율 20%대 회복",
                effort="PM + Android 개발자 + 디자이너, 2주",
            ),
            Action(
                priority="short_term",
                title="UA 캠페인 소재·타겟 D7 기준 재심사",
                effect="재개 시 저품질 유입 비중 50% 이상 감소",
                effort="마케터 + 데이터 분석가, 1주",
            ),
            Action(
                priority="mid_term",
                title="시즌 종료-신규 콘텐츠 공백 제로화 프로세스 수립",
                effect="이벤트 공백으로 인한 리텐션 낙폭 구조적 차단",
                effort="PM + 기획 조직, 3~4주",
            ),
        ],
    )
    return AnomalyAnalysis(
        anomaly=anomaly,
        segmentation=segmentation,
        hypotheses=hypotheses,
        validation_results=validation_results,
        root_cause=root_cause,
        action_plan=action_plan,
    )


# ------------------------------------------------------------------
# 이상 지표 4: 유저당 평균 결제액 (MEDIUM, 세부 분석 미지원)
# ------------------------------------------------------------------


def _demo_arppu_unanalyzed() -> UnanalyzedAnomaly:
    return UnanalyzedAnomaly(
        anomaly=AnomalyItem(
            metric="arppu",
            metric_label="유저당 평균 결제액 (arppu)",
            change=-0.118,
            change_display="-11.8%",
            comparison_detail=(
                "-11.8% (직전 7일 평균 5,167원 → 최근 7일 평균 4,637원)"
            ),
            severity="MEDIUM",
            reasoning=(
                "3/25 이후 ARPPU가 4,296~4,867원대로 내려앉아 7일 연속 하락 추세. "
                "신규 설치 급증에 따른 연동 효과로 추정된다 "
                "(선행: new_installs ↑ → 후행: arppu ↓)."
            ),
        ),
    )


# ==================================================================
# 이커머스 도메인 — 시나리오: kitchen 카테고리 인기 상품(p_kitchen_01) 품절
# ==================================================================
#
# 라이브 분석(2026-05-09 측정) 결과를 Pydantic 객체 패턴으로 고정.
# 본 묶음의 검증 통과 결과 — 두 segmentable KPI(orders, gmv)가 모두
# "재고 부족" 으로 정답 도출, ⑤ RCR 환각 방어 정상 작동.

_LABEL_ORDERS = "주문 수"
_LABEL_GMV = "총 거래액"
_LABEL_CONVERSION = "전환율"


def _demo_orders() -> AnomalyAnalysis:
    anomaly = AnomalyItem(
        metric="orders",
        metric_label="주문 수 (orders)",
        change=-0.126,
        change_display="-12.6%",
        comparison_detail=(
            "직전 22일 평균 500 → 최근 8일 평균 437"
        ),
        severity="HIGH",
        reasoning=(
            "3/24(화)부터 주문 수가 500건에서 437건으로 수직 하락하여 8일 연속 "
            "동일 수준 유지 중. 정상 변동 범위(±5%)를 크게 초과하는 12.6% 급감이며, "
            "단일 이벤트성 변동이 아닌 구조적 하락 고착 패턴. 같은 기간 방문자(visitors)는 "
            "14,500~15,100명대로 정상 유지되어 트래픽 감소가 아닌 전환 단계의 문제로 추정됨."
        ),
    )
    segmentation = SegmentationReport(
        anomaly=_LABEL_ORDERS,
        concentration=SegmentConcentration(
            dimension="country", focus="japan", change=-13.1,
        ),
        breakdown={
            "country": {
                "germany": -5.5, "japan": -13.1,
                "korea": -9.5, "usa": -10.6,
            },
            "customer_type": {
                "new": -9.8, "returning": -12.8, "vip": -9.2,
            },
            "device": {
                "desktop": -13.0, "mobile": -10.8, "tablet": 3.2,
            },
        },
        summary="주문 수 감소가 특정 세그먼트에 집중되지 않음 (전반적 하락)",
        spread_type="spread",
    )
    hypotheses = HypothesisList(
        anomaly=_LABEL_ORDERS,
        hypotheses=[
            Hypothesis(
                hypothesis="인기 상품 품절·단종으로 구매 가능 상품 감소",
                reasoning=(
                    "주문 수 단일 시점 수직 하락 + 방문자 정상 유지 → "
                    "유입은 있으나 살 게 없는 상황 의심"
                ),
                required_tables=[
                    "inventory_changes", "products", "orders",
                    "category_daily_revenue",
                ],
            ),
            Hypothesis(
                hypothesis="결제 성공률 하락으로 인한 주문 전환 실패 증가",
                reasoning=(
                    "전환율 하락(3.37% → 2.96%) 발생 → 결제 단계 장애 가능성 점검"
                ),
                required_tables=["daily_kpi"],
            ),
            Hypothesis(
                hypothesis="프로모션/할인 캠페인 종료로 구매 유인 소멸",
                reasoning=(
                    "전사 주문 수 동시 하락 시 가격 매력도 소멸이 일반적 원인"
                ),
                required_tables=["orders", "category_daily_revenue"],
            ),
            Hypothesis(
                hypothesis="체크아웃 UI/UX 변경(빌드 배포)으로 전환 퍼널 이탈 증가",
                reasoning=(
                    "전환율 하락 + 트래픽 정상 → 결제·체크아웃 흐름 변경 의심"
                ),
                required_tables=["daily_kpi", "orders"],
            ),
        ],
    )
    validation_results = [
        ValidationResult(
            hypothesis="인기 상품 품절·단종으로 구매 가능 상품 감소",
            status="supported",
            evidence=(
                "3/24 시점에 kitchen 카테고리 핵심 상품(p_kitchen_01)이 "
                "out_of_stock으로 전환(보충 일정 미정)되었고, 동시에 kitchen "
                "주문이 125건 → 62건(-50.4%)으로 급감했다. beauty·electronics·"
                "fashion 3개 카테고리는 주문 수 변동이 전혀 없어 품절이 직접 "
                "원인임이 명확하다. 전체 주문 감소분(-63건)이 kitchen 감소분"
                "(-63건)과 정확히 일치한다."
            ),
        ),
        ValidationResult(
            hypothesis="결제 성공률 하락으로 인한 주문 전환 실패 증가",
            status="rejected",
            evidence=(
                "결제 성공률은 3/24 이후 오히려 소폭 상승(96.63% → 96.94%)했으며, "
                "전환율 하락(3.37% → 2.96%)과 방문자 수 정상(14,848 → 14,803)은 "
                "결제 게이트웨이 장애와 무관함을 보여준다. 결제 성공률이 상승한 "
                "사실이 해당 가설을 정면으로 반박한다."
            ),
        ),
        ValidationResult(
            hypothesis="프로모션/할인 캠페인 종료로 구매 유인 소멸",
            status="rejected",
            evidence=(
                "프로모션 종료라면 전 카테고리 전환율이 동반 하락해야 하나, "
                "beauty·electronics·fashion은 3/24 이후에도 주문 수가 125건으로 "
                "완전히 동일하게 유지됐다. 하락이 kitchen 단일 카테고리에만 "
                "국한되어 있어 가격 매력도 소멸 가설과 부합하지 않는다."
            ),
        ),
        ValidationResult(
            hypothesis="체크아웃 UI/UX 변경(빌드 배포)으로 전환 퍼널 이탈 증가",
            status="rejected",
            evidence=(
                "UI/UX 배포 장애라면 전 카테고리 전환율이 동시에 하락해야 하나, "
                "beauty·electronics·fashion 3개 카테고리는 3/24 전후 주문 수가 "
                "125건으로 동일하며 영향이 전혀 없다. 하락이 kitchen 카테고리에만 "
                "집중된 구조는 UI 변경이 아닌 상품 가용성 문제를 가리킨다."
            ),
        ),
    ]
    root_cause = RootCauseReport(
        anomaly=_LABEL_ORDERS,
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step="키친 카테고리 핵심 상품 품절 전환",
                    evidence=(
                        "3/24 00:00 기준 p_kitchen_01(Kitchen Item 1)이 "
                        "'재고 소진(보충 일정 미정)' 사유로 out_of_stock 상태로 "
                        "전환되었고, 같은 날 inventory_changes에 기록된 품절 "
                        "상품은 해당 1건뿐이다."
                    ),
                ),
                CausalStep(
                    step=(
                        "품절로 인해 키친 카테고리 전환율이 하락하여 "
                        "키친 주문 수가 절반으로 급감"
                    ),
                    evidence=(
                        "kitchen 카테고리 일평균 주문 수가 3/24 이전 125건에서 "
                        "이후 62건으로 -50.4% 급감한 반면, "
                        "beauty·electronics·fashion 3개 카테고리는 동 기간 125건"
                        "으로 전혀 변동이 없다."
                    ),
                ),
                CausalStep(
                    step=(
                        "키친 주문 감소분이 전체 주문 감소분과 완전히 일치하여 "
                        "전사 주문 수 -12.6% 하락 고착"
                    ),
                    evidence=(
                        "전체 일평균 주문 수가 500건 → 437건(-63건)으로 "
                        "감소했으며, 이는 kitchen 감소분(-63건)과 정확히 일치하고, "
                        "방문자 수는 14,848명 → 14,803명으로 정상 유지되어 "
                        "트래픽 이슈가 아님이 확인된다."
                    ),
                ),
            ],
            summary=(
                "3/24 kitchen 카테고리 핵심 상품(Kitchen Item 1) 품절로 인해 "
                "kitchen 주문이 50.4% 급감했고, 이 감소분이 전체 주문 수 -12.6% "
                "하락과 정확히 일치한다."
            ),
        ),
    )
    action_plan = ActionPlan(
        anomaly=_LABEL_ORDERS,
        actions=[
            Action(
                priority="urgent",
                title="p_kitchen_01 긴급 재고 확보 및 판매 재개",
                effect="kitchen 카테고리 주문 수 회복으로 전사 주문 수 -12.6% 하락 즉시 해소 기대",
                effort="MD/소싱 담당자 1명, 공급사 긴급 협의 1~2일",
            ),
            Action(
                priority="urgent",
                title="p_kitchen_01 품절 기간 대체 상품 전면 노출",
                effect="품절 공백 기간 동안 kitchen 카테고리 전환율 하락 최소화 및 주문 이탈 방어",
                effort="MD 1명 + 프론트 운영자 1명, 반나절 (상품 선정 및 배너/진열 변경)",
            ),
            Action(
                priority="short_term",
                title="kitchen 카테고리 핵심 상품 재고 임계치 알림 설정",
                effect="핵심 상품 품절 전환 전 사전 감지로 동일 유형의 주문 급감 재발 방지",
                effort="개발자 1명, 3일 (재고 임계치 알림 로직 구현 및 MD 알림 채널 연동)",
            ),
            Action(
                priority="mid_term",
                title="카테고리별 핵심 상품 안전 재고 기준 수립 및 정기 점검 프로세스 도입",
                effect="단일 상품 품절이 카테고리·전사 주문에 미치는 리스크를 구조적으로 차단",
                effort="MD팀 + 운영팀 협업, 2주 (기준 정의 → 점검 주기 및 책임자 지정 → 문서화)",
            ),
        ],
    )
    return AnomalyAnalysis(
        anomaly=anomaly,
        segmentation=segmentation,
        hypotheses=hypotheses,
        validation_results=validation_results,
        root_cause=root_cause,
        action_plan=action_plan,
    )


def _demo_gmv_ecommerce() -> AnomalyAnalysis:
    anomaly = AnomalyItem(
        metric="gmv",
        metric_label="총 거래액 (gmv)",
        change=-0.127,
        change_display="-12.7%",
        comparison_detail=(
            "직전 22일 평균 2,001,479 → 최근 8일 평균 1,749,647"
        ),
        severity="MEDIUM",
        reasoning=(
            "3/24부터 GMV가 약 200만에서 175만 수준으로 하락하여 8일 연속 유지. "
            "orders 급감(-12.6%)과 동일 시점·동일 폭으로 하락하며 주문 수 감소의 "
            "직접적인 후행 결과. 객단가(GMV/orders)는 3,978→4,006으로 오히려 "
            "소폭 상승, 즉 단가 문제가 아닌 주문 건수 감소가 원인."
        ),
    )
    segmentation = SegmentationReport(
        anomaly=_LABEL_GMV,
        concentration=SegmentConcentration(
            dimension="device", focus="desktop", change=-16.6,
        ),
        breakdown={
            "country": {
                "germany": -9.5, "japan": -13.6,
                "korea": -11.9, "usa": -13.8,
            },
            "customer_type": {
                "new": -9.7, "returning": -10.9, "vip": -12.5,
            },
            "device": {
                "desktop": -16.6, "mobile": -6.7, "tablet": 1.8,
            },
        },
        summary="총 거래액 감소가 특정 세그먼트에 집중되지 않음 (전반적 하락)",
        spread_type="spread",
    )
    hypotheses = HypothesisList(
        anomaly=_LABEL_GMV,
        hypotheses=[
            Hypothesis(
                hypothesis="인기 카테고리 또는 베스트셀러 상품 품절/단종으로 구매 가능 상품 축소",
                reasoning=(
                    "GMV -12.7% 와 주문수 -12.6% 동조 하락 + 객단가 정상 → "
                    "주문 건수 감소가 매출 하락 견인. 단일 상품/카테고리 이슈 의심"
                ),
                required_tables=[
                    "inventory_changes", "products", "orders",
                    "category_daily_revenue",
                ],
            ),
            Hypothesis(
                hypothesis="주요 프로모션/할인 캠페인 종료로 인한 전체 주문 수 급감",
                reasoning=(
                    "전체 GMV 동시 하락이라면 가격 매력도 소멸이 일반적 원인"
                ),
                required_tables=["orders", "category_daily_revenue"],
            ),
            Hypothesis(
                hypothesis="외부 트래픽(마케팅 채널/광고) 유입 감소로 방문자 수 자체가 줄어 주문 감소",
                reasoning=(
                    "GMV 하락 시 매출 깔때기 가장 위 단계인 유입부터 점검"
                ),
                required_tables=["daily_kpi"],
            ),
            Hypothesis(
                hypothesis="결제 성공률 하락 또는 결제 시스템 부분 장애로 완결 주문 감소",
                reasoning=(
                    "GMV 직접 영향 가능한 결제 단계의 장애 가능성 점검"
                ),
                required_tables=["daily_kpi"],
            ),
        ],
    )
    validation_results = [
        ValidationResult(
            hypothesis="인기 카테고리 또는 베스트셀러 상품 품절/단종으로 구매 가능 상품 축소",
            status="supported",
            evidence=(
                "3/24 당일 p_kitchen_01(Kitchen Item 1)이 '재고 소진(보충 일정 "
                "미정)'으로 out_of_stock 전환되었다. kitchen 카테고리 주문이 "
                "정확히 3/24부터 125건 → 62건으로 -50.4% 급감하였으며, 전체 주문 "
                "감소(-63건/일)와 kitchen 카테고리 감소(-63건/일)가 완전히 1:1로 "
                "일치한다. 나머지 3개 카테고리(beauty·electronics·fashion)는 "
                "동일 기간 변화가 없어 품절이 단일 원인임을 강하게 지지한다."
            ),
        ),
        ValidationResult(
            hypothesis="주요 프로모션/할인 캠페인 종료로 인한 전체 주문 수 급감",
            status="rejected",
            evidence=(
                "beauty·electronics·fashion 3개 카테고리는 3/24 이후에도 일 125건"
                "으로 전혀 감소하지 않았다. 주문 감소는 kitchen 카테고리 단독"
                "(-50%)에서만 발생했으며, 프로모션 종료 시 예상되는 전 세그먼트·"
                "전 카테고리 동시 하락 패턴과 일치하지 않는다. 객단가 역시 "
                "3,998원 → 4,004원으로 거의 변동이 없어 할인 종료에 따른 단가 "
                "상승 효과도 확인되지 않는다."
            ),
        ),
        ValidationResult(
            hypothesis="외부 트래픽(마케팅 채널/광고) 유입 감소로 방문자 수 자체가 줄어 주문 감소",
            status="rejected",
            evidence=(
                "3/24 이후 일평균 방문자 수는 14,803명으로, 이전(14,848명) 대비 "
                "-0.3%에 불과하여 사실상 변동이 없다. 트래픽 유입 감소 가설이 "
                "성립하려면 방문자 수의 유의미한 하락이 선행되어야 하나, 데이터상 "
                "이를 전혀 확인할 수 없다. 전환율 하락(3.37% → 2.96%)은 트래픽 "
                "감소가 아닌 구매 가능 상품 축소에 기인한 것으로 판단된다."
            ),
        ),
        ValidationResult(
            hypothesis="결제 성공률 하락 또는 결제 시스템 부분 장애로 완결 주문 감소",
            status="rejected",
            evidence=(
                "3/24 이후 결제 성공률은 96.94%로, 이전(96.63%) 대비 오히려 "
                "+0.31%p 소폭 상승하였다. 결제 시스템 장애의 증거가 전혀 없으며, "
                "전환율 하락(3.37% → 2.96%)은 결제 실패가 아닌 kitchen 카테고리 "
                "핵심 상품 품절로 인한 구매 가능 상품 감소에 기인한다. desktop "
                "기기의 상대적 큰 하락(-16.6%) 역시 결제 모듈 장애가 아닌 상품 "
                "부재로 설명된다."
            ),
        ),
    ]
    root_cause = RootCauseReport(
        anomaly=_LABEL_GMV,
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step="Kitchen Item 1 재고 소진으로 품절 전환",
                    evidence=(
                        "3/24 p_kitchen_01이 'out_of_stock(재고 소진, 보충 일정 "
                        "미정)'으로 전환되었으며, inventory_changes 기준 해당일 "
                        "재고 상태 변경은 이 1건이 유일하다."
                    ),
                ),
                CausalStep(
                    step="kitchen 카테고리 전환율 하락 및 주문 수 급감",
                    evidence=(
                        "kitchen 카테고리 일 주문 수가 3/23 125건에서 3/24 62건"
                        "으로 -50.4% 급감하였으며, 동일 기간 "
                        "beauty·electronics·fashion 3개 카테고리는 일 125건으로 "
                        "변동 없었다."
                    ),
                ),
                CausalStep(
                    step="kitchen 카테고리 주문 감소가 전체 주문 감소와 완전히 1:1 대응",
                    evidence=(
                        "전체 일 주문 수가 500건→437건(-63건)으로 감소하였고, "
                        "kitchen 카테고리 감소분(-63건)과 완전히 일치한다."
                    ),
                ),
                CausalStep(
                    step="전체 전환율 하락 (방문자 수는 유지)",
                    evidence=(
                        "3/24 이후 일평균 방문자 수는 14,803명으로 이전(14,848명) "
                        "대비 -0.3% 수준이나, 전환율은 3.37%→2.96%로 -0.41%p "
                        "하락하였다."
                    ),
                ),
                CausalStep(
                    step="주문 건수 감소가 총 거래액 -12.7% 하락으로 직결",
                    evidence=(
                        "객단가는 3,998원→4,004원으로 사실상 동일하고, 총 거래액은 "
                        "일평균 2,001,479→1,749,647로 -12.7% 하락하여 주문 건수 "
                        "감소(-12.6%)와 동일한 폭·시점으로 하락하였다."
                    ),
                ),
            ],
            summary=(
                "3/24 Kitchen Item 1(p_kitchen_01)의 재고 소진(보충 일정 미정)으로 "
                "kitchen 카테고리 주문이 -50.4% 급감하였고, 이 감소분이 전체 주문 "
                "수 감소와 1:1로 일치하여 총 거래액이 -12.7% 하락하였다."
            ),
        ),
    )
    action_plan = ActionPlan(
        anomaly=_LABEL_GMV,
        actions=[
            Action(
                priority="urgent",
                title="p_kitchen_01 긴급 재입고 발주",
                effect="kitchen 카테고리 주문 수 회복으로 GMV -12.7% 하락 즉시 반전 기대",
                effort="MD/구매 담당자 1명, 당일 공급사 긴급 발주 처리",
            ),
            Action(
                priority="urgent",
                title="p_kitchen_01 품절 페이지에 대체 상품 노출",
                effect="품절 방문자의 이탈 방지 및 kitchen 카테고리 전환율 부분 회복",
                effort="프론트엔드 개발자 1명 + MD 1명, 0.5일",
            ),
            Action(
                priority="short_term",
                title="kitchen 카테고리 할인/기획전으로 수요 회복",
                effect="품절 기간 중 카테고리 내 타 상품 구매 유도로 GMV 손실 최소화",
                effort="마케팅 담당자 1명 + MD 1명, 2~3일",
            ),
            Action(
                priority="short_term",
                title="재고 소진 임박 시 자동 알림 설정",
                effect="주요 상품 품절 전환 전 선제 발주로 동일 사고 재발 방지",
                effort="운영 담당자 1명, 재고 관리 시스템 임계값 설정 1일",
            ),
            Action(
                priority="mid_term",
                title="단일 상품 의존도 모니터링 대시보드 구축",
                effect="특정 상품·카테고리 GMV 기여 집중도 상시 파악으로 리스크 조기 감지",
                effort="데이터 분석가 1명 + 개발자 1명, 1~2주",
            ),
        ],
    )
    return AnomalyAnalysis(
        anomaly=anomaly,
        segmentation=segmentation,
        hypotheses=hypotheses,
        validation_results=validation_results,
        root_cause=root_cause,
        action_plan=action_plan,
    )


def _demo_conversion_unanalyzed() -> UnanalyzedAnomaly:
    return UnanalyzedAnomaly(
        anomaly=AnomalyItem(
            metric="conversion",
            metric_label="전환율 (conversion)",
            change=-0.119,
            change_display="3.4% → 3.0% (-0.4%p)",
            comparison_detail=(
                "직전 22일 평균 3.37% → 최근 8일 평균 2.97%"
            ),
            severity="MEDIUM",
            reasoning=(
                "3/24부터 전환율이 3.3~3.5%에서 2.9~3.0%로 0.4%p 급락하여 "
                "8일 연속 이 수준에 고착. orders 급감과 동일 시점에 발생하며 "
                "선행 지표인 orders 이상과 연동된 후행 확인 지표. 방문자 수는 "
                "정상이므로 유입 문제가 아닌 장바구니·결제 흐름 상의 전환 장벽이 "
                "발생한 것으로 추정됨."
            ),
        ),
    )


def build_demo_report_ecommerce() -> PipelineReport:
    """이커머스 데모용 PipelineReport 를 구성한다."""
    return PipelineReport(
        entity_id="ecommerce_demo",
        period_from="2026-03-02",
        period_to="2026-03-31",
        analyzed=[
            _demo_orders(),
            _demo_gmv_ecommerce(),
        ],
        unanalyzed=[_demo_conversion_unanalyzed()],
        normal_metrics=["visitors", "payment_success_rate"],
        anomaly_order=["orders", "conversion", "gmv"],
    )


# ------------------------------------------------------------------
# 리포트 조립
# ------------------------------------------------------------------


def build_demo_report() -> PipelineReport:
    """데모용 PipelineReport를 구성한다.

    anomaly_order는 ① 병목 탐지 보고 순서를 따른다.
    """
    return PipelineReport(
        entity_id="pizza_ready",
        period_from="2026-03-02",
        period_to="2026-03-31",
        analyzed=[
            _demo_payment_success_rate(),
            _demo_revenue(),
            _demo_d7_retention(),
        ],
        unanalyzed=[_demo_arppu_unanalyzed()],
        normal_metrics=[
            "dau", "sessions", "mau", "d1_retention",
            "new_installs", "avg_session_sec",
        ],
        anomaly_order=[
            "payment_success_rate",
            "revenue",
            "d7_retention",
            "arppu",
        ],
    )


# ------------------------------------------------------------------
# 파이프라인 시뮬레이션
# ------------------------------------------------------------------


def _lookup_label_and_direction(
    report: PipelineReport,
    metric: str,
) -> tuple[str, str]:
    """지표 코드로 한글 라벨 + 증가/감소 방향을 찾는다."""
    for a in report.analyzed:
        if a.anomaly.metric == metric:
            label = a.anomaly.metric_label.split("(")[0].strip()
            direction = "증가" if a.anomaly.change > 0 else "감소"
            return label, direction
    for ua in report.unanalyzed:
        if ua.anomaly.metric == metric:
            label = ua.anomaly.metric_label.split("(")[0].strip()
            direction = "증가" if ua.anomaly.change > 0 else "감소"
            return label, direction
    return metric, ""


def _simulate_analyze_one(
    on_step: OnStepCallback,
    analysis: AnomalyAnalysis,
) -> None:
    """segmentable 1개 지표의 ②~⑥ 콜백을 순차 발화한다."""
    m = analysis.anomaly.metric
    n_hyp = len(analysis.hypotheses.hypotheses)
    sup = sum(1 for v in analysis.validation_results if v.status == "supported")
    rej = sum(1 for v in analysis.validation_results if v.status == "rejected")
    unv = sum(1 for v in analysis.validation_results if v.status == "unverified")
    n_act = len(analysis.action_plan.actions)
    rc_summary = "원인 불명" if not analysis.root_cause.root_cause.chain else "완료"

    steps = [
        ("segmentation", m, analysis.segmentation.concentration.focus),
        ("hypothesis", m, f"가설 {n_hyp}개"),
        ("validation", m, f"확인 {sup} / 기각 {rej} / 미검증 {unv}"),
        ("root_cause", m, rc_summary),
        ("action", m, f"액션 {n_act}개"),
    ]
    for agent, metric, done_summary in steps:
        _notify(on_step, agent, "active", metric, metric=metric)
        time.sleep(_STEP_DELAYS[agent])
        _notify(on_step, agent, "done", done_summary, metric=metric)


_DEMO_BUILDERS = {
    "game": build_demo_report,
    "ecommerce": build_demo_report_ecommerce,
}


def run_demo(
    domain: str = "game",
    *,
    on_step: OnStepCallback = None,
) -> PipelineReport:
    """데모 파이프라인을 실행한다.

    실제 ``PipelineOrchestrator.run()`` 이 발화하는 콜백 순서·메타를 그대로 재현한다:

        bottleneck active → bottleneck done (요약)
        direction info × N (각 이상 지표 방향)
        unsupported done × K (미지원 지표 선표시)
        [segmentable 지표 각각]
            segmentation active → done
            hypothesis active → done (가설 N개)
            validation active → done (확인 S / 기각 R / 미검증 U)
            root_cause active → done
            action active → done (액션 N개)

    Args:
        domain: 데모 대상 도메인 ("game" / "ecommerce"). 디폴트는 "game" (legacy).
    """
    builder = _DEMO_BUILDERS.get(domain, build_demo_report)
    report = builder()

    # ── ① 병목 탐지 ─────────────────────────────────────
    _notify(on_step, "bottleneck", "active")
    time.sleep(_BOTTLENECK_ACTIVE_SEC)

    labels_with_direction: list[str] = []
    for metric in report.anomaly_order:
        label, direction = _lookup_label_and_direction(report, metric)
        labels_with_direction.append(f"{label} {direction}")

    n = len(report.anomaly_order)
    summary = f"이상 지표 {n}개 발견 ({', '.join(labels_with_direction)})"
    _notify(on_step, "bottleneck", "done", summary)

    # 각 이상 지표의 방향 정보 전달 (카드 라벨 suffix용)
    for metric in report.anomaly_order:
        _, direction = _lookup_label_and_direction(report, metric)
        _notify(on_step, "direction", "info", direction, metric=metric)

    # ── 미지원 지표 먼저 알림 ──────────────────────────
    for ua in report.unanalyzed:
        _notify(
            on_step, "unsupported", "done",
            "세부 분석 미지원", metric=ua.anomaly.metric,
        )

    # ── segmentable 지표 ②~⑥ 시뮬레이션 ───────────────
    for analysis in report.analyzed:
        _simulate_analyze_one(on_step, analysis)

    return report