"""데모 모드 — API 호출 없이 파이프라인 시뮬레이션.

화면2 진행 콜백을 실제처럼 발생시키고,
미리 구성된 PipelineReport를 반환한다.
"""

from __future__ import annotations

import time
from datetime import date

from datapilot.agents.action_recommender import Action, ActionPlan
from datapilot.agents.bottleneck_detector import AnomalyItem
from datapilot.agents.data_validator import ValidationResult
from datapilot.agents.hypothesis_generator import Hypothesis, HypothesisList
from datapilot.agents.root_cause_reasoner import (
    CausalStep,
    RootCause,
    RootCauseReport,
    UnverifiedHypothesis,
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


def _notify(on_step: OnStepCallback, agent: str, status: str,
            summary: str = "", metric: str = "") -> None:
    if on_step:
        on_step(PipelineStep(agent=agent, status=status,
                             summary=summary, metric=metric))


def _demo_analyzed_payment() -> AnomalyAnalysis:
    """결제 성공률 — segmentable 완전 분석 데모 데이터."""
    anomaly = AnomalyItem(
        metric="payment_success_rate",
        metric_label="결제 성공률 (payment_success_rate)",
        change=-0.058,
        change_display="98.1% -> 92.4%",
        comparison_detail="98.1% -> 92.4% (직전 26일 평균 98.0% -> 3/31 단일 수치 92.4%)",
        severity="HIGH",
        reasoning="3/31 결제 성공률이 92.4%로 급락하여 30일 중 최저치를 기록했다. "
                  "직전 29일 평균이 98.0% 수준으로 매우 안정적이었던 것과 대조적이다.",
    )
    segmentation = SegmentationReport(
        anomaly="결제 성공률",
        concentration=SegmentConcentration(
            dimension="country", focus="brazil", change=-0.2857,
        ),
        breakdown={
            "platform": {"android": -0.019, "ios": -0.009},
            "country": {
                "brazil": -0.286, "india": -0.015, "japan": -0.003,
                "korea": -0.009, "others": -0.042, "usa": 0.001,
            },
            "user_type": {"existing": -0.055, "new": -0.029},
            "device_model": {"high": -0.091, "low": -0.117, "mid": -0.009},
        },
        summary="결제 성공률 감소가 brazil에 집중 (-28.57%, 나머지 국가는 정상 수준)",
        spread_type="concentrated",
    )
    hypotheses = HypothesisList(
        anomaly="결제 성공률",
        hypotheses=[
            Hypothesis(
                hypothesis="브라질 담당 PG사(Gateway) 장애 또는 간헐적 오류 발생",
                reasoning="브라질 결제 실패율이 다른 국가 대비 현저히 높음",
                required_tables=["payment_attempts", "gateways"],
                required_data=None,
            ),
            Hypothesis(
                hypothesis="브라질 현지 결제 수단 연동 오류 또는 정책 변경",
                reasoning="PagSeguro 게이트웨이 상태 이상 확인 필요",
                required_tables=["payment_attempts", "payment_errors", "gateways"],
                required_data=None,
            ),
            Hypothesis(
                hypothesis="브라질 대상 앱 업데이트로 인한 결제 플로우 버그",
                reasoning="3/28 Android 빌드 배포 시점과 겹침",
                required_tables=["payment_attempts", "content_releases"],
                required_data=None,
            ),
        ],
    )
    validation_results = [
        ValidationResult(
            hypothesis="브라질 담당 PG사(Gateway) 장애 또는 간헐적 오류 발생",
            status="supported",
            evidence="PagSeguro 상태가 degraded로 기록됨. 3/31 실패율 56.25%로 급등.",
        ),
        ValidationResult(
            hypothesis="브라질 현지 결제 수단 연동 오류 또는 정책 변경",
            status="supported",
            evidence="PagSeguro 3/31 18:00~18:30 사이 E001·E002·E003 3종 에러 연쇄 발생.",
        ),
        ValidationResult(
            hypothesis="브라질 대상 앱 업데이트로 인한 결제 플로우 버그",
            status="rejected",
            evidence="빌드 배포 직후 3/28~3/30 실패율은 0~5%로 정상. 3/31 급등은 PagSeguro 장애.",
        ),
    ]
    root_cause = RootCauseReport(
        anomaly="결제 성공률",
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step="PagSeguro 게이트웨이에 인프라 장애 발생",
                    evidence="gateways 테이블에서 PagSeguro만 status='degraded'",
                ),
                CausalStep(
                    step="3/31 PagSeguro 결제 실패율 56.25%로 수직 상승",
                    evidence="16건 시도 중 9건 실패. E003 5건, E002 3건, E001 1건.",
                ),
                CausalStep(
                    step="PagSeguro는 브라질 결제의 68.4%를 처리하므로 영향이 집중됨",
                    evidence="브라질 결제 시도 954건 중 PagSeguro 653건(68.4%)",
                ),
                CausalStep(
                    step="브라질 결제 성공률 -28.57%p 급락이 전체 지표를 끌어내림",
                    evidence="country 차원 concentrated. 나머지 국가 -1.5% 이내 정상.",
                ),
            ],
            summary="브라질 전용 PG사 PagSeguro의 인프라 장애로 전체 결제 성공률이 92.4%로 하락",
        ),
        additional_investigation=[
            UnverifiedHypothesis(
                hypothesis="브라질 정부·금융 규제 변경으로 인한 결제 차단",
                required_data="브라질 디지털 상품 관련 세금·결제 규제 변경 이력",
            ),
        ],
    )
    action_plan = ActionPlan(
        anomaly="결제 성공률",
        actions=[
            Action(
                priority="urgent",
                title="PagSeguro 장애 시 대체 PG로 즉시 우회 라우팅",
                effect="실패율 56.25%를 5% 이하로 즉시 억제",
                effort="백엔드 개발자 1명, 2~4시간",
            ),
            Action(
                priority="short_term",
                title="PagSeguro 에러 코드 발생 시 자동 failover 로직 배포",
                effect="향후 재장애 시 자동 대응",
                effort="백엔드 개발자 2명, 3~5일",
            ),
            Action(
                priority="mid_term",
                title="브라질 결제 PG 다중화 — 2차 PG 추가 계약·연동",
                effect="단일 PG 장애 시 최대 노출을 10% 이하로 구조적 감소",
                effort="백엔드 2명 + 비즈니스 1명, 3~6주",
            ),
        ],
        note="PagSeguro 인프라 장애가 명확한 인과 체인으로 확인된 케이스.",
    )
    return AnomalyAnalysis(
        anomaly=anomaly,
        segmentation=segmentation,
        hypotheses=hypotheses,
        validation_results=validation_results,
        root_cause=root_cause,
        action_plan=action_plan,
    )


def _demo_analyzed_revenue() -> AnomalyAnalysis:
    """인앱결제 매출 — segmentable 완전 분석 데모 데이터."""
    anomaly = AnomalyItem(
        metric="revenue",
        metric_label="인앱결제 매출 (revenue)",
        change=-0.208,
        change_display="-20.8%",
        comparison_detail="-20.8% (3/17 고점 1,204,600원 -> 3/31 861,300원)",
        severity="MEDIUM",
        reasoning="3/17 최고점 이후 매출이 지속 하락하여 3/31에 30일 구간 최저치를 기록.",
    )
    segmentation = SegmentationReport(
        anomaly="인앱결제 매출",
        concentration=SegmentConcentration(
            dimension="platform", focus="android", change=-0.138,
        ),
        breakdown={
            "platform": {"android": -0.138, "ios": 0.056},
            "country": {
                "brazil": -0.073, "india": -0.048, "japan": -0.093,
                "korea": -0.012, "others": -0.082, "usa": -0.052,
            },
            "user_type": {"existing": -0.037, "new": -0.141},
            "device_model": {"high": -0.033, "low": -0.081, "mid": -0.123},
        },
        summary="인앱결제 매출 감소가 Android에 집중 (-13.8%, iOS는 +5.6%로 정상)",
        spread_type="concentrated",
    )
    hypotheses = HypothesisList(
        anomaly="인앱결제 매출",
        hypotheses=[
            Hypothesis(
                hypothesis="시즌 이벤트 종료로 Android 유저 구매 동기 감소",
                reasoning="Pizza Festival Season 3 종료일(3/17)과 매출 고점 일치",
                required_tables=["content_releases", "event_participate"],
                required_data=None,
            ),
            Hypothesis(
                hypothesis="Android 주요 PG사 장애로 결제 실패율 증가",
                reasoning="PagSeguro 장애와 시기 겹침",
                required_tables=["payment_attempts", "gateways"],
                required_data=None,
            ),
        ],
    )
    validation_results = [
        ValidationResult(
            hypothesis="시즌 이벤트 종료로 Android 유저 구매 동기 감소",
            status="supported",
            evidence="Pizza Festival Season 3 종료(3/17) 후 event_participate 0건으로 소멸. "
                     "Android 매출 -14.9% vs iOS -2.6%.",
        ),
        ValidationResult(
            hypothesis="Android 주요 PG사 장애로 결제 실패율 증가",
            status="supported",
            evidence="3/31 PagSeguro 성공률 43.75%로 급락. Android+PagSeguro 33.33%.",
        ),
    ]
    root_cause = RootCauseReport(
        anomaly="인앱결제 매출",
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step="Pizza Festival Season 3 이벤트 3/17 종료",
                    evidence="content_releases에서 종료일 2026-03-17 확인. 이후 참여 로그 0건.",
                ),
                CausalStep(
                    step="이벤트 종료 후 Android 유저 구매 동기 감소 → 매출 구조적 하락",
                    evidence="Android 일평균 매출 660,500원 → 562,279원 (-14.9%)",
                ),
                CausalStep(
                    step="3/31 PagSeguro 장애로 결제 실패 급등",
                    evidence="PagSeguro 3/31 성공률 43.75%. Android+PagSeguro 33.33%.",
                ),
            ],
            summary="이벤트 종료에 의한 구조적 하락과 PG 장애가 복합 작용하여 매출 -20.8% 하락",
        ),
        additional_investigation=[
            UnverifiedHypothesis(
                hypothesis="Android 상점 UI 변경으로 프리미엄 상품 노출 감소",
                required_data="상품별 노출→클릭→결제 퍼널 로그",
            ),
        ],
    )
    action_plan = ActionPlan(
        anomaly="인앱결제 매출",
        actions=[
            Action(
                priority="urgent",
                title="PagSeguro 결제 트래픽을 대체 PG로 우회",
                effect="결제 실패율 억제로 당일 매출 손실 차단",
                effort="백엔드 개발자 1명, 0.5일",
            ),
            Action(
                priority="short_term",
                title="이벤트 종료 후 복귀 보상 푸시 발송",
                effect="Android 유저 구매 동기 재점화",
                effort="마케터 1명, 1일",
            ),
            Action(
                priority="mid_term",
                title="시즌 간 브릿지 콘텐츠 기획 프로세스 수립",
                effect="이벤트 종료 시 매출 낙폭을 85% 이상으로 유지",
                effort="PM 1명 + 콘텐츠 기획자 1명, 3주",
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


def _demo_unanalyzed() -> list[UnanalyzedAnomaly]:
    """non-segmentable 이상 지표 데모 데이터."""
    return [
        UnanalyzedAnomaly(
            anomaly=AnomalyItem(
                metric="arppu",
                metric_label="결제 유저당 평균 매출 (arppu)",
                change=-0.138,
                change_display="-13.8%",
                comparison_detail="-13.8% (직전 14일 평균 5,113원 -> 최근 4일 평균 4,405원)",
                severity="HIGH",
                reasoning="3/28부터 ARPPU가 급격히 하락. 4일 연속 4,600원 이하를 기록.",
            ),
        ),
        UnanalyzedAnomaly(
            anomaly=AnomalyItem(
                metric="new_installs",
                metric_label="신규 설치 (new_installs)",
                change=4.72,
                change_display="+472%",
                comparison_detail="+472% (직전 24일 평균 25.5건 -> 3/25 이후 7일 평균 128건)",
                severity="MEDIUM",
                reasoning="3/25부터 신규 설치가 이례적으로 급등. 7일 연속 100건을 상회.",
            ),
        ),
    ]


def build_demo_report() -> PipelineReport:
    """데모용 PipelineReport를 구성한다."""
    return PipelineReport(
        game_id="pizza_ready",
        period_from="2026-03-02",
        period_to="2026-03-31",
        analyzed=[_demo_analyzed_payment(), _demo_analyzed_revenue()],
        unanalyzed=_demo_unanalyzed(),
        normal_metrics=["dau", "sessions", "mau", "d1_retention", "d7_retention"],
        anomaly_order=["payment_success_rate", "arppu", "new_installs", "revenue"],
    )


def run_demo(
    period: tuple[date, date],
    *,
    on_step: OnStepCallback = None,
) -> PipelineReport:
    """데모 파이프라인 — API 호출 없이 진행 콜백 + 결과 반환.

    화면2 진행 애니메이션을 실제처럼 시뮬레이션한다.
    """
    report = build_demo_report()

    # ① 병목 탐지
    _notify(on_step, "bottleneck", "active")
    time.sleep(1.0)
    _notify(on_step, "bottleneck", "done", "이상 지표 4개 발견")

    # segmentable 2개에 대해 ②~⑥ 시뮬레이션
    for analysis in report.analyzed:
        m = analysis.anomaly.metric
        n_hyp = len(analysis.hypotheses.hypotheses)
        sup = sum(1 for v in analysis.validation_results if v.status == "supported")
        rej = sum(1 for v in analysis.validation_results if v.status == "rejected")
        unv = sum(1 for v in analysis.validation_results if v.status == "unverified")
        n_act = len(analysis.action_plan.actions)
        agents = [
            ("segmentation", "active", "", 0.8),
            ("segmentation", "done", analysis.segmentation.concentration.focus, 0),
            ("hypothesis", "active", "", 0.6),
            ("hypothesis", "done", f"가설 {n_hyp}개", 0),
            ("validation", "active", "", 1.5),
            ("validation", "done", f"확인 {sup} / 기각 {rej} / 미검증 {unv}", 0),
            ("root_cause", "active", "", 0.8),
            ("root_cause", "done", "완료", 0),
            ("action", "active", "", 0.6),
            ("action", "done", f"액션 {n_act}개", 0),
        ]
        for agent, status, summary, delay in agents:
            _notify(on_step, agent, status, summary, metric=m)
            if delay:
                time.sleep(delay)

    return report