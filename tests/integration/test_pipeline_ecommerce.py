"""이커머스 도메인 풀 파이프라인 통합 테스트.

검증 범위:
- ``make_repository("ecommerce")`` + ``AgentBundle.create("ecommerce", repo=repo)``
  + ``PipelineOrchestrator(repo, agents=agents)`` 풀 흐름이 동작
- 6 에이전트가 모두 호출되고 ②~⑥ 가 anomaly 별 루프 실행
- ②③ 메서드에 ``entity_id="ecommerce_demo"`` 가 위치 인자로 전달
- AgentBundle 의 도메인 키워드가 ③⑤⑥ 에 주입된 상태로 호출됨
- 두 anomaly (gmv 하락 + orders 하락) 에 대한 분석 결과가 PipelineReport 에 보존

LLM 호출은 mock — 실제 LLM 응답은 단위 테스트 영역 (test_root_cause_3rules,
test_user_template_keywords 등) 또는 manual eval 로 검증.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, Mock

import pytest

from datapilot.agents import AgentBundle
from datapilot.agents.action_recommender import Action, ActionPlan
from datapilot.agents.bottleneck_detector import AnomalyItem, AnomalyReport
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
from datapilot.domain import ECOMMERCE
from datapilot.pipeline import PipelineOrchestrator
from datapilot.repository.port import DataRepository

PERIOD = (date(2026, 3, 2), date(2026, 3, 31))
ENTITY_ID = "ecommerce_demo"


# ════════════════════════════════════════════════════════════════════
# 헬퍼 — 이커머스 mock 출력
# ════════════════════════════════════════════════════════════════════


def _ecommerce_anomaly_report() -> AnomalyReport:
    """① BottleneckDetector mock — 이커머스 두 이상 지표 (gmv ↓ + orders ↓)."""
    return AnomalyReport(
        anomalies=[
            AnomalyItem(
                metric="gmv",
                metric_label="총거래액 (gmv)",
                change=-0.20,
                change_display="-20%",
                comparison_detail="평균 -20%",
                severity="HIGH",
                reasoning="D-3 부터 평균 객단가 하락",
            ),
            AnomalyItem(
                metric="orders",
                metric_label="주문 건수 (orders)",
                change=-0.125,
                change_display="-12.5%",
                comparison_detail="평균 -12.5%",
                severity="MEDIUM",
                reasoning="D-7 부터 kitchen 카테고리 주문 50% 감소",
            ),
        ],
        normal=["payment_success_rate", "conversion"],
    )


def _segmentation(metric: str) -> SegmentationReport:
    return SegmentationReport(
        anomaly=metric,
        concentration=SegmentConcentration(
            dimension="customer_type",
            focus="returning",
            change=-0.15,
        ),
        breakdown={
            "customer_type": {"new": -0.10, "returning": -0.15, "vip": -0.08},
        },
        summary=f"{metric} 의 returning 고객 집중",
        spread_type="concentrated",
    )


def _hypotheses(metric: str) -> HypothesisList:
    return HypothesisList(
        anomaly=metric,
        hypotheses=[
            Hypothesis(
                hypothesis=(
                    "재고 부족" if metric == "orders" else "프로모션 종료"
                ),
                reasoning="...",
                required_tables=(
                    ["products", "orders"]
                    if metric == "orders"
                    else ["promotions", "orders"]
                ),
            ),
        ],
    )


def _validation_results(metric: str) -> list[ValidationResult]:
    return [
        ValidationResult(
            hypothesis=("재고 부족" if metric == "orders" else "프로모션 종료"),
            status="supported",
            evidence="(mock) GROUP BY 결과로 입증",
        ),
    ]


def _root_cause(metric: str) -> RootCauseReport:
    return RootCauseReport(
        anomaly=metric,
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step=(
                        "kitchen 카테고리 인기 상품 품절"
                        if metric == "orders"
                        else "spring_sale 종료"
                    ),
                    evidence="(mock) 근거 SQL",
                ),
            ],
            summary=(
                "재고 부족" if metric == "orders" else "프로모션 종료"
            ),
        ),
    )


def _action_plan(metric: str) -> ActionPlan:
    return ActionPlan(
        anomaly=metric,
        actions=[
            Action(
                priority="urgent",
                title=(
                    "재고 보충"
                    if metric == "orders"
                    else "프로모션 연장 검토"
                ),
                effect="복구",
                effort="운영 1명",
            ),
        ],
    )


def _make_mock_repo() -> MagicMock:
    repo = MagicMock(spec=DataRepository)
    repo.get_daily_kpi.return_value = {
        "entity_id": ENTITY_ID,
        "period": {"from": "2026-03-02", "to": "2026-03-31"},
        "daily": [],
    }
    repo.get_available_schema.return_value = {
        "tables": [
            {"name": "orders", "columns": [], "description": ""},
            {"name": "products", "columns": [], "description": ""},
            {"name": "promotions", "columns": [], "description": ""},
        ],
    }
    return repo


def _make_orchestrator():
    """이커머스 AgentBundle + Pipeline + mock 6 에이전트."""
    repo = _make_mock_repo()
    agents = AgentBundle.create("ecommerce", repo=repo)
    orch = PipelineOrchestrator(repo=repo, agents=agents)

    orch._detector.detect = Mock(return_value=_ecommerce_anomaly_report())
    orch._segmenter.analyze = Mock(
        side_effect=lambda entity_id, anomaly, *a, **kw: _segmentation(anomaly.metric),
    )
    orch._hypothesis_gen.generate = Mock(
        side_effect=lambda entity_id, anomaly, *a, **kw: _hypotheses(anomaly.metric),
    )
    orch._validator.validate = Mock(
        side_effect=lambda hypotheses, schema, **kw: _validation_results(
            hypotheses.anomaly,
        ),
    )
    orch._reasoner.reason = Mock(
        side_effect=lambda anomaly, *a, **kw: _root_cause(anomaly.metric),
    )
    orch._recommender.recommend = Mock(
        side_effect=lambda root_cause, **kw: _action_plan(root_cause.anomaly),
    )
    return orch, repo


# ════════════════════════════════════════════════════════════════════
# 1. 풀 파이프라인 실행
# ════════════════════════════════════════════════════════════════════


class TestEcommercePipelineRun:
    def test_runs_without_error(self):
        orch, _ = _make_orchestrator()
        report = orch.run(ENTITY_ID, PERIOD)
        assert report is not None
        assert report.entity_id == ENTITY_ID

    def test_two_anomalies_analyzed(self):
        """gmv + orders 두 anomaly 가 ②~⑥ 루프를 각각 통과."""
        orch, _ = _make_orchestrator()
        report = orch.run(ENTITY_ID, PERIOD)
        assert len(report.analyzed) == 2
        analyzed_metrics = {a.anomaly.metric for a in report.analyzed}
        assert analyzed_metrics == {"gmv", "orders"}

    def test_unanalyzed_for_non_segmentable(self):
        """payment_success_rate / conversion 은 ECOMMERCE.supported_segment_metrics
        밖이라 ② 호출 안 되고 unanalyzed 카드로 빠진다."""
        orch, _ = _make_orchestrator()
        report = orch.run(ENTITY_ID, PERIOD)
        # mock anomaly report 에 normal=[psr, conversion] 인데 anomaly 자체는
        # gmv + orders 둘 다 segmentable → unanalyzed = []
        assert report.unanalyzed == []


# ════════════════════════════════════════════════════════════════════
# 2. AgentBundle 도메인 키워드 주입 검증
# ════════════════════════════════════════════════════════════════════


class TestAgentBundleEcommerceKeywords:
    def test_hypothesis_has_ecommerce_keywords(self):
        orch, _ = _make_orchestrator()
        # AgentBundle.create("ecommerce") 가 ③⑤⑥ 에 ECOMMERCE 키워드 주입
        assert (
            orch._hypothesis_gen._domain_keywords
            is ECOMMERCE.agent_keywords
        )
        assert (
            orch._reasoner._domain_keywords
            is ECOMMERCE.agent_keywords
        )
        assert (
            orch._recommender._domain_keywords
            is ECOMMERCE.agent_keywords
        )

    def test_validator_has_ecommerce_allowed_tables(self):
        orch, _ = _make_orchestrator()
        assert (
            orch._validator._domain_allowed_tables
            == ECOMMERCE.allowed_tables
        )


# ════════════════════════════════════════════════════════════════════
# 3. ②③ 메서드에 entity_id 가 전달됐는지 spy
# ════════════════════════════════════════════════════════════════════


class TestEntityIdPropagation:
    def test_segmenter_called_with_entity_id(self):
        orch, _ = _make_orchestrator()
        orch.run(ENTITY_ID, PERIOD)
        # ② analyze 가 anomaly 별 호출됨 — 각 호출에 entity_id 가 위치 인자로
        for call in orch._segmenter.analyze.call_args_list:
            args, _kwargs = call
            assert args[0] == ENTITY_ID, (
                f"② analyze 의 entity_id 인자 어긋남: {args[0]!r}"
            )

    def test_hypothesis_gen_called_with_entity_id(self):
        orch, _ = _make_orchestrator()
        orch.run(ENTITY_ID, PERIOD)
        for call in orch._hypothesis_gen.generate.call_args_list:
            args, _kwargs = call
            assert args[0] == ENTITY_ID

    def test_repo_get_daily_kpi_called_with_entity_id(self):
        orch, repo = _make_orchestrator()
        orch.run(ENTITY_ID, PERIOD)
        repo.get_daily_kpi.assert_called_once_with(ENTITY_ID, PERIOD)


# ════════════════════════════════════════════════════════════════════
# 4. 6 에이전트 모두 호출됐는지
# ════════════════════════════════════════════════════════════════════


class TestAllAgentsInvoked:
    def test_each_agent_called_at_least_once(self):
        orch, _ = _make_orchestrator()
        orch.run(ENTITY_ID, PERIOD)
        assert orch._detector.detect.called
        assert orch._segmenter.analyze.called
        assert orch._hypothesis_gen.generate.called
        assert orch._validator.validate.called
        assert orch._reasoner.reason.called
        assert orch._recommender.recommend.called

    def test_per_anomaly_agents_called_twice(self):
        """anomaly 2 개 → ②~⑥ 가 각각 2 번 호출."""
        orch, _ = _make_orchestrator()
        orch.run(ENTITY_ID, PERIOD)
        assert orch._segmenter.analyze.call_count == 2
        assert orch._hypothesis_gen.generate.call_count == 2
        assert orch._validator.validate.call_count == 2
        assert orch._reasoner.reason.call_count == 2
        assert orch._recommender.recommend.call_count == 2


# ════════════════════════════════════════════════════════════════════
# 5. 시나리오 정답 키워드 매핑 (mock 출력 기준)
# ════════════════════════════════════════════════════════════════════


class TestScenarioAnswerMapping:
    """PRD §6 정답 키워드 셋 — 시나리오 B/C 의 인과 체인 마지막 노드 검증.

    실제 LLM 출력의 정답 도출은 manual eval 영역. 본 테스트는 mock 출력 기준
    매핑이 PipelineReport 에 보존되는지만 검증.
    """

    def test_orders_anomaly_resolved_to_inventory(self):
        orch, _ = _make_orchestrator()
        report = orch.run(ENTITY_ID, PERIOD)
        orders_analysis = next(
            a for a in report.analyzed if a.anomaly.metric == "orders"
        )
        summary = orders_analysis.root_cause.root_cause.summary
        assert "재고 부족" in summary

    def test_gmv_anomaly_resolved_to_promotion_end(self):
        orch, _ = _make_orchestrator()
        report = orch.run(ENTITY_ID, PERIOD)
        gmv_analysis = next(
            a for a in report.analyzed if a.anomaly.metric == "gmv"
        )
        summary = gmv_analysis.root_cause.root_cause.summary
        assert "프로모션 종료" in summary
