"""PipelineOrchestrator 단위 테스트.

검증 범위:
  1. Pydantic 모델 — AnomalyAnalysis, UnanalyzedAnomaly, PipelineReport 생성 + 기본값
  2. 데이터클래스 — PipelineStep 생성
  3. 오케스트레이터 통합 흐름 (모든 에이전트 Mock)
     - 정상 흐름: segmentable 2개 + non-segmentable 1개
     - 모든 지표 정상: anomalies 빈 배열
     - 전부 non-segmentable
     - on_step 콜백 호출 검증
     - available_schema 1회만 조회 검증
     - PipelineReport 필드 일치 검증

LLM API 실제 호출 없음. 에이전트 메서드를 MagicMock으로 직접 교체한다.

Java 비유:
    @ExtendWith(MockitoExtension.class)
    class PipelineOrchestratorTest {
        @Mock BottleneckDetector detector;
        @Mock SegmentationAnalyzer segmenter;
        // ... 6종 에이전트 모두 @Mock
        @InjectMocks PipelineOrchestrator orchestrator;
    }
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, Mock

import pytest
from pydantic import ValidationError

from datapilot.agents.action_recommender import Action, ActionPlan
from datapilot.agents.bottleneck_detector import AnomalyItem, AnomalyReport
from datapilot.agents.data_validator import ValidationResult
from datapilot.agents.hypothesis_generator import Hypothesis, HypothesisList
from datapilot.agents.root_cause_reasoner import (
    CausalStep,
    RootCause,
    RootCauseReport,
    UnverifiedHypothesis,
)
from datapilot.agents.segmentation_analyzer import SegmentConcentration, SegmentationReport
from datapilot.pipeline import (
    AnomalyAnalysis,
    PipelineOrchestrator,
    PipelineReport,
    PipelineStep,
    UnanalyzedAnomaly,
)
from datapilot.repository.port import GameDataRepository


# ──────────────────────────────────────────────────────────────────
# 헬퍼 — 최소 유효 객체 팩토리
# Java 비유: 빌더 패턴 기본값 세팅 메서드
# ──────────────────────────────────────────────────────────────────


def _make_anomaly_item(metric: str = "revenue", severity: str = "HIGH") -> AnomalyItem:
    """테스트용 AnomalyItem 생성. metric만 바꿔서 다양한 시나리오 지원."""
    return AnomalyItem(
        metric=metric,
        metric_label=f"{metric} 지표",
        change=-0.11,
        change_display="-11%",
        comparison_detail="-11.1% (직전 4일 평균 351,200 -> 최근 4일 평균 312,400)",
        severity=severity,
        reasoning="테스트용 판정 근거.",
    )


def _make_segmentation_report(metric: str = "revenue") -> SegmentationReport:
    """테스트용 SegmentationReport 생성."""
    return SegmentationReport(
        anomaly=metric,
        concentration=SegmentConcentration(
            dimension="platform",
            focus="android",
            change=-0.18,
        ),
        breakdown={"platform": {"android": -0.18, "ios": 0.002}},
        summary="매출 감소가 Android에 집중 (-18%, iOS는 +0.2%로 정상)",
        spread_type="concentrated",
    )


def _make_hypothesis_list(anomaly: str = "revenue") -> HypothesisList:
    """테스트용 HypothesisList 생성 (가설 2개)."""
    return HypothesisList(
        anomaly=anomaly,
        hypotheses=[
            Hypothesis(
                hypothesis="Android 상점 UI 변경으로 프리미엄 패키지 노출 감소",
                reasoning="UI 변경이 노출 순서를 바꿀 수 있음",
                required_tables=["shop_impressions"],
            ),
            Hypothesis(
                hypothesis="외부 광고 채널 변경",
                reasoning="광고비 효율 저하",
                required_tables=[],
                required_data="광고 플랫폼 데이터 필요",
            ),
        ],
    )


def _make_validation_results() -> list[ValidationResult]:
    """테스트용 ValidationResult 목록 (supported 1, unverified 1)."""
    return [
        ValidationResult(
            hypothesis="Android 상점 UI 변경으로 프리미엄 패키지 노출 감소",
            status="supported",
            evidence="shop_impressions -22%",
        ),
        ValidationResult(
            hypothesis="외부 광고 채널 변경",
            status="unverified",
            required_data="광고 플랫폼 데이터 필요",
        ),
    ]


def _make_root_cause_report(anomaly: str = "revenue") -> RootCauseReport:
    """테스트용 RootCauseReport 생성 (인과 체인 1단계)."""
    return RootCauseReport(
        anomaly=anomaly,
        root_cause=RootCause(
            chain=[CausalStep(step="Android UI 변경", evidence="shop_impressions -22%")],
            summary="Android UI 변경 → 매출 하락",
        ),
        additional_investigation=[
            UnverifiedHypothesis(
                hypothesis="외부 광고 채널 변경",
                required_data="광고 플랫폼 데이터 필요",
            )
        ],
    )


def _make_action_plan(anomaly: str = "revenue") -> ActionPlan:
    """테스트용 ActionPlan 생성 (액션 1개)."""
    return ActionPlan(
        anomaly=anomaly,
        actions=[
            Action(
                priority="urgent",
                title="Android v1.2.3 핫픽스 배포",
                effect="매출 즉시 회복",
                effort="Android 개발자 1명, 2일",
            )
        ],
    )


def _make_mock_repo(anomaly_report: AnomalyReport | None = None) -> MagicMock:
    """GameDataRepository 인터페이스 Mock 생성.

    Java 비유: Mockito.mock(GameDataRepository.class)
    """
    mock_repo = MagicMock(spec=GameDataRepository)
    mock_repo.get_daily_kpi.return_value = {
        "game_id": "pizza_ready",
        "period": {"from": "2025-03-25", "to": "2025-04-01"},
        "daily": [{"date": "2025-03-25", "revenue": 350000}],
    }
    mock_repo.get_available_schema.return_value = {
        "tables": [{"name": "payments", "columns": ["id", "amount"], "description": ""}]
    }
    return mock_repo


def _make_orchestrator_with_mocks(
    anomaly_report: AnomalyReport,
    segmentation_results: dict[str, SegmentationReport] | None = None,
    hypothesis_results: dict[str, HypothesisList] | None = None,
    validation_results: list[ValidationResult] | None = None,
    root_cause_results: dict[str, RootCauseReport] | None = None,
    action_plan_results: dict[str, ActionPlan] | None = None,
) -> tuple[PipelineOrchestrator, MagicMock]:
    """6종 에이전트가 Mock으로 대체된 PipelineOrchestrator + mock_repo 반환.

    Java 비유:
        PipelineOrchestrator orchestrator = new PipelineOrchestrator(mockRepo);
        orchestrator.detector = Mockito.mock(BottleneckDetector.class);
        // ... 각 에이전트 Mock 교체
    """
    mock_repo = _make_mock_repo()
    orchestrator = PipelineOrchestrator(repo=mock_repo)

    # ① Bottleneck Detector
    orchestrator._detector.detect = Mock(return_value=anomaly_report)

    # ② Segmentation Analyzer — metric별 반환값 지원
    if segmentation_results is not None:
        def _seg_side_effect(game_id, anomaly, period, repo):
            return segmentation_results[anomaly.metric]
        orchestrator._segmenter.analyze = Mock(side_effect=_seg_side_effect)
    else:
        orchestrator._segmenter.analyze = Mock(
            side_effect=lambda game_id, anomaly, period, repo: _make_segmentation_report(anomaly.metric)
        )

    # ③ Hypothesis Generator — metric별 반환값 지원
    if hypothesis_results is not None:
        def _hyp_side_effect(game_id, anomaly, segmentation, repo):
            return hypothesis_results[anomaly.metric]
        orchestrator._hypothesis_gen.generate = Mock(side_effect=_hyp_side_effect)
    else:
        orchestrator._hypothesis_gen.generate = Mock(
            side_effect=lambda game_id, anomaly, segmentation, repo: _make_hypothesis_list(anomaly.metric)
        )

    # ④ Data Validator
    vr = validation_results if validation_results is not None else _make_validation_results()
    orchestrator._validator.validate = Mock(return_value=vr)

    # ⑤ Root Cause Reasoner — metric별 반환값 지원
    if root_cause_results is not None:
        def _rc_side_effect(anomaly, segmentation, validation_results):
            return root_cause_results[anomaly.metric]
        orchestrator._reasoner.reason = Mock(side_effect=_rc_side_effect)
    else:
        orchestrator._reasoner.reason = Mock(
            side_effect=lambda anomaly, segmentation, validation_results: _make_root_cause_report(anomaly.metric)
        )

    # ⑥ Action Recommender — metric별 반환값 지원
    if action_plan_results is not None:
        def _ap_side_effect(root_cause_report):
            return action_plan_results[root_cause_report.anomaly]
        orchestrator._recommender.recommend = Mock(side_effect=_ap_side_effect)
    else:
        orchestrator._recommender.recommend = Mock(
            side_effect=lambda root_cause_report: _make_action_plan(root_cause_report.anomaly)
        )

    return orchestrator, mock_repo


_DEFAULT_PERIOD = (date(2025, 3, 25), date(2025, 4, 1))
_GAME_ID = "pizza_ready"


# ════════════════════════════════════════════════════════════════════
# 1. Pydantic 모델 — AnomalyAnalysis
# ════════════════════════════════════════════════════════════════════


class TestAnomalyAnalysisModel:
    """AnomalyAnalysis Pydantic 모델 생성 + 필드 접근 검증.

    Java 비유: DTO 직렬화/역직렬화 단위 테스트.
    """

    def test_creates_valid_anomaly_analysis(self):
        """모든 필수 필드를 채우면 AnomalyAnalysis가 정상 생성된다."""
        analysis = AnomalyAnalysis(
            anomaly=_make_anomaly_item(),
            segmentation=_make_segmentation_report(),
            hypotheses=_make_hypothesis_list(),
            validation_results=_make_validation_results(),
            root_cause=_make_root_cause_report(),
            action_plan=_make_action_plan(),
        )
        assert analysis.anomaly.metric == "revenue"

    def test_anomaly_analysis_preserves_all_fields(self):
        """AnomalyAnalysis의 6개 필드가 모두 보존된다."""
        seg = _make_segmentation_report()
        hyp = _make_hypothesis_list()
        vr = _make_validation_results()
        rc = _make_root_cause_report()
        ap = _make_action_plan()

        analysis = AnomalyAnalysis(
            anomaly=_make_anomaly_item(),
            segmentation=seg,
            hypotheses=hyp,
            validation_results=vr,
            root_cause=rc,
            action_plan=ap,
        )

        assert analysis.segmentation is seg
        assert analysis.hypotheses is hyp
        # Pydantic은 list[ValidationResult] 필드를 파싱 시 새 리스트로 복사하므로
        # 동일 객체(is) 대신 동등성(==)으로 검증한다.
        # Java 비유: assertEquals(expected, actual) — equals() 비교
        assert analysis.validation_results == vr
        assert analysis.root_cause is rc
        assert analysis.action_plan is ap

    def test_raises_validation_error_when_anomaly_missing(self):
        """anomaly 필드 누락 → ValidationError.

        Java 비유: @NotNull 위반 시 ConstraintViolationException.
        """
        with pytest.raises(ValidationError):
            AnomalyAnalysis(
                segmentation=_make_segmentation_report(),
                hypotheses=_make_hypothesis_list(),
                validation_results=[],
                root_cause=_make_root_cause_report(),
                action_plan=_make_action_plan(),
            )

    def test_raises_validation_error_when_action_plan_missing(self):
        """action_plan 필드 누락 → ValidationError."""
        with pytest.raises(ValidationError):
            AnomalyAnalysis(
                anomaly=_make_anomaly_item(),
                segmentation=_make_segmentation_report(),
                hypotheses=_make_hypothesis_list(),
                validation_results=[],
                root_cause=_make_root_cause_report(),
            )

    def test_validation_results_can_be_empty_list(self):
        """validation_results가 빈 리스트여도 허용된다."""
        analysis = AnomalyAnalysis(
            anomaly=_make_anomaly_item(),
            segmentation=_make_segmentation_report(),
            hypotheses=_make_hypothesis_list(),
            validation_results=[],
            root_cause=_make_root_cause_report(),
            action_plan=_make_action_plan(),
        )
        assert analysis.validation_results == []


# ════════════════════════════════════════════════════════════════════
# 2. Pydantic 모델 — UnanalyzedAnomaly
# ════════════════════════════════════════════════════════════════════


class TestUnanalyzedAnomalyModel:
    """UnanalyzedAnomaly — non-segmentable 이상 지표 보존 모델."""

    def test_creates_with_default_reason(self):
        """reason 기본값: '현재 세부 분석 미지원 지표'."""
        ua = UnanalyzedAnomaly(anomaly=_make_anomaly_item(metric="mau"))
        assert ua.reason == "현재 세부 분석 미지원 지표"

    def test_reason_can_be_overridden(self):
        """reason을 직접 지정할 수 있다."""
        ua = UnanalyzedAnomaly(
            anomaly=_make_anomaly_item(metric="sessions"),
            reason="세션 세그먼트 분석 미구현",
        )
        assert ua.reason == "세션 세그먼트 분석 미구현"

    def test_preserves_anomaly_item(self):
        """anomaly 필드에 전달한 AnomalyItem이 그대로 보존된다."""
        item = _make_anomaly_item(metric="mau")
        ua = UnanalyzedAnomaly(anomaly=item)
        assert ua.anomaly.metric == "mau"

    def test_raises_validation_error_when_anomaly_missing(self):
        """anomaly 필드 누락 → ValidationError."""
        with pytest.raises(ValidationError):
            UnanalyzedAnomaly()


# ════════════════════════════════════════════════════════════════════
# 3. Pydantic 모델 — PipelineReport
# ════════════════════════════════════════════════════════════════════


class TestPipelineReportModel:
    """PipelineReport — 파이프라인 전체 출력 모델."""

    def test_creates_with_required_fields_only(self):
        """game_id, period_from, period_to만으로 생성 — 나머지 기본값."""
        report = PipelineReport(
            game_id="pizza_ready",
            period_from="2025-03-25",
            period_to="2025-04-01",
        )
        assert report.game_id == "pizza_ready"
        assert report.analyzed == []
        assert report.unanalyzed == []
        assert report.normal_metrics == []

    def test_analyzed_defaults_to_empty_list(self):
        """analyzed 기본값은 빈 리스트."""
        report = PipelineReport(
            game_id="g1", period_from="2025-01-01", period_to="2025-01-07"
        )
        assert report.analyzed == []

    def test_unanalyzed_defaults_to_empty_list(self):
        """unanalyzed 기본값은 빈 리스트."""
        report = PipelineReport(
            game_id="g1", period_from="2025-01-01", period_to="2025-01-07"
        )
        assert report.unanalyzed == []

    def test_normal_metrics_defaults_to_empty_list(self):
        """normal_metrics 기본값은 빈 리스트."""
        report = PipelineReport(
            game_id="g1", period_from="2025-01-01", period_to="2025-01-07"
        )
        assert report.normal_metrics == []

    def test_creates_full_report(self):
        """모든 필드를 채운 PipelineReport 생성."""
        analysis = AnomalyAnalysis(
            anomaly=_make_anomaly_item(),
            segmentation=_make_segmentation_report(),
            hypotheses=_make_hypothesis_list(),
            validation_results=_make_validation_results(),
            root_cause=_make_root_cause_report(),
            action_plan=_make_action_plan(),
        )
        ua = UnanalyzedAnomaly(anomaly=_make_anomaly_item(metric="mau"))

        report = PipelineReport(
            game_id="pizza_ready",
            period_from="2025-03-25",
            period_to="2025-04-01",
            analyzed=[analysis],
            unanalyzed=[ua],
            normal_metrics=["dau", "sessions"],
        )

        assert len(report.analyzed) == 1
        assert len(report.unanalyzed) == 1
        assert "dau" in report.normal_metrics

    def test_raises_validation_error_when_game_id_missing(self):
        """game_id 누락 → ValidationError."""
        with pytest.raises(ValidationError):
            PipelineReport(period_from="2025-01-01", period_to="2025-01-07")

    def test_raises_validation_error_when_period_from_missing(self):
        """period_from 누락 → ValidationError."""
        with pytest.raises(ValidationError):
            PipelineReport(game_id="g1", period_to="2025-01-07")


# ════════════════════════════════════════════════════════════════════
# 4. 데이터클래스 — PipelineStep
# ════════════════════════════════════════════════════════════════════


class TestPipelineStepDataclass:
    """PipelineStep 데이터클래스 생성 + 필드 검증.

    Java 비유: @Data 어노테이션이 붙은 POJO 검증.
    Python의 @dataclass는 Java @Data(Lombok)와 동일하게
    __init__, __repr__, __eq__를 자동 생성한다.
    """

    def test_creates_with_all_fields(self):
        step = PipelineStep(agent="bottleneck", status="active", summary="탐지 중")
        assert step.agent == "bottleneck"
        assert step.status == "active"
        assert step.summary == "탐지 중"

    def test_summary_defaults_to_empty_string(self):
        """summary 기본값은 빈 문자열."""
        step = PipelineStep(agent="segmentation", status="done")
        assert step.summary == ""

    def test_equality_between_identical_steps(self):
        """동일한 필드값을 가진 PipelineStep 두 인스턴스는 동등하다.

        Java 비유: assertEquals(step1, step2) — @Data가 equals()를 생성하므로 통과.
        Python @dataclass도 eq=True(기본값)라 동일하게 동작.
        """
        step1 = PipelineStep(agent="bottleneck", status="done", summary="완료")
        step2 = PipelineStep(agent="bottleneck", status="done", summary="완료")
        assert step1 == step2

    def test_inequality_when_status_differs(self):
        step1 = PipelineStep(agent="bottleneck", status="active")
        step2 = PipelineStep(agent="bottleneck", status="done")
        assert step1 != step2


# ════════════════════════════════════════════════════════════════════
# 5. PipelineOrchestrator — 정상 흐름
#    segmentable 2개 + non-segmentable 1개
# ════════════════════════════════════════════════════════════════════


class TestPipelineRunHappyPath:
    """segmentable 이상 2개 + non-segmentable 이상 1개 시나리오.

    anomalies: revenue(segmentable), d7_retention(segmentable), mau(non-segmentable)
    기대값: analyzed 2개, unanalyzed 1개, ②~⑥ 각 2회 호출.

    Java 비유:
        @Test void happyPathWithMixedAnomalies() {
            // 3개 이상 발견 → segmentable 2개만 완전 분석, mau는 카드 보존
        }
    """

    @pytest.fixture
    def anomaly_report(self) -> AnomalyReport:
        return AnomalyReport(
            anomalies=[
                _make_anomaly_item(metric="revenue"),
                _make_anomaly_item(metric="d7_retention"),
                _make_anomaly_item(metric="mau"),  # non-segmentable
            ],
            normal=["dau", "sessions"],
        )

    @pytest.fixture
    def orchestrator_and_repo(self, anomaly_report):
        return _make_orchestrator_with_mocks(anomaly_report)

    def test_returns_pipeline_report(self, orchestrator_and_repo):
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert isinstance(result, PipelineReport)

    def test_analyzed_count_equals_segmentable_count(self, orchestrator_and_repo):
        """segmentable 이상(revenue, d7_retention) 2개 → analyzed 2개."""
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert len(result.analyzed) == 2

    def test_unanalyzed_count_equals_non_segmentable_count(self, orchestrator_and_repo):
        """non-segmentable 이상(mau) 1개 → unanalyzed 1개."""
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert len(result.unanalyzed) == 1

    def test_unanalyzed_metric_is_mau(self, orchestrator_and_repo):
        """unanalyzed에 mau가 담긴다."""
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert result.unanalyzed[0].anomaly.metric == "mau"

    def test_analyzed_metrics_are_segmentable(self, orchestrator_and_repo):
        """analyzed에 revenue, d7_retention만 포함된다."""
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        analyzed_metrics = {a.anomaly.metric for a in result.analyzed}
        assert analyzed_metrics == {"revenue", "d7_retention"}

    def test_segmenter_called_twice(self, orchestrator_and_repo):
        """② 세그먼트 분석이 segmentable 이상 수(2)만큼 호출된다.

        Java 비유: verify(mockSegmenter, times(2)).analyze(...)
        """
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert orchestrator._segmenter.analyze.call_count == 2

    def test_hypothesis_gen_called_twice(self, orchestrator_and_repo):
        """③ 가설 생성이 2회 호출된다."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert orchestrator._hypothesis_gen.generate.call_count == 2

    def test_validator_called_twice(self, orchestrator_and_repo):
        """④ 데이터 검증이 2회 호출된다."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert orchestrator._validator.validate.call_count == 2

    def test_reasoner_called_twice(self, orchestrator_and_repo):
        """⑤ 원인 추론이 2회 호출된다."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert orchestrator._reasoner.reason.call_count == 2

    def test_recommender_called_twice(self, orchestrator_and_repo):
        """⑥ 액션 추천이 2회 호출된다."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert orchestrator._recommender.recommend.call_count == 2

    def test_normal_metrics_preserved(self, orchestrator_and_repo):
        """AnomalyReport.normal이 PipelineReport.normal_metrics로 전달된다."""
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert "dau" in result.normal_metrics
        assert "sessions" in result.normal_metrics

    def test_unanalyzed_has_default_reason(self, orchestrator_and_repo):
        """non-segmentable 이상의 reason이 기본값이다."""
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert result.unanalyzed[0].reason == "현재 세부 분석 미지원 지표"


# ════════════════════════════════════════════════════════════════════
# 6. 모든 지표 정상 — anomalies 빈 배열
# ════════════════════════════════════════════════════════════════════


class TestPipelineRunAllNormal:
    """anomalies가 빈 배열일 때: 에이전트 ②~⑥ 호출 0회.

    Java 비유:
        @Test void shouldSkipAnalysisWhenNoAnomalies() {
            verify(mockSegmenter, never()).analyze(any());
        }
    """

    @pytest.fixture
    def orchestrator_and_repo(self):
        anomaly_report = AnomalyReport(
            anomalies=[],
            normal=["revenue", "dau", "mau", "sessions"],
        )
        return _make_orchestrator_with_mocks(anomaly_report)

    def test_returns_pipeline_report(self, orchestrator_and_repo):
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert isinstance(result, PipelineReport)

    def test_analyzed_is_empty(self, orchestrator_and_repo):
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert result.analyzed == []

    def test_unanalyzed_is_empty(self, orchestrator_and_repo):
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert result.unanalyzed == []

    def test_segmenter_not_called(self, orchestrator_and_repo):
        """이상 없음 → ② 세그먼트 분석 미호출."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._segmenter.analyze.assert_not_called()

    def test_hypothesis_gen_not_called(self, orchestrator_and_repo):
        """③ 가설 생성 미호출."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._hypothesis_gen.generate.assert_not_called()

    def test_validator_not_called(self, orchestrator_and_repo):
        """④ 데이터 검증 미호출."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._validator.validate.assert_not_called()

    def test_reasoner_not_called(self, orchestrator_and_repo):
        """⑤ 원인 추론 미호출."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._reasoner.reason.assert_not_called()

    def test_recommender_not_called(self, orchestrator_and_repo):
        """⑥ 액션 추천 미호출."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._recommender.recommend.assert_not_called()

    def test_normal_metrics_all_preserved(self, orchestrator_and_repo):
        """정상 지표 4개가 모두 normal_metrics에 포함된다."""
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert set(result.normal_metrics) == {"revenue", "dau", "mau", "sessions"}


# ════════════════════════════════════════════════════════════════════
# 7. 전부 non-segmentable
# ════════════════════════════════════════════════════════════════════


class TestPipelineRunAllNonSegmentable:
    """모든 이상 지표가 non-segmentable일 때.

    anomalies: mau, sessions (둘 다 SUPPORTED_SEGMENT_METRICS 밖)
    기대: analyzed 0개, unanalyzed 2개, ②~⑥ 호출 0회.

    Java 비유:
        @Test void shouldPreserveAllAnomaliesAsUnanalyzedWhenNoneSegmentable() { ... }
    """

    @pytest.fixture
    def orchestrator_and_repo(self):
        anomaly_report = AnomalyReport(
            anomalies=[
                _make_anomaly_item(metric="mau"),
                _make_anomaly_item(metric="sessions"),
            ],
            normal=["revenue"],
        )
        return _make_orchestrator_with_mocks(anomaly_report)

    def test_analyzed_is_empty(self, orchestrator_and_repo):
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert result.analyzed == []

    def test_unanalyzed_count_is_two(self, orchestrator_and_repo):
        """non-segmentable 2개 → unanalyzed 2개."""
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert len(result.unanalyzed) == 2

    def test_unanalyzed_metrics_are_mau_and_sessions(self, orchestrator_and_repo):
        orchestrator, _ = orchestrator_and_repo
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        unanalyzed_metrics = {u.anomaly.metric for u in result.unanalyzed}
        assert unanalyzed_metrics == {"mau", "sessions"}

    def test_segmenter_not_called(self, orchestrator_and_repo):
        """non-segmentable만 존재 → ② 세그먼트 분석 미호출."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._segmenter.analyze.assert_not_called()

    def test_validator_not_called(self, orchestrator_and_repo):
        """④ 데이터 검증 미호출."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._validator.validate.assert_not_called()

    def test_recommender_not_called(self, orchestrator_and_repo):
        """⑥ 액션 추천 미호출."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._recommender.recommend.assert_not_called()

    def test_detector_called_once(self, orchestrator_and_repo):
        """① 탐지는 항상 1회만 호출된다."""
        orchestrator, _ = orchestrator_and_repo
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        orchestrator._detector.detect.assert_called_once()


# ════════════════════════════════════════════════════════════════════
# 8. on_step 콜백 호출 검증
# ════════════════════════════════════════════════════════════════════


class TestPipelineOnStepCallback:
    """on_step 콜백이 올바른 순서와 인자로 호출되는지 검증.

    Java 비유:
        ArgumentCaptor<PipelineStep> captor = ...;
        verify(mockCallback, times(N)).accept(captor.capture());
        List<PipelineStep> steps = captor.getAllValues();
    """

    def _run_with_callback(self, anomaly_report: AnomalyReport) -> list[PipelineStep]:
        """파이프라인을 실행하고 콜백에 전달된 PipelineStep 목록을 반환."""
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        captured_steps: list[PipelineStep] = []

        def capture_step(step: PipelineStep) -> None:
            captured_steps.append(step)

        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=capture_step)
        return captured_steps

    def test_bottleneck_active_is_first_step(self):
        """파이프라인 첫 콜백은 bottleneck active이다."""
        anomaly_report = AnomalyReport(anomalies=[], normal=[])
        steps = self._run_with_callback(anomaly_report)
        assert steps[0].agent == "bottleneck"
        assert steps[0].status == "active"

    def test_bottleneck_done_is_second_step(self):
        """두 번째 콜백은 bottleneck done이다."""
        anomaly_report = AnomalyReport(anomalies=[], normal=[])
        steps = self._run_with_callback(anomaly_report)
        assert steps[1].agent == "bottleneck"
        assert steps[1].status == "done"

    def test_bottleneck_done_summary_contains_anomaly_count(self):
        """bottleneck done 요약에 이상 지표 수가 포함된다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        steps = self._run_with_callback(anomaly_report)
        done_step = next(s for s in steps if s.agent == "bottleneck" and s.status == "done")
        assert "1" in done_step.summary

    def test_no_step_when_on_step_is_none(self):
        """on_step=None이면 에러 없이 실행된다."""
        anomaly_report = AnomalyReport(anomalies=[], normal=[])
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        # 예외 없이 실행되는지 확인
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=None)
        assert isinstance(result, PipelineReport)

    def test_segmentation_active_and_done_called_per_anomaly(self):
        """segmentable 이상 2개 → segmentation active/done이 각 2회씩 호출된다."""
        anomaly_report = AnomalyReport(
            anomalies=[
                _make_anomaly_item(metric="revenue"),
                _make_anomaly_item(metric="d7_retention"),
            ],
            normal=[],
        )
        steps = self._run_with_callback(anomaly_report)
        seg_active = [s for s in steps if s.agent == "segmentation" and s.status == "active"]
        seg_done = [s for s in steps if s.agent == "segmentation" and s.status == "done"]
        assert len(seg_active) == 2
        assert len(seg_done) == 2

    def test_all_agent_types_appear_in_steps_for_one_segmentable(self):
        """segmentable 이상 1개 → bottleneck, segmentation, hypothesis, validation, root_cause, action 모두 등장."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        steps = self._run_with_callback(anomaly_report)
        agent_names = {s.agent for s in steps}
        assert "bottleneck" in agent_names
        assert "segmentation" in agent_names
        assert "hypothesis" in agent_names
        assert "validation" in agent_names
        assert "root_cause" in agent_names
        assert "action" in agent_names

    def test_each_agent_has_active_then_done_order(self):
        """각 에이전트 유형에서 active가 done보다 먼저 등장한다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        steps = self._run_with_callback(anomaly_report)

        for agent_name in ["bottleneck", "segmentation", "hypothesis", "validation", "root_cause", "action"]:
            agent_steps = [s for s in steps if s.agent == agent_name]
            # active가 done보다 먼저 나와야 함
            statuses = [s.status for s in agent_steps]
            active_idx = statuses.index("active")
            done_idx = statuses.index("done")
            assert active_idx < done_idx, f"{agent_name}: active가 done보다 먼저여야 함"

    def test_callback_receives_pipeline_step_instances(self):
        """콜백에 전달되는 객체가 PipelineStep 인스턴스다."""
        anomaly_report = AnomalyReport(anomalies=[], normal=[])
        steps = self._run_with_callback(anomaly_report)
        assert all(isinstance(s, PipelineStep) for s in steps)

    def test_validation_done_summary_contains_status_counts(self):
        """validation done 요약에 확인/기각/미검증 수치가 포함된다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        steps = self._run_with_callback(anomaly_report)
        val_done = next(s for s in steps if s.agent == "validation" and s.status == "done")
        # "확인 X / 기각 Y / 미검증 Z" 형식이므로 각 숫자가 포함됨
        assert "확인" in val_done.summary
        assert "기각" in val_done.summary
        assert "미검증" in val_done.summary


# ════════════════════════════════════════════════════════════════════
# 9. available_schema 1회만 조회 검증
# ════════════════════════════════════════════════════════════════════


class TestPipelineSchemaFetchedOnce:
    """available_schema는 segmentable 이상이 여러 개여도 1회만 조회된다.

    DB I/O 최적화: 스키마는 루프 밖에서 한 번 조회해 공유.

    Java 비유:
        verify(mockRepo, times(1)).getAvailableSchema(anyString());
    """

    def test_schema_fetched_once_for_two_segmentable_anomalies(self):
        """segmentable 이상 2개 → get_available_schema 1회만 호출."""
        anomaly_report = AnomalyReport(
            anomalies=[
                _make_anomaly_item(metric="revenue"),
                _make_anomaly_item(metric="d7_retention"),
            ],
            normal=[],
        )
        orchestrator, mock_repo = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        mock_repo.get_available_schema.assert_called_once()

    def test_schema_fetched_once_even_when_no_anomalies(self):
        """이상 없어도 get_available_schema는 1회 호출된다.

        (분류 단계 이후 스키마 조회는 anomalies 유무와 무관하게 항상 실행됨)
        """
        anomaly_report = AnomalyReport(anomalies=[], normal=["revenue"])
        orchestrator, mock_repo = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        mock_repo.get_available_schema.assert_called_once()

    def test_schema_fetch_called_with_game_id(self):
        """get_available_schema가 올바른 game_id로 호출된다."""
        anomaly_report = AnomalyReport(anomalies=[], normal=[])
        orchestrator, mock_repo = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator.run("my_game", _DEFAULT_PERIOD)
        mock_repo.get_available_schema.assert_called_once_with("my_game")

    def test_kpi_fetch_called_once(self):
        """get_daily_kpi도 1회만 호출된다."""
        anomaly_report = AnomalyReport(anomalies=[], normal=[])
        orchestrator, mock_repo = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        mock_repo.get_daily_kpi.assert_called_once()


# ════════════════════════════════════════════════════════════════════
# 10. PipelineReport 필드 — game_id, period 일치 검증
# ════════════════════════════════════════════════════════════════════


class TestPipelineReportFields:
    """run() 반환 PipelineReport의 필드가 입력값과 일치하는지 검증.

    Java 비유:
        assertEquals("pizza_ready", result.getGameId());
        assertEquals("2025-03-25", result.getPeriodFrom());
    """

    @pytest.fixture
    def result(self) -> PipelineReport:
        anomaly_report = AnomalyReport(anomalies=[], normal=[])
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        return orchestrator.run("pizza_ready", (date(2025, 3, 25), date(2025, 4, 1)))

    def test_game_id_matches_input(self, result):
        assert result.game_id == "pizza_ready"

    def test_period_from_matches_start_date(self, result):
        """period_from은 ISO format 문자열 "2025-03-25"이어야 한다."""
        assert result.period_from == "2025-03-25"

    def test_period_to_matches_end_date(self, result):
        """period_to는 ISO format 문자열 "2025-04-01"이어야 한다."""
        assert result.period_to == "2025-04-01"

    def test_period_from_and_to_are_iso_format_strings(self, result):
        """period_from/to는 YYYY-MM-DD 형식이어야 한다."""
        # date.fromisoformat()으로 파싱 가능하면 올바른 ISO 형식
        from datetime import date as date_cls
        date_cls.fromisoformat(result.period_from)
        date_cls.fromisoformat(result.period_to)

    def test_different_game_id_propagates_to_report(self):
        """다른 game_id로 실행해도 report에 정확히 반영된다."""
        anomaly_report = AnomalyReport(anomalies=[], normal=[])
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        result = orchestrator.run("another_game", _DEFAULT_PERIOD)
        assert result.game_id == "another_game"


# ════════════════════════════════════════════════════════════════════
# 11. _analyze_one 흐름 — 단일 segmentable 이상 완전 분석
# ════════════════════════════════════════════════════════════════════


class TestAnalyzeOnePipeline:
    """_analyze_one이 ②~⑥을 순서대로 호출하고 AnomalyAnalysis를 반환하는지 검증.

    run()을 통해 간접적으로 검증한다 (private 메서드 직접 호출 지양).

    Java 비유: Spring의 @Service 메서드 테스트 — 내부 로직을 통합적으로 검증.
    """

    @pytest.fixture
    def result(self) -> PipelineReport:
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="payment_success_rate")],
            normal=["dau"],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        return orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)

    def test_single_segmentable_produces_one_analyzed(self, result):
        assert len(result.analyzed) == 1

    def test_analyzed_anomaly_metric_matches(self, result):
        assert result.analyzed[0].anomaly.metric == "payment_success_rate"

    def test_analyzed_contains_segmentation_report(self, result):
        """analyzed[0].segmentation은 SegmentationReport 인스턴스다."""
        assert isinstance(result.analyzed[0].segmentation, SegmentationReport)

    def test_analyzed_contains_hypothesis_list(self, result):
        """analyzed[0].hypotheses는 HypothesisList 인스턴스다."""
        assert isinstance(result.analyzed[0].hypotheses, HypothesisList)

    def test_analyzed_contains_validation_results(self, result):
        """analyzed[0].validation_results는 리스트다."""
        assert isinstance(result.analyzed[0].validation_results, list)

    def test_analyzed_contains_root_cause_report(self, result):
        """analyzed[0].root_cause는 RootCauseReport 인스턴스다."""
        assert isinstance(result.analyzed[0].root_cause, RootCauseReport)

    def test_analyzed_contains_action_plan(self, result):
        """analyzed[0].action_plan은 ActionPlan 인스턴스다."""
        assert isinstance(result.analyzed[0].action_plan, ActionPlan)

    def test_segmenter_called_with_correct_anomaly(self):
        """② 세그먼트 분석이 올바른 anomaly 인자로 호출된다."""
        anomaly_item = _make_anomaly_item(metric="dau")
        anomaly_report = AnomalyReport(anomalies=[anomaly_item], normal=[])
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)

        call_args = orchestrator._segmenter.analyze.call_args
        # 두 번째 인자(positional)가 anomaly
        assert call_args[0][1].metric == "dau"

    def test_validator_receives_hypothesis_list_and_schema(self):
        """④ 검증기가 HypothesisList와 available_schema를 받는다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)

        call_args = orchestrator._validator.validate.call_args
        hypothesis_arg = call_args[0][0]
        schema_arg = call_args[0][1]
        assert isinstance(hypothesis_arg, HypothesisList)
        assert "tables" in schema_arg

    def test_recommender_receives_root_cause_report(self):
        """⑥ 액션 추천기가 RootCauseReport를 받는다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)

        call_args = orchestrator._recommender.recommend.call_args
        root_cause_arg = call_args[0][0]
        assert isinstance(root_cause_arg, RootCauseReport)


# ════════════════════════════════════════════════════════════════════
# 12. SUPPORTED_SEGMENT_METRICS 분류 경계 검증
# ════════════════════════════════════════════════════════════════════


class TestSegmentableClassification:
    """SUPPORTED_SEGMENT_METRICS 기준으로 segmentable/non-segmentable이 정확히 분류되는지 검증.

    4개 지원 metric: revenue, dau, payment_success_rate, d7_retention
    그 외: mau, sessions, arppu, d1_retention 등 → non-segmentable
    """

    def _run_and_get_report(self, metrics: list[str]) -> PipelineReport:
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric=m) for m in metrics],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        return orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)

    def test_revenue_is_segmentable(self):
        result = self._run_and_get_report(["revenue"])
        assert len(result.analyzed) == 1
        assert len(result.unanalyzed) == 0

    def test_dau_is_segmentable(self):
        result = self._run_and_get_report(["dau"])
        assert len(result.analyzed) == 1
        assert len(result.unanalyzed) == 0

    def test_payment_success_rate_is_segmentable(self):
        result = self._run_and_get_report(["payment_success_rate"])
        assert len(result.analyzed) == 1
        assert len(result.unanalyzed) == 0

    def test_d7_retention_is_segmentable(self):
        result = self._run_and_get_report(["d7_retention"])
        assert len(result.analyzed) == 1
        assert len(result.unanalyzed) == 0

    def test_mau_is_non_segmentable(self):
        result = self._run_and_get_report(["mau"])
        assert len(result.analyzed) == 0
        assert len(result.unanalyzed) == 1

    def test_sessions_is_non_segmentable(self):
        result = self._run_and_get_report(["sessions"])
        assert len(result.analyzed) == 0
        assert len(result.unanalyzed) == 1

    def test_arppu_is_non_segmentable(self):
        result = self._run_and_get_report(["arppu"])
        assert len(result.analyzed) == 0
        assert len(result.unanalyzed) == 1

    def test_d1_retention_is_non_segmentable(self):
        result = self._run_and_get_report(["d1_retention"])
        assert len(result.analyzed) == 0
        assert len(result.unanalyzed) == 1

    def test_all_four_supported_metrics_analyzed(self):
        """4개 지원 metric 전부 이상 → analyzed 4개."""
        result = self._run_and_get_report(
            ["revenue", "dau", "payment_success_rate", "d7_retention"]
        )
        assert len(result.analyzed) == 4
        assert len(result.unanalyzed) == 0

    def test_mixed_four_segmentable_two_non_segmentable(self):
        """segmentable 4개 + non-segmentable 2개 → analyzed 4, unanalyzed 2."""
        result = self._run_and_get_report(
            ["revenue", "dau", "payment_success_rate", "d7_retention", "mau", "sessions"]
        )
        assert len(result.analyzed) == 4
        assert len(result.unanalyzed) == 2


# ════════════════════════════════════════════════════════════════════
# 13. PipelineStep.metric 필드 검증
# ════════════════════════════════════════════════════════════════════


class TestPipelineStepMetricField:
    """PipelineStep.metric 필드가 콜백에서 올바르게 전달되는지 검증.

    Phase 8 UI에서 "② 세그먼트 분석: 진행 중 (2/3)" 같은 표시를 위해
    어떤 이상 지표에 대한 스텝인지 식별해야 한다.

    Java 비유: captor.getValue().getMetric() 으로 인자 확인.
    """

    def test_metric_defaults_to_empty_string(self):
        """metric 기본값은 빈 문자열."""
        step = PipelineStep(agent="bottleneck", status="active")
        assert step.metric == ""

    def test_metric_field_assigned(self):
        step = PipelineStep(agent="segmentation", status="active", metric="revenue")
        assert step.metric == "revenue"

    def test_bottleneck_steps_have_empty_metric(self):
        """① 병목 탐지는 전체 대상이므로 metric이 빈 문자열."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        steps: list[PipelineStep] = []
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=steps.append)

        bottleneck_steps = [s for s in steps if s.agent == "bottleneck"]
        assert all(s.metric == "" for s in bottleneck_steps)

    def test_segmentation_steps_carry_metric(self):
        """② 세그먼트 분석 콜백에 해당 metric이 전달된다."""
        anomaly_report = AnomalyReport(
            anomalies=[
                _make_anomaly_item(metric="revenue"),
                _make_anomaly_item(metric="d7_retention"),
            ],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        steps: list[PipelineStep] = []
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=steps.append)

        seg_steps = [s for s in steps if s.agent == "segmentation"]
        seg_metrics = [s.metric for s in seg_steps]
        assert "revenue" in seg_metrics
        assert "d7_retention" in seg_metrics

    def test_all_analyze_steps_carry_same_metric_per_anomaly(self):
        """하나의 이상 지표에 대한 ②~⑥ 스텝이 모두 동일한 metric을 갖는다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        steps: list[PipelineStep] = []
        orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=steps.append)

        analyze_steps = [s for s in steps if s.agent != "bottleneck"]
        assert all(s.metric == "revenue" for s in analyze_steps)


# ════════════════════════════════════════════════════════════════════
# 14. anomaly_order 필드 검증
# ════════════════════════════════════════════════════════════════════


class TestPipelineAnomalyOrder:
    """PipelineReport.anomaly_order가 ① 탐지 순서를 보존하는지 검증.

    segmentable/non-segmentable 분류로 순서가 깨지지 않도록
    원본 순서를 별도 필드에 기록한다.

    Java 비유: assertEquals(List.of("revenue", "mau", "d7_retention"), report.getAnomalyOrder());
    """

    def test_order_preserves_detection_sequence(self):
        """① 탐지 순서 [revenue, mau, d7_retention]이 그대로 보존된다."""
        anomaly_report = AnomalyReport(
            anomalies=[
                _make_anomaly_item(metric="revenue"),
                _make_anomaly_item(metric="mau"),
                _make_anomaly_item(metric="d7_retention"),
            ],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert result.anomaly_order == ["revenue", "mau", "d7_retention"]

    def test_order_empty_when_no_anomalies(self):
        anomaly_report = AnomalyReport(anomalies=[], normal=["dau"])
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert result.anomaly_order == []

    def test_order_includes_both_segmentable_and_non_segmentable(self):
        """segmentable과 non-segmentable이 섞여도 원본 순서 유지."""
        anomaly_report = AnomalyReport(
            anomalies=[
                _make_anomaly_item(metric="mau"),       # non-seg
                _make_anomaly_item(metric="revenue"),    # seg
                _make_anomaly_item(metric="sessions"),   # non-seg
                _make_anomaly_item(metric="dau"),        # seg
            ],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        result = orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)
        assert result.anomaly_order == ["mau", "revenue", "sessions", "dau"]

    def test_order_defaults_to_empty_in_model(self):
        """PipelineReport 직접 생성 시 기본값은 빈 리스트."""
        report = PipelineReport(
            game_id="g1", period_from="2025-01-01", period_to="2025-01-07",
        )
        assert report.anomaly_order == []


# ════════════════════════════════════════════════════════════════════
# 15. 에러 핸들링 — error 콜백 + 예외 재전파
# ════════════════════════════════════════════════════════════════════


class TestPipelineErrorHandling:
    """에이전트 실패 시 error 콜백이 호출되고 예외가 재전파되는지 검증.

    screen-spec 2.4: 실패한 스텝의 아이콘을 빨강 X로 변경,
    상태 텍스트: "실패 - {에러 메시지}"

    Java 비유:
        assertThrows(RuntimeException.class, () -> orchestrator.run(...));
        verify(mockCallback).accept(argThat(s -> s.getStatus().equals("error")));
    """

    def test_segmenter_error_triggers_error_callback(self):
        """② 세그먼트 분석 실패 → error 콜백 호출."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator._segmenter.analyze.side_effect = RuntimeError("API timeout")

        steps: list[PipelineStep] = []
        with pytest.raises(RuntimeError, match="API timeout"):
            orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=steps.append)

        error_steps = [s for s in steps if s.status == "error"]
        assert len(error_steps) == 1
        assert error_steps[0].agent == "segmentation"
        assert "API timeout" in error_steps[0].summary

    def test_hypothesis_error_triggers_error_callback(self):
        """③ 가설 생성 실패 → error 콜백에 hypothesis agent가 기록된다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator._hypothesis_gen.generate.side_effect = RuntimeError("LLM error")

        steps: list[PipelineStep] = []
        with pytest.raises(RuntimeError):
            orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=steps.append)

        error_steps = [s for s in steps if s.status == "error"]
        assert error_steps[0].agent == "hypothesis"

    def test_error_callback_carries_metric(self):
        """에러 콜백에도 metric 필드가 전달된다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="d7_retention")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator._reasoner.reason.side_effect = RuntimeError("fail")

        steps: list[PipelineStep] = []
        with pytest.raises(RuntimeError):
            orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=steps.append)

        error_steps = [s for s in steps if s.status == "error"]
        assert error_steps[0].metric == "d7_retention"

    def test_exception_propagates_after_error_callback(self):
        """에러 콜백 호출 후 예외가 그대로 재전파된다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        orchestrator._recommender.recommend.side_effect = ValueError("bad input")

        with pytest.raises(ValueError, match="bad input"):
            orchestrator.run(_GAME_ID, _DEFAULT_PERIOD)

    def test_successful_steps_before_error_are_recorded(self):
        """에러 발생 전 완료된 스텝들은 정상 기록된다."""
        anomaly_report = AnomalyReport(
            anomalies=[_make_anomaly_item(metric="revenue")],
            normal=[],
        )
        orchestrator, _ = _make_orchestrator_with_mocks(anomaly_report)
        # ④ 검증에서 실패
        orchestrator._validator.validate.side_effect = RuntimeError("DB error")

        steps: list[PipelineStep] = []
        with pytest.raises(RuntimeError):
            orchestrator.run(_GAME_ID, _DEFAULT_PERIOD, on_step=steps.append)

        # ①②③ 는 done까지 완료, ④에서 error
        done_agents = [s.agent for s in steps if s.status == "done"]
        assert "bottleneck" in done_agents
        assert "segmentation" in done_agents
        assert "hypothesis" in done_agents
        assert "validation" not in done_agents
