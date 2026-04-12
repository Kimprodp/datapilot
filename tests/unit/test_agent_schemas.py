"""에이전트 Pydantic 스키마 + 순수 함수 단위 테스트.

검증 범위:
  - ① AnomalyItem / AnomalyReport 스키마
  - ② SegmentConcentration / SegmentationReport 스키마
  - ③ Hypothesis / HypothesisList 스키마
  - ④ ValidationResult 스키마
  - ⑤ CausalStep / RootCause / UnverifiedHypothesis / RootCauseReport 스키마
  - ⑤ prepare_input() 순수 함수 — 3상태 분리
  - ⑥ Action / ActionPlan 스키마
  - ⑥ prepare_input() 순수 함수 — is_unknown_cause 플래그
  - config 상수 존재 여부

LLM 호출 없음. 모든 테스트는 순수 Python/Pydantic 레벨에서 실행된다.

Java 비유:
    JUnit5 단위 테스트 — @Test + assertThrows / assertEquals
    Pydantic ValidationError ≈ Bean Validation의 ConstraintViolationException
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

# ──────────────────────────────────────────────────────────────────
# import 대상
# ──────────────────────────────────────────────────────────────────

from datapilot.config import ANTHROPIC_API_KEY, OPUS_MODEL, SONNET_MODEL
from datapilot.agents.bottleneck_detector import AnomalyItem, AnomalyReport
from datapilot.agents.segmentation_analyzer import (
    SegmentConcentration,
    SegmentationReport,
)
from datapilot.agents.hypothesis_generator import Hypothesis, HypothesisList
from datapilot.agents.data_validator import ValidationResult
from datapilot.agents.root_cause_reasoner import (
    CausalStep,
    RootCause,
    RootCauseReport,
    UnverifiedHypothesis,
    prepare_input as root_prepare_input,
)
from datapilot.agents.action_recommender import (
    Action,
    ActionPlan,
    prepare_input as action_prepare_input,
)


# ──────────────────────────────────────────────────────────────────
# 헬퍼 — 최소 유효 객체 팩토리
# ──────────────────────────────────────────────────────────────────

def _make_anomaly_item(**overrides) -> AnomalyItem:
    """테스트용 AnomalyItem 기본값 생성.

    Java 비유: 빌더 패턴 기본값 세팅 (AnomalyItemBuilder.defaults())
    """
    defaults = {
        "metric": "revenue",
        "metric_label": "인앱결제 매출 (revenue)",
        "change": -0.11,
        "change_display": "-11%",
        "comparison_detail": "-11.1% (직전 4일 평균 351,200 -> 최근 4일 평균 312,400)",
        "severity": "HIGH",
        "reasoning": "D-3 Android 배포 이후 매출 지속 하락.",
    }
    defaults.update(overrides)
    return AnomalyItem(**defaults)


def _make_segmentation_report(**overrides) -> SegmentationReport:
    defaults = {
        "anomaly": "revenue",
        "concentration": SegmentConcentration(
            dimension="platform", focus="android", change=-0.18
        ),
        "breakdown": {"platform": {"android": -0.18, "ios": 0.002}},
        "summary": "매출 감소가 Android에 집중 (-18%, iOS는 +0.2%로 정상)",
        "spread_type": "concentrated",
    }
    defaults.update(overrides)
    return SegmentationReport(**defaults)


def _make_validation_result(status: str, **overrides) -> ValidationResult:
    defaults = {
        "hypothesis": "Android 상점 UI 변경으로 프리미엄 패키지 노출 감소",
        "status": status,
        "evidence": "매출 -18%" if status == "supported" else None,
    }
    defaults.update(overrides)
    return ValidationResult(**defaults)


def _make_root_cause_report(chain_empty: bool = False) -> RootCauseReport:
    if chain_empty:
        chain = []
        summary = "원인 불명"
    else:
        chain = [
            CausalStep(step="Android 상점 UI 변경", evidence="payments 테이블 매출 -18%"),
            CausalStep(step="프리미엄 패키지 노출 감소", evidence="shop_impressions 건수 -22%"),
        ]
        summary = "Android 상점 UI 변경 -> 프리미엄 패키지 노출 감소 -> 매출 하락"

    return RootCauseReport(
        anomaly="revenue",
        root_cause=RootCause(chain=chain, summary=summary),
        additional_investigation=[
            UnverifiedHypothesis(
                hypothesis="광고 채널 변경으로 신규 유저 품질 하락",
                required_data="광고 플랫폼 퍼포먼스 데이터 (외부)",
            )
        ],
    )


# ════════════════════════════════════════════════════════════════════
# config 상수
# ════════════════════════════════════════════════════════════════════


class TestConfig:
    """config.py에서 export되는 상수 존재 여부 확인.

    Java 비유: @Value 주입이 정상인지 테스트하는 것과 동일.
    실제 API 키 값은 검사하지 않고 "상수가 str 타입으로 존재하는가"만 확인.
    """

    def test_anthropic_api_key_is_str(self):
        assert isinstance(ANTHROPIC_API_KEY, str)

    def test_sonnet_model_is_non_empty_str(self):
        assert isinstance(SONNET_MODEL, str)
        assert len(SONNET_MODEL) > 0

    def test_opus_model_is_non_empty_str(self):
        assert isinstance(OPUS_MODEL, str)
        assert len(OPUS_MODEL) > 0

    def test_sonnet_and_opus_are_different_models(self):
        """Sonnet과 Opus는 서로 다른 모델 ID여야 한다."""
        assert SONNET_MODEL != OPUS_MODEL


# ════════════════════════════════════════════════════════════════════
# ① AnomalyItem / AnomalyReport
# ════════════════════════════════════════════════════════════════════


class TestAnomalyItem:
    """① Bottleneck Detector 출력 스키마 검증."""

    def test_creates_valid_anomaly_item(self):
        item = _make_anomaly_item()
        assert item.metric == "revenue"
        assert item.severity == "HIGH"

    def test_severity_accepts_high(self):
        item = _make_anomaly_item(severity="HIGH")
        assert item.severity == "HIGH"

    def test_severity_accepts_medium(self):
        item = _make_anomaly_item(severity="MEDIUM")
        assert item.severity == "MEDIUM"

    def test_severity_accepts_low(self):
        item = _make_anomaly_item(severity="LOW")
        assert item.severity == "LOW"

    def test_raises_validation_error_when_severity_is_invalid(self):
        """severity에 허용되지 않는 값 → ValidationError.

        Java 비유: @Pattern 어노테이션 위반 시 ConstraintViolationException.
        """
        with pytest.raises(ValidationError):
            _make_anomaly_item(severity="CRITICAL")

    def test_raises_validation_error_when_metric_missing(self):
        with pytest.raises(ValidationError):
            AnomalyItem(
                metric_label="라벨",
                change=-0.1,
                change_display="-10%",
                comparison_detail="detail",
                severity="HIGH",
                reasoning="reason",
            )

    def test_change_can_be_positive(self):
        """변화율이 양수(증가)도 허용된다."""
        item = _make_anomaly_item(change=0.05, change_display="+5%")
        assert item.change > 0

    def test_model_dump_json_is_valid_json_string(self):
        """model_dump_json()이 JSON 문자열을 반환하는지 확인.

        에이전트 내부에서 프롬프트 직렬화 시 사용되는 메서드.
        """
        import json
        item = _make_anomaly_item()
        dumped = item.model_dump_json()
        parsed = json.loads(dumped)
        assert parsed["metric"] == "revenue"


class TestAnomalyReport:
    def test_default_factory_creates_empty_lists(self):
        """anomalies, normal 모두 default_factory=list → 기본값은 빈 리스트."""
        report = AnomalyReport()
        assert report.anomalies == []
        assert report.normal == []

    def test_creates_report_with_anomalies(self):
        item = _make_anomaly_item()
        report = AnomalyReport(anomalies=[item], normal=["dau", "mau"])
        assert len(report.anomalies) == 1
        assert "dau" in report.normal

    def test_raises_validation_error_when_anomalies_item_invalid(self):
        """anomalies 리스트 내 아이템이 유효하지 않으면 ValidationError."""
        with pytest.raises(ValidationError):
            AnomalyReport(anomalies=[{"metric": "revenue"}])  # AnomalyItem 필수 필드 누락


# ════════════════════════════════════════════════════════════════════
# ② SegmentConcentration / SegmentationReport
# ════════════════════════════════════════════════════════════════════


class TestSegmentConcentration:
    def test_creates_valid_segment_concentration(self):
        sc = SegmentConcentration(dimension="platform", focus="android", change=-0.18)
        assert sc.dimension == "platform"
        assert sc.change == pytest.approx(-0.18)

    def test_raises_validation_error_when_dimension_missing(self):
        with pytest.raises(ValidationError):
            SegmentConcentration(focus="android", change=-0.18)


class TestSegmentationReport:
    def test_spread_type_accepts_concentrated(self):
        report = _make_segmentation_report(spread_type="concentrated")
        assert report.spread_type == "concentrated"

    def test_spread_type_accepts_spread(self):
        report = _make_segmentation_report(spread_type="spread")
        assert report.spread_type == "spread"

    def test_spread_type_accepts_crossed(self):
        report = _make_segmentation_report(spread_type="crossed")
        assert report.spread_type == "crossed"

    def test_raises_validation_error_when_spread_type_invalid(self):
        """spread_type에 허용 외 값 → ValidationError."""
        with pytest.raises(ValidationError):
            _make_segmentation_report(spread_type="unknown")

    def test_breakdown_stores_nested_dict(self):
        breakdown = {"platform": {"android": -0.18, "ios": 0.002}}
        report = _make_segmentation_report(breakdown=breakdown)
        assert report.breakdown["platform"]["android"] == pytest.approx(-0.18)

    def test_raises_validation_error_when_concentration_missing(self):
        with pytest.raises(ValidationError):
            SegmentationReport(
                anomaly="revenue",
                breakdown={},
                summary="요약",
                spread_type="spread",
                # concentration 누락
            )


# ════════════════════════════════════════════════════════════════════
# ③ Hypothesis / HypothesisList
# ════════════════════════════════════════════════════════════════════


class TestHypothesis:
    def test_creates_hypothesis_with_required_tables(self):
        h = Hypothesis(
            hypothesis="Android 상점 UI 변경으로 프리미엄 패키지 노출 감소",
            reasoning="UI 변경이 노출 순서를 바꿀 수 있음",
            required_tables=["shop_impressions", "payments"],
        )
        assert len(h.required_tables) == 2

    def test_required_tables_defaults_to_empty_list(self):
        """required_tables는 default_factory=list → 기본값 빈 리스트."""
        h = Hypothesis(
            hypothesis="외부 광고 채널 변경",
            reasoning="광고비 효율 저하",
        )
        assert h.required_tables == []

    def test_required_data_defaults_to_none(self):
        h = Hypothesis(hypothesis="가설", reasoning="이유")
        assert h.required_data is None

    def test_required_data_can_be_set(self):
        h = Hypothesis(
            hypothesis="가설",
            reasoning="이유",
            required_data="광고 플랫폼 퍼포먼스 데이터 필요",
        )
        assert h.required_data == "광고 플랫폼 퍼포먼스 데이터 필요"

    def test_raises_validation_error_when_hypothesis_missing(self):
        with pytest.raises(ValidationError):
            Hypothesis(reasoning="이유")


class TestHypothesisList:
    def test_hypotheses_defaults_to_empty_list(self):
        hl = HypothesisList(anomaly="revenue")
        assert hl.hypotheses == []

    def test_creates_hypothesis_list_with_multiple_items(self):
        items = [
            Hypothesis(hypothesis=f"가설 {i}", reasoning=f"이유 {i}")
            for i in range(5)
        ]
        hl = HypothesisList(anomaly="revenue", hypotheses=items)
        assert len(hl.hypotheses) == 5


# ════════════════════════════════════════════════════════════════════
# ④ ValidationResult
# ════════════════════════════════════════════════════════════════════


class TestValidationResult:
    def test_status_accepts_supported(self):
        vr = _make_validation_result("supported")
        assert vr.status == "supported"

    def test_status_accepts_rejected(self):
        vr = _make_validation_result("rejected")
        assert vr.status == "rejected"

    def test_status_accepts_unverified(self):
        vr = _make_validation_result("unverified")
        assert vr.status == "unverified"

    def test_raises_validation_error_when_status_invalid(self):
        """status에 허용 외 값 → ValidationError.

        예: "unknown"이나 "pending"은 거부되어야 한다.
        """
        with pytest.raises(ValidationError):
            ValidationResult(hypothesis="가설", status="unknown")

    def test_evidence_defaults_to_none(self):
        vr = ValidationResult(hypothesis="가설", status="unverified")
        assert vr.evidence is None

    def test_queries_run_defaults_to_empty_list(self):
        vr = ValidationResult(hypothesis="가설", status="supported")
        assert vr.queries_run == []

    def test_query_results_defaults_to_empty_list(self):
        vr = ValidationResult(hypothesis="가설", status="supported")
        assert vr.query_results == []

    def test_required_data_defaults_to_none(self):
        vr = ValidationResult(hypothesis="가설", status="unverified")
        assert vr.required_data is None

    def test_creates_full_validation_result(self):
        """모든 필드를 채운 ValidationResult 생성."""
        vr = ValidationResult(
            hypothesis="Android 상점 UI 변경",
            status="supported",
            evidence="payments 테이블: Android 매출 -18%",
            queries_run=["SELECT * FROM payments WHERE platform='android'"],
            query_results=[{"revenue": 100}],
            required_data=None,
        )
        assert vr.status == "supported"
        assert len(vr.queries_run) == 1


# ════════════════════════════════════════════════════════════════════
# ⑤ Pydantic 스키마
# ════════════════════════════════════════════════════════════════════


class TestCausalStep:
    def test_creates_causal_step(self):
        step = CausalStep(
            step="Android 상점 UI 변경",
            evidence="shop_impressions 건수 -22%",
        )
        assert step.step == "Android 상점 UI 변경"

    def test_raises_validation_error_when_evidence_missing(self):
        with pytest.raises(ValidationError):
            CausalStep(step="단계만 있음")


class TestRootCause:
    def test_chain_defaults_to_empty_list(self):
        rc = RootCause(summary="원인 불명")
        assert rc.chain == []

    def test_creates_root_cause_with_chain(self):
        steps = [
            CausalStep(step="A 발생", evidence="증거 A"),
            CausalStep(step="B 발생", evidence="증거 B"),
        ]
        rc = RootCause(chain=steps, summary="A → B → 이상")
        assert len(rc.chain) == 2


class TestUnverifiedHypothesis:
    def test_creates_unverified_hypothesis(self):
        uh = UnverifiedHypothesis(
            hypothesis="광고 채널 변경",
            required_data="광고 플랫폼 데이터 필요",
        )
        assert uh.hypothesis == "광고 채널 변경"

    def test_raises_validation_error_when_required_data_missing(self):
        with pytest.raises(ValidationError):
            UnverifiedHypothesis(hypothesis="가설만 있음")


class TestRootCauseReport:
    def test_additional_investigation_defaults_to_empty_list(self):
        report = RootCauseReport(
            anomaly="revenue",
            root_cause=RootCause(summary="원인 불명"),
        )
        assert report.additional_investigation == []

    def test_creates_full_root_cause_report(self):
        report = _make_root_cause_report(chain_empty=False)
        assert report.anomaly == "revenue"
        assert len(report.root_cause.chain) == 2
        assert len(report.additional_investigation) == 1


# ════════════════════════════════════════════════════════════════════
# ⑤ prepare_input() 순수 함수
# ════════════════════════════════════════════════════════════════════


class TestRootCausePrepareInput:
    """prepare_input()은 ValidationResult 리스트를 3상태(supported/rejected/unverified)로 분리.

    Java 비유: 순수 static 메서드 테스트 — 입력만 주면 결과가 결정적(deterministic).
    """

    def _make_mixed_results(self) -> list[ValidationResult]:
        return [
            _make_validation_result("supported", hypothesis="가설 A"),
            _make_validation_result("supported", hypothesis="가설 B"),
            _make_validation_result("rejected", hypothesis="가설 C"),
            _make_validation_result("unverified", hypothesis="가설 D"),
            _make_validation_result("unverified", hypothesis="가설 E"),
        ]

    def test_separates_supported_correctly(self):
        anomaly = _make_anomaly_item()
        seg = _make_segmentation_report()
        results = self._make_mixed_results()

        prepared = root_prepare_input(anomaly, seg, results)

        assert len(prepared["supported"]) == 2

    def test_separates_rejected_correctly(self):
        anomaly = _make_anomaly_item()
        seg = _make_segmentation_report()
        results = self._make_mixed_results()

        prepared = root_prepare_input(anomaly, seg, results)

        assert len(prepared["rejected"]) == 1

    def test_separates_unverified_correctly(self):
        anomaly = _make_anomaly_item()
        seg = _make_segmentation_report()
        results = self._make_mixed_results()

        prepared = root_prepare_input(anomaly, seg, results)

        assert len(prepared["unverified"]) == 2

    def test_returns_all_three_status_keys(self):
        anomaly = _make_anomaly_item()
        seg = _make_segmentation_report()
        results = self._make_mixed_results()

        prepared = root_prepare_input(anomaly, seg, results)

        assert "supported" in prepared
        assert "rejected" in prepared
        assert "unverified" in prepared

    def test_returns_anomaly_and_segmentation_as_dicts(self):
        """anomaly와 segmentation은 dict 형태로 반환 (JSON 직렬화용)."""
        anomaly = _make_anomaly_item()
        seg = _make_segmentation_report()
        prepared = root_prepare_input(anomaly, seg, [])

        assert isinstance(prepared["anomaly"], dict)
        assert isinstance(prepared["segmentation"], dict)

    def test_anomaly_dict_contains_metric_key(self):
        anomaly = _make_anomaly_item(metric="d7_retention")
        seg = _make_segmentation_report()
        prepared = root_prepare_input(anomaly, seg, [])

        assert prepared["anomaly"]["metric"] == "d7_retention"

    def test_returns_empty_lists_when_no_results(self):
        """검증 결과가 없으면 3상태 모두 빈 리스트."""
        anomaly = _make_anomaly_item()
        seg = _make_segmentation_report()
        prepared = root_prepare_input(anomaly, seg, [])

        assert prepared["supported"] == []
        assert prepared["rejected"] == []
        assert prepared["unverified"] == []

    def test_all_supported_when_all_results_are_supported(self):
        anomaly = _make_anomaly_item()
        seg = _make_segmentation_report()
        results = [_make_validation_result("supported") for _ in range(3)]
        prepared = root_prepare_input(anomaly, seg, results)

        assert len(prepared["supported"]) == 3
        assert prepared["rejected"] == []
        assert prepared["unverified"] == []


# ════════════════════════════════════════════════════════════════════
# ⑥ Action / ActionPlan 스키마
# ════════════════════════════════════════════════════════════════════


class TestAction:
    def test_priority_accepts_urgent(self):
        action = Action(
            priority="urgent",
            title="Android v1.2.3 롤백",
            effect="매출 즉시 회복",
            effort="Android 개발자 1명, 2시간",
        )
        assert action.priority == "urgent"

    def test_priority_accepts_short_term(self):
        action = Action(
            priority="short_term",
            title="상점 A/B 테스트",
            effect="최적 UI 확인",
            effort="기획자 1명, 3일",
        )
        assert action.priority == "short_term"

    def test_priority_accepts_mid_term(self):
        action = Action(
            priority="mid_term",
            title="배포 프로세스 개선",
            effect="재발 방지",
            effort="팀 전체, 2주",
        )
        assert action.priority == "mid_term"

    def test_raises_validation_error_when_priority_invalid(self):
        """priority에 허용 외 값 → ValidationError.

        예: "immediate"나 "long_term"은 거부.
        """
        with pytest.raises(ValidationError):
            Action(
                priority="immediate",
                title="즉시 조치",
                effect="효과",
                effort="리소스",
            )

    def test_related_cause_step_defaults_to_none(self):
        action = Action(
            priority="urgent", title="롤백", effect="효과", effort="리소스"
        )
        assert action.related_cause_step is None

    def test_related_cause_step_can_be_set(self):
        action = Action(
            priority="urgent",
            title="롤백",
            effect="효과",
            effort="리소스",
            related_cause_step="Android 상점 UI 변경",
        )
        assert action.related_cause_step == "Android 상점 UI 변경"

    def test_raises_validation_error_when_title_missing(self):
        with pytest.raises(ValidationError):
            Action(priority="urgent", effect="효과", effort="리소스")


class TestActionPlan:
    def test_actions_defaults_to_empty_list(self):
        plan = ActionPlan(anomaly="revenue")
        assert plan.actions == []

    def test_note_defaults_to_none(self):
        plan = ActionPlan(anomaly="revenue")
        assert plan.note is None

    def test_creates_action_plan_with_actions(self):
        actions = [
            Action(priority="urgent", title="롤백", effect="효과", effort="리소스"),
            Action(priority="short_term", title="점검", effect="확인", effort="1일"),
        ]
        plan = ActionPlan(anomaly="revenue", actions=actions, note="원인 명확")
        assert len(plan.actions) == 2
        assert plan.note == "원인 명확"


# ════════════════════════════════════════════════════════════════════
# ⑥ prepare_input() 순수 함수
# ════════════════════════════════════════════════════════════════════


class TestActionPrepareInput:
    """prepare_input()은 RootCauseReport에서 is_unknown_cause 플래그를 파생.

    Java 비유: 순수 static 변환 메서드.
    chain이 비어있으면 is_unknown_cause = True.
    """

    def test_is_unknown_cause_is_false_when_chain_has_steps(self):
        report = _make_root_cause_report(chain_empty=False)
        prepared = action_prepare_input(report)

        assert prepared["is_unknown_cause"] is False

    def test_is_unknown_cause_is_true_when_chain_is_empty(self):
        """chain이 빈 배열이면 is_unknown_cause = True."""
        report = _make_root_cause_report(chain_empty=True)
        prepared = action_prepare_input(report)

        assert prepared["is_unknown_cause"] is True

    def test_anomaly_matches_report_anomaly(self):
        report = _make_root_cause_report()
        prepared = action_prepare_input(report)

        assert prepared["anomaly"] == "revenue"

    def test_root_cause_is_dict(self):
        """root_cause는 dict 형태로 반환 (JSON 직렬화용)."""
        report = _make_root_cause_report()
        prepared = action_prepare_input(report)

        assert isinstance(prepared["root_cause"], dict)

    def test_root_cause_dict_contains_chain_key(self):
        report = _make_root_cause_report(chain_empty=False)
        prepared = action_prepare_input(report)

        assert "chain" in prepared["root_cause"]

    def test_additional_investigation_is_list_of_dicts(self):
        """additional_investigation은 dict 리스트."""
        report = _make_root_cause_report()
        prepared = action_prepare_input(report)

        assert isinstance(prepared["additional_investigation"], list)
        assert all(isinstance(i, dict) for i in prepared["additional_investigation"])

    def test_additional_investigation_contains_hypothesis_and_required_data(self):
        report = _make_root_cause_report()
        prepared = action_prepare_input(report)

        first = prepared["additional_investigation"][0]
        assert "hypothesis" in first
        assert "required_data" in first

    def test_returns_empty_additional_investigation_when_none(self):
        """additional_investigation이 없으면 빈 리스트."""
        report = RootCauseReport(
            anomaly="revenue",
            root_cause=RootCause(
                chain=[CausalStep(step="A", evidence="근거")],
                summary="요약",
            ),
            additional_investigation=[],
        )
        prepared = action_prepare_input(report)

        assert prepared["additional_investigation"] == []
        assert prepared["is_unknown_cause"] is False