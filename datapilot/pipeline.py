"""파이프라인 오케스트레이터.

6종 에이전트를 하나의 파이프라인으로 연결한다.
① 병목 탐지 -> segmentable/non-segmentable 분류 -> 이상별 ②~⑥ 루프 -> 리포트 취합.

Java 비유:
    @Service
    public class PipelineOrchestrator {
        // 6종 에이전트를 @Autowired로 주입받아 순차 실행
        public PipelineReport run(String gameId, Period period) { ... }
    }
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

from pydantic import BaseModel, Field

from datapilot.agents.action_recommender import ActionPlan, ActionRecommender
from datapilot.agents.bottleneck_detector import (
    AnomalyItem,
    BottleneckDetector,
)
from datapilot.agents.data_validator import DataValidator, ValidationResult
from datapilot.agents.hypothesis_generator import HypothesisGenerator, HypothesisList
from datapilot.agents.root_cause_reasoner import RootCauseReasoner, RootCauseReport
from datapilot.agents.segmentation_analyzer import (
    SegmentationAnalyzer,
    SegmentationReport,
)
from datapilot.repository.port import SUPPORTED_SEGMENT_METRICS, GameDataRepository

# ------------------------------------------------------------------
# 파이프라인 State 모델
# ------------------------------------------------------------------


class AnomalyAnalysis(BaseModel):
    """segmentable 이상 지표 1개의 완전 분석 결과 (②~⑥)."""

    anomaly: AnomalyItem
    segmentation: SegmentationReport
    hypotheses: HypothesisList
    validation_results: list[ValidationResult]
    root_cause: RootCauseReport
    action_plan: ActionPlan


class UnanalyzedAnomaly(BaseModel):
    """non-segmentable 이상 지표. ① 원본만 보존."""

    anomaly: AnomalyItem
    reason: str = "현재 세부 분석 미지원 지표"


class PipelineReport(BaseModel):
    """파이프라인 전체 출력. Streamlit UI의 데이터 소스."""

    game_id: str
    period_from: str = Field(description="시작일 ISO format")
    period_to: str = Field(description="종료일 ISO format")
    analyzed: list[AnomalyAnalysis] = Field(
        default_factory=list,
        description="segmentable 이상 지표 완전 분석 결과",
    )
    unanalyzed: list[UnanalyzedAnomaly] = Field(
        default_factory=list,
        description="non-segmentable 이상 지표 (① 원본만)",
    )
    normal_metrics: list[str] = Field(
        default_factory=list,
        description="정상 지표명 목록",
    )
    anomaly_order: list[str] = Field(
        default_factory=list,
        description="① 탐지 순서 보존 (metric 코드 리스트). UI 카드 정렬 기준",
    )


# ------------------------------------------------------------------
# 진행 콜백 (Phase 8 Streamlit 연동용)
# ------------------------------------------------------------------


@dataclass
class PipelineStep:
    """에이전트 진행 상태 1건. on_step 콜백에 전달된다."""

    agent: str      # "bottleneck", "segmentation", ...
    status: str     # "active", "done", "error"
    summary: str = ""
    metric: str = ""  # 어떤 이상 지표에 대한 것인지 (②~⑥ 루프용)


#: 콜백 타입. None이면 진행 보고 생략.
OnStepCallback = Callable[[PipelineStep], None] | None


# ------------------------------------------------------------------
# 오케스트레이터
# ------------------------------------------------------------------


class PipelineOrchestrator:
    """6종 에이전트를 순차 실행하는 파이프라인.

    Java 비유::

        public PipelineOrchestrator(
            @Autowired BottleneckDetector detector,
            @Autowired SegmentationAnalyzer segmenter,
            ...
        ) { ... }
    """

    def __init__(self, repo: GameDataRepository) -> None:
        self._repo = repo
        self._detector = BottleneckDetector()
        self._segmenter = SegmentationAnalyzer()
        self._hypothesis_gen = HypothesisGenerator()
        self._validator = DataValidator(repo=repo)
        self._reasoner = RootCauseReasoner()
        self._recommender = ActionRecommender()

    def run(
        self,
        game_id: str,
        period: tuple[date, date],
        *,
        on_step: OnStepCallback = None,
    ) -> PipelineReport:
        """파이프라인을 실행한다.

        Args:
            game_id: 게임 식별자.
            period: (시작일, 종료일) inclusive.
            on_step: 에이전트 단계별 콜백 (Phase 8 Streamlit 연동용).

        Returns:
            PipelineReport -- 분석 완료 리포트.
        """
        def _notify(
            agent: str, status: str, summary: str = "", metric: str = "",
        ) -> None:
            if on_step:
                on_step(PipelineStep(
                    agent=agent, status=status, summary=summary, metric=metric,
                ))

        # ── ① 병목 탐지 ──────────────────────────────────────
        _notify("bottleneck", "active")
        kpi_series = self._repo.get_daily_kpi(game_id, period)
        anomaly_report = self._detector.detect(kpi_series)
        n = len(anomaly_report.anomalies)
        _notify("bottleneck", "done", f"이상 지표 {n}개 발견")

        # ── 분류: segmentable / non-segmentable ───────────────
        segmentable: list[AnomalyItem] = []
        non_segmentable: list[AnomalyItem] = []
        for a in anomaly_report.anomalies:
            if a.metric in SUPPORTED_SEGMENT_METRICS:
                segmentable.append(a)
            else:
                non_segmentable.append(a)

        # ① 탐지 순서 보존 (UI 카드 정렬 기준)
        anomaly_order = [a.metric for a in anomaly_report.anomalies]

        # 스키마는 segmentable이 있을 때만 1회 조회 (불필요한 DB I/O 방지)
        available_schema = (
            self._repo.get_available_schema(game_id) if segmentable else {}
        )

        # ── 이상별 ②~⑥ 루프 ─────────────────────────────────
        analyzed: list[AnomalyAnalysis] = []
        for anomaly in segmentable:
            result = self._analyze_one(
                game_id, anomaly, period, available_schema, _notify,
            )
            analyzed.append(result)

        unanalyzed = [
            UnanalyzedAnomaly(anomaly=a) for a in non_segmentable
        ]

        return PipelineReport(
            game_id=game_id,
            period_from=period[0].isoformat(),
            period_to=period[1].isoformat(),
            analyzed=analyzed,
            unanalyzed=unanalyzed,
            normal_metrics=anomaly_report.normal,
            anomaly_order=anomaly_order,
        )

    def _analyze_one(
        self,
        game_id: str,
        anomaly: AnomalyItem,
        period: tuple[date, date],
        available_schema: dict[str, Any],
        notify: Callable[..., None],
    ) -> AnomalyAnalysis:
        """segmentable 이상 지표 1개에 대해 ②~⑥을 순차 실행한다.

        에이전트 실패 시 해당 스텝을 error 상태로 콜백한 뒤 예외를 재전파한다.
        Phase 8 UI가 실패 스텝을 식별해 "다시 시도" 버튼을 표시할 수 있다.
        """
        m = anomaly.metric
        current_step = ""

        try:
            # ② 세그먼트 분석
            current_step = "segmentation"
            notify("segmentation", "active", m, metric=m)
            segmentation = self._segmenter.analyze(
                game_id, anomaly, period, self._repo,
            )
            notify("segmentation", "done", segmentation.concentration.focus, metric=m)

            # ③ 가설 생성
            current_step = "hypothesis"
            notify("hypothesis", "active", m, metric=m)
            hypotheses = self._hypothesis_gen.generate(
                game_id, anomaly, segmentation, self._repo,
            )
            notify("hypothesis", "done", f"가설 {len(hypotheses.hypotheses)}개", metric=m)

            # ④ 데이터 검증
            current_step = "validation"
            notify("validation", "active", m, metric=m)
            validation_results = self._validator.validate(
                hypotheses, available_schema,
            )
            sup = sum(1 for v in validation_results if v.status == "supported")
            rej = sum(1 for v in validation_results if v.status == "rejected")
            unv = sum(1 for v in validation_results if v.status == "unverified")
            notify(
                "validation", "done",
                f"확인 {sup} / 기각 {rej} / 미검증 {unv}", metric=m,
            )

            # ⑤ 원인 추론
            current_step = "root_cause"
            notify("root_cause", "active", m, metric=m)
            root_cause = self._reasoner.reason(
                anomaly, segmentation, validation_results,
            )
            rc_summary = "원인 불명" if not root_cause.root_cause.chain else "완료"
            notify("root_cause", "done", rc_summary, metric=m)

            # ⑥ 액션 추천
            current_step = "action"
            notify("action", "active", m, metric=m)
            action_plan = self._recommender.recommend(root_cause)
            notify("action", "done", f"액션 {len(action_plan.actions)}개", metric=m)

        except Exception as e:
            notify(current_step, "error", str(e), metric=m)
            raise

        return AnomalyAnalysis(
            anomaly=anomaly,
            segmentation=segmentation,
            hypotheses=hypotheses,
            validation_results=validation_results,
            root_cause=root_cause,
            action_plan=action_plan,
        )