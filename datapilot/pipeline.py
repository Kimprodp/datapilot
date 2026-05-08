"""파이프라인 오케스트레이터.

6종 에이전트를 하나의 파이프라인으로 연결한다.
① 병목 탐지 -> segmentable/non-segmentable 분류 -> 이상별 ②~⑥ 루프 -> 리포트 취합.

run() 호출 시 ``metrics`` 옵션으로 :class:`MetricsCollector` 를 주입하면
각 에이전트 단계가 ``with metrics.span(agent_name)`` 으로 감싸지고
LLM 호출 usage_metadata 가 자동 추출된다. 분석 종료 시 ``metrics.flush()``
가 콘솔 + ``.logs/<run_id>.jsonl`` 로 batch 출력한다. ``metrics=None`` 이면
no-op 대체로 동작해 기존 동작이 보존된다.

Java 비유:
    @Service
    public class PipelineOrchestrator {
        // 6종 에이전트를 @Autowired로 주입받아 순차 실행
        public PipelineReport run(String gameId, Period period, MetricsCollector m) { ... }
    }
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

from langchain_core.callbacks import BaseCallbackHandler
from pydantic import BaseModel, Field

from datapilot.agents.action_recommender import ActionPlan, ActionRecommender
from datapilot.agents.bottleneck_detector import (
    AnomalyItem,
    BottleneckDetector,
)
from datapilot.agents.bundle import AgentBundle
from datapilot.agents.data_validator import DataValidator, ValidationResult
from datapilot.agents.hypothesis_generator import HypothesisGenerator, HypothesisList
from datapilot.agents.root_cause_reasoner import RootCauseReasoner, RootCauseReport
from datapilot.agents.segmentation_analyzer import (
    SegmentationAnalyzer,
    SegmentationReport,
)
from datapilot.observability import NULL_METRICS, MetricsCollector
from datapilot.repository.port import DataRepository

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

    entity_id: str
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

    def __init__(
        self,
        repo: DataRepository,
        *,
        agents: AgentBundle | None = None,
    ) -> None:
        self._repo = repo
        # agents=None 이면 게임 도메인 디폴트 (백워드 호환)
        if agents is None:
            agents = AgentBundle.create("game", repo=repo)
        self._detector = agents.bottleneck
        self._segmenter = agents.segmenter
        self._hypothesis_gen = agents.hypothesis
        self._validator = agents.validator
        self._reasoner = agents.reasoner
        self._recommender = agents.recommender
        # 도메인 segmentable KPI 셋 — anomaly 분류에 사용
        self._segment_metrics = agents.supported_segment_metrics

    def run(
        self,
        entity_id: str,
        period: tuple[date, date],
        *,
        on_step: OnStepCallback = None,
        metrics: MetricsCollector | None = None,
    ) -> PipelineReport:
        """파이프라인을 실행한다.

        Args:
            entity_id: 분석 대상 식별자 (게임 ID / 스토어 ID 등).
            period: (시작일, 종료일) inclusive.
            on_step: 에이전트 단계별 콜백 (Streamlit 연동용).
            metrics: 단계별 latency / 토큰 / cache 측정용 collector.
                None 이면 NullMetricsCollector 로 대체되어 측정 비용 없이 동작한다.

        Returns:
            PipelineReport -- 분석 완료 리포트.
        """
        # metrics=None 이면 no-op 더미로 대체 → 본문 분기 없이 with 사용 가능
        m: BaseCallbackHandler = metrics or NULL_METRICS

        def _notify(
            agent: str, status: str, summary: str = "", metric: str = "",
        ) -> None:
            if on_step:
                on_step(PipelineStep(
                    agent=agent, status=status, summary=summary, metric=metric,
                ))

        try:
            # ── ① 병목 탐지 ──────────────────────────────────────
            _notify("bottleneck", "active")
            kpi_series = self._repo.get_daily_kpi(entity_id, period)
            with m.span("bottleneck"):
                anomaly_report = self._detector.detect(kpi_series, metrics=m)
            n = len(anomaly_report.anomalies)
            labels = [a.metric_label.split("(")[0].strip() for a in anomaly_report.anomalies]
            direction = [
                f"{l} {'증가' if a.change > 0 else '감소'}"
                for l, a in zip(labels, anomaly_report.anomalies)
            ]
            _notify("bottleneck", "done", f"이상 지표 {n}개 발견 ({', '.join(direction)})")
            # 각 이상 지표의 방향을 metric 코드 기준으로 전달
            for a in anomaly_report.anomalies:
                _notify("direction", "info",
                        "증가" if a.change > 0 else "감소", metric=a.metric)

            # ── 분류: segmentable / non-segmentable ───────────────
            segmentable: list[AnomalyItem] = []
            non_segmentable: list[AnomalyItem] = []
            for a in anomaly_report.anomalies:
                if a.metric in self._segment_metrics:
                    segmentable.append(a)
                else:
                    non_segmentable.append(a)

            # ① 탐지 순서 보존 (UI 카드 정렬 기준)
            anomaly_order = [a.metric for a in anomaly_report.anomalies]

            # 스키마는 segmentable이 있을 때만 1회 조회 (불필요한 DB I/O 방지)
            available_schema = (
                self._repo.get_available_schema(entity_id) if segmentable else {}
            )

            # ── 미지원 지표 먼저 알림 (화면2에 카드 즉시 표시) ────
            unanalyzed = []
            for a in non_segmentable:
                _notify("unsupported", "done", "세부 분석 미지원", metric=a.metric)
                unanalyzed.append(UnanalyzedAnomaly(anomaly=a))

            # ── 이상별 ②~⑥ (순차 실행) ────────────────────────────
            analyzed: list[AnomalyAnalysis] = []
            for anomaly in segmentable:
                analyzed.append(self._analyze_one(
                    entity_id, anomaly, period, available_schema, _notify, m,
                ))

            return PipelineReport(
                entity_id=entity_id,
                period_from=period[0].isoformat(),
                period_to=period[1].isoformat(),
                analyzed=analyzed,
                unanalyzed=unanalyzed,
                normal_metrics=anomaly_report.normal,
                anomaly_order=anomaly_order,
            )
        except Exception:
            if metrics is not None:
                metrics.mark_partial()
            raise
        finally:
            # 사용자가 명시적으로 metrics 를 줄 때만 flush.
            # NULL_METRICS 는 flush 무의미 + 파일 생성 부작용 회피.
            if metrics is not None:
                try:
                    metrics.flush()
                except Exception as e:
                    print(f"[metrics] flush failed: {e}", file=sys.stderr)

    def _analyze_one(
        self,
        entity_id: str,
        anomaly: AnomalyItem,
        period: tuple[date, date],
        available_schema: dict[str, Any],
        notify: Callable[..., None],
        metrics: BaseCallbackHandler,
    ) -> AnomalyAnalysis:
        """segmentable 이상 지표 1개에 대해 ②~⑥을 순차 실행한다."""
        repo = self._repo
        validator = self._validator
        m = anomaly.metric
        current_step = ""

        try:
            # ② 세그먼트 분석
            current_step = "segmentation"
            notify("segmentation", "active", m, metric=m)
            with metrics.span("segmentation", metric=m):
                segmentation = self._segmenter.analyze(
                    entity_id, anomaly, period, repo, metrics=metrics,
                )
            notify("segmentation", "done", segmentation.concentration.focus, metric=m)

            # ③ 가설 생성
            current_step = "hypothesis"
            notify("hypothesis", "active", m, metric=m)
            with metrics.span("hypothesis", metric=m):
                hypotheses = self._hypothesis_gen.generate(
                    entity_id, anomaly, segmentation, repo, metrics=metrics,
                )
            notify("hypothesis", "done", f"가설 {len(hypotheses.hypotheses)}개", metric=m)

            # ④ 데이터 검증
            current_step = "validation"
            notify("validation", "active", m, metric=m)
            with metrics.span("validation", metric=m):
                validation_results = validator.validate(
                    hypotheses, available_schema, metrics=metrics,
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
            with metrics.span("root_cause", metric=m):
                root_cause = self._reasoner.reason(
                    anomaly, segmentation, validation_results, metrics=metrics,
                )
            rc_summary = "원인 불명" if not root_cause.root_cause.chain else "완료"
            notify("root_cause", "done", rc_summary, metric=m)

            # ⑥ 액션 추천
            current_step = "action"
            notify("action", "active", m, metric=m)
            with metrics.span("action", metric=m):
                action_plan = self._recommender.recommend(root_cause, metrics=metrics)
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