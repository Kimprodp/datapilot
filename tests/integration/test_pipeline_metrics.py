"""파이프라인 + MetricsCollector 통합 테스트.

검증 범위:
- 분석 1회 실행 → metrics 가 6 에이전트 + validator_round 모두 측정
- 분석 종료 시 flush() 1회 호출 (콘솔 + .logs/<run_id>.jsonl 생성)
- metrics=None 시 기존 동작 보존 (회귀)
- 에이전트 예외 발생 시 metrics.partial=True + 예외 전파
- 측정 부하 ≤ 100ms (metrics 적용 vs 미적용 분석 시간 차이)

6 에이전트는 mock 으로 대체 (LangChain LLM 호출 회피).
on_llm_end 콜백은 별도 단위 테스트가 검증 — 본 테스트는 span 통합과
flush 동작에 집중.
"""

from __future__ import annotations

import json
import time
from datetime import date
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from datapilot.agents.action_recommender import Action, ActionPlan
from datapilot.agents.bottleneck_detector import (
    AnomalyItem,
    AnomalyReport,
)
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
from datapilot.observability import MetricsCollector
from datapilot.pipeline import PipelineOrchestrator
from datapilot.repository.port import GameDataRepository

# ─── 헬퍼 ────────────────────────────────────────────────────────────


_PERIOD = (date(2026, 3, 25), date(2026, 4, 1))
_GAME_ID = "pizza_ready"
_METRIC = "revenue"


def _anomaly_report() -> AnomalyReport:
    return AnomalyReport(
        anomalies=[
            AnomalyItem(
                metric=_METRIC,
                metric_label="인앱결제 매출 (revenue)",
                change=-0.11,
                change_display="-11%",
                comparison_detail="-11.1%",
                severity="HIGH",
                reasoning="테스트용",
            )
        ],
        normal=["dau"],
    )


def _segmentation() -> SegmentationReport:
    return SegmentationReport(
        anomaly=_METRIC,
        concentration=SegmentConcentration(
            dimension="platform", focus="android", change=-0.18,
        ),
        breakdown={"platform": {"android": -18.0, "ios": -1.0}},
        summary="요약",
        spread_type="concentrated",
    )


def _hypotheses() -> HypothesisList:
    return HypothesisList(
        anomaly=_METRIC,
        hypotheses=[
            Hypothesis(
                hypothesis="Android 상점 UI 변경",
                reasoning="...",
                required_tables=["payments"],
            )
        ],
    )


def _validation_results() -> list[ValidationResult]:
    return [
        ValidationResult(
            hypothesis="Android 상점 UI 변경",
            status="supported",
            evidence="근거",
        )
    ]


def _root_cause() -> RootCauseReport:
    return RootCauseReport(
        anomaly=_METRIC,
        root_cause=RootCause(
            chain=[CausalStep(step="원인", evidence="근거")],
            summary="요약",
        ),
    )


def _action_plan() -> ActionPlan:
    return ActionPlan(
        anomaly=_METRIC,
        actions=[
            Action(
                priority="urgent",
                title="롤백",
                effect="복구",
                effort="개발 1명",
            )
        ],
    )


def _make_mock_repo() -> MagicMock:
    repo = MagicMock(spec=GameDataRepository)
    repo.get_daily_kpi.return_value = {"game_id": _GAME_ID, "daily": []}
    repo.get_available_schema.return_value = {
        "tables": [{"name": "payments", "columns": [], "description": ""}]
    }
    return repo


def _make_orchestrator(
    *,
    raise_on: str | None = None,
) -> tuple[PipelineOrchestrator, MagicMock]:
    """6 에이전트가 mock 으로 대체된 orchestrator + repo.

    raise_on: "hypothesis" 등 에이전트 명. 해당 단계에서 RuntimeError 발생.
    """
    repo = _make_mock_repo()
    orch = PipelineOrchestrator(repo=repo)

    orch._detector.detect = Mock(return_value=_anomaly_report())
    orch._segmenter.analyze = Mock(
        side_effect=lambda *a, **kw: _segmentation(),
    )
    if raise_on == "hypothesis":
        orch._hypothesis_gen.generate = Mock(
            side_effect=RuntimeError("hypothesis exploded"),
        )
    else:
        orch._hypothesis_gen.generate = Mock(
            side_effect=lambda *a, **kw: _hypotheses(),
        )
    orch._validator.validate = Mock(return_value=_validation_results())
    orch._reasoner.reason = Mock(side_effect=lambda *a, **kw: _root_cause())
    orch._recommender.recommend = Mock(side_effect=lambda *a, **kw: _action_plan())
    return orch, repo


# ════════════════════════════════════════════════════════════════════
# 1. metrics 통합 — 정상 분석 1회
# ════════════════════════════════════════════════════════════════════


class TestMetricsIntegration:
    def test_all_six_agents_recorded_as_spans(self, tmp_path):
        orch, _ = _make_orchestrator()
        m = MetricsCollector(log_dir=tmp_path)

        orch.run(_GAME_ID, _PERIOD, metrics=m)

        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        names = [s["name"] for s in log["spans"]]
        # ① bottleneck + ②~⑥ (segmentation/hypothesis/validation/root_cause/action)
        assert "bottleneck" in names
        assert "segmentation" in names
        assert "hypothesis" in names
        assert "validation" in names
        assert "root_cause" in names
        assert "action" in names

    def test_anomaly_metric_is_tagged_on_per_anomaly_spans(self, tmp_path):
        orch, _ = _make_orchestrator()
        m = MetricsCollector(log_dir=tmp_path)

        orch.run(_GAME_ID, _PERIOD, metrics=m)

        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        # ②~⑥ span 들은 metric=revenue 태그가 붙어 있어야 한다 (PR 본 task 의 핵심)
        per_anomaly = [
            s for s in log["spans"]
            if s["name"] in {"segmentation", "hypothesis", "validation",
                             "root_cause", "action"}
        ]
        assert per_anomaly  # 비어있으면 회귀
        for s in per_anomaly:
            assert s.get("metric") == _METRIC

    def test_flush_creates_jsonl_file_with_run_id(self, tmp_path):
        orch, _ = _make_orchestrator()
        m = MetricsCollector(log_dir=tmp_path)

        orch.run(_GAME_ID, _PERIOD, metrics=m)

        path = tmp_path / f"{m.run_id}.jsonl"
        assert path.exists()
        log = json.loads(path.read_text("utf-8"))
        assert log["run_id"] == m.run_id
        assert log["partial"] is False


# ════════════════════════════════════════════════════════════════════
# 2. metrics=None 회귀
# ════════════════════════════════════════════════════════════════════


class TestRegressionWithoutMetrics:
    def test_pipeline_runs_without_metrics_argument(self, tmp_path, monkeypatch):
        """metrics 인자 없이 호출 → 기존 동작 보존, 파일 생성 X."""
        # NULL_METRICS 의 기본 log_dir 가 .logs/ 라 cwd 에 폴더 생기는 것 회피.
        # 단, NullMetricsCollector 는 flush 가 no-op 이라 파일 생성 안 함.
        # 그래도 안전하게 cwd 격리.
        monkeypatch.chdir(tmp_path)
        orch, _ = _make_orchestrator()

        report = orch.run(_GAME_ID, _PERIOD)  # metrics 인자 없음

        assert report.game_id == _GAME_ID
        assert len(report.analyzed) == 1
        assert not (tmp_path / ".logs").exists()  # NULL 은 파일 미생성

    def test_pipeline_runs_with_metrics_none_explicitly(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        orch, _ = _make_orchestrator()

        report = orch.run(_GAME_ID, _PERIOD, metrics=None)

        assert report.game_id == _GAME_ID
        assert not (tmp_path / ".logs").exists()


# ════════════════════════════════════════════════════════════════════
# 3. 예외 처리 — partial 플래그
# ════════════════════════════════════════════════════════════════════


class TestPartialOnException:
    def test_exception_marks_partial_and_propagates(self, tmp_path):
        orch, _ = _make_orchestrator(raise_on="hypothesis")
        m = MetricsCollector(log_dir=tmp_path)

        with pytest.raises(RuntimeError, match="hypothesis exploded"):
            orch.run(_GAME_ID, _PERIOD, metrics=m)

        # finally 블록이 flush 호출 → 파일 생성 + partial 플래그
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        assert log["partial"] is True

    def test_partial_run_still_records_completed_spans(self, tmp_path):
        """예외 직전까지 완료된 span 은 손실되지 않는다."""
        orch, _ = _make_orchestrator(raise_on="hypothesis")
        m = MetricsCollector(log_dir=tmp_path)

        with pytest.raises(RuntimeError):
            orch.run(_GAME_ID, _PERIOD, metrics=m)

        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        names = [s["name"] for s in log["spans"]]
        assert "bottleneck" in names  # ① 는 통과
        assert "segmentation" in names  # ② 도 통과
        # ③ hypothesis 는 예외라 ok=False 로 기록
        hyp_spans = [s for s in log["spans"] if s["name"] == "hypothesis"]
        assert hyp_spans
        assert hyp_spans[0]["ok"] is False


# ════════════════════════════════════════════════════════════════════
# 4. 측정 부하 ≤ 100ms
# ════════════════════════════════════════════════════════════════════


class TestMeasurementOverhead:
    def test_metrics_overhead_below_100ms(self, tmp_path, monkeypatch):
        """metrics 적용 분석 시간 - metrics 미적용 분석 시간 ≤ 100ms.

        6 에이전트 mock 이 즉시 반환하므로 측정 외 시간은 거의 0 →
        차이가 곧 metrics 모듈의 누적 부하.
        """
        monkeypatch.chdir(tmp_path)

        orch_no, _ = _make_orchestrator()
        t0 = time.perf_counter()
        orch_no.run(_GAME_ID, _PERIOD)
        baseline_ms = (time.perf_counter() - t0) * 1000

        orch_yes, _ = _make_orchestrator()
        m = MetricsCollector(log_dir=tmp_path)
        t0 = time.perf_counter()
        orch_yes.run(_GAME_ID, _PERIOD, metrics=m)
        with_metrics_ms = (time.perf_counter() - t0) * 1000

        overhead = with_metrics_ms - baseline_ms
        assert overhead < 100, (
            f"측정 부하 한도 초과: {overhead:.1f}ms (baseline={baseline_ms:.1f}, "
            f"with_metrics={with_metrics_ms:.1f})"
        )


# ════════════════════════════════════════════════════════════════════
# 5. flush 실패가 분석 결과를 죽이지 않는다 (degraded)
# ════════════════════════════════════════════════════════════════════


class TestFlushDegraded:
    def test_flush_exception_does_not_break_pipeline(self, tmp_path, capsys):
        orch, _ = _make_orchestrator()
        m = MetricsCollector(log_dir=tmp_path)

        # flush 가 예외를 내도 pipeline.run 의 finally 가 잡아낸다
        with patch.object(m, "flush", side_effect=RuntimeError("flush oops")):
            report = orch.run(_GAME_ID, _PERIOD, metrics=m)

        assert report.game_id == _GAME_ID  # 분석 결과 보존
        captured = capsys.readouterr()
        assert "flush failed" in captured.err
