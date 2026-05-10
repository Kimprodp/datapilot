"""데모 모드 — API 호출 없이 파이프라인 시뮬레이션.

라이브 분석 결과를 ``data/demo/<domain>_demo.json`` fixture 로 보존하고,
진행 콜백을 실제 파이프라인과 동일한 순서·구조로 발화한다. 영상 촬영·빠른 데모 용도.

fixture 갱신: ``scripts/dump_demo_fixture.py`` 로 라이브 1회 → JSON 캡처 → commit.
"""

from __future__ import annotations

import time
from pathlib import Path

from datapilot.pipeline import (
    AnomalyAnalysis,
    OnStepCallback,
    PipelineReport,
    PipelineStep,
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
# fixture 경로 (data/demo/<domain>_demo.json)
# ------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_FIXTURE_DIR = _PROJECT_ROOT / "data" / "demo"


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
# 리포트 로드 — 라이브 결과 JSON fixture 그대로 사용
# ------------------------------------------------------------------


def _load_fixture(domain: str) -> PipelineReport:
    path = _FIXTURE_DIR / f"{domain}_demo.json"
    if not path.exists():
        raise FileNotFoundError(
            f"데모 fixture 파일이 없습니다: {path}. "
            f"scripts/dump_demo_fixture.py --domain {domain} 로 먼저 생성하세요."
        )
    return PipelineReport.model_validate_json(path.read_text(encoding="utf-8"))


def build_demo_report() -> PipelineReport:
    """게임 도메인 데모 리포트 (legacy 호환 — 디폴트 도메인)."""
    return _load_fixture("game")


def build_demo_report_ecommerce() -> PipelineReport:
    """이커머스 도메인 데모 리포트."""
    return _load_fixture("ecommerce")


_DEMO_BUILDERS = {
    "game": build_demo_report,
    "ecommerce": build_demo_report_ecommerce,
}


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
