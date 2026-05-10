"""데모 fixture 갱신 도구 — 라이브 분석 1회 + PipelineReport JSON dump.

데모 모드 (`run_demo`) 가 사용하는 fixture 를 라이브 결과로 새로 캡처.
mock 변경 / LLM 정확도 개선 / 모델 업그레이드 후 갱신 시 사용.

사용:
    uv run python scripts/dump_demo_fixture.py --domain game
    uv run python scripts/dump_demo_fixture.py --domain ecommerce

출력:
    1. 콘솔 — 시간 / anomaly 개수 / 가설 / SQL / 인과 체인 요약
    2. ``data/demo/<domain>_demo.json`` — PipelineReport.model_dump_json()
       (``datapilot/demo.py`` 의 빌더가 이 파일 로드. git 추적 — 갱신 시 commit 필요)
    3. ``.logs/<run_id>.jsonl`` — MetricsCollector 의 메트릭 로그

fixture 갱신 절차:
    1) 본 명령 실행 (게임/이커머스 각 1회, ~5분/회 + Anthropic API 비용)
    2) 결과 검토 (콘솔 시간/anomaly/체인 요약)
    3) 만족스러우면 ``data/demo/<domain>_demo.json`` git commit + push
    4) Streamlit Cloud 자동 재배포 → 새 데모 데이터로 시연
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datapilot.agents import AgentBundle  # noqa: E402
from datapilot.domain import DOMAINS  # noqa: E402
from datapilot.observability import MetricsCollector  # noqa: E402
from datapilot.pipeline import PipelineOrchestrator  # noqa: E402
from datapilot.repository import make_repository  # noqa: E402

PERIOD = (date(2026, 3, 2), date(2026, 3, 31))
DEFAULT_FIXTURE_DIR = Path("data/demo")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--domain",
        type=str,
        default="game",
        choices=sorted(DOMAINS.keys()),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="출력 JSON 경로 (기본: data/demo/<domain>_demo.json)",
    )
    args = parser.parse_args()

    out_path = args.out or DEFAULT_FIXTURE_DIR / f"{args.domain}_demo.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    metrics = MetricsCollector()
    print(f"\n{'=' * 60}")
    print(f"라이브 분석 시작 — {args.domain}")
    print("=" * 60)

    t0 = time.time()
    with make_repository(args.domain) as repo:
        agents = AgentBundle.create(args.domain, repo=repo)
        orch = PipelineOrchestrator(repo, agents=agents)
        entity_id = DOMAINS[args.domain].ui_labels.entity_default_id
        report = orch.run(entity_id, PERIOD, metrics=metrics)
    elapsed = time.time() - t0

    out_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

    print(f"\n{'=' * 60}")
    print("라이브 분석 결과 요약")
    print("=" * 60)
    print(f"  소요 시간: {elapsed:.1f} 초 ({elapsed / 60:.2f} 분)")
    print(f"  segmentable 분석: {len(report.analyzed)}")
    print(f"  미지원 지표: {len(report.unanalyzed)}")
    for a in report.analyzed:
        n_validation = len(a.validation_results)
        n_supported = sum(1 for v in a.validation_results if v.status == "supported")
        n_total_sql = sum(len(v.queries_run) for v in a.validation_results)
        chain_len = len(a.root_cause.root_cause.chain)
        rc_summary = a.root_cause.root_cause.summary
        print(
            f"  - {a.anomaly.metric}: "
            f"가설 {n_validation} (supported {n_supported}) / "
            f"SQL 총 {n_total_sql}회 / "
            f"체인 {chain_len}단계 — {rc_summary[:40]}..."
        )
    print(f"\n  PipelineReport JSON: {out_path}")
    print(f"  메트릭 로그: .logs/{metrics.run_id}.jsonl")
    print("\n  fixture 만족스러우면 git commit + push → Streamlit Cloud 자동 재배포.")


if __name__ == "__main__":
    main()
