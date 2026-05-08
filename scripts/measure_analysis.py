"""분석 측정 스크립트 — 분석 N회 연속 실행 + .logs/<run_id>.jsonl 출력.

사용:
    uv run python scripts/measure_analysis.py                          # 게임, 연속 2회
    uv run python scripts/measure_analysis.py --runs 1                 # 게임, 단발 1회
    uv run python scripts/measure_analysis.py --domain ecommerce       # 이커머스, 연속 2회
    uv run python scripts/measure_analysis.py --domain ecommerce --runs 1

연속 2회: 1회차 cache_creation → 2회차 cache_read 로 캐싱 작동 입증.
단발 1회: cache_creation 비용만 측정 (운영 기본 패턴).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

# scripts/ 안에서 실행할 때 프로젝트 루트를 import 경로에 넣는다.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datapilot.agents import AgentBundle  # noqa: E402
from datapilot.domain import DOMAINS  # noqa: E402
from datapilot.observability import MetricsCollector  # noqa: E402
from datapilot.pipeline import PipelineOrchestrator  # noqa: E402
from datapilot.repository import make_repository  # noqa: E402
from datapilot.repository.duckdb_adapter import DuckDBAdapter  # noqa: E402

# Mock 데이터 기준일 2026-03-31, 최근 30일 (app.py 기본 period 와 동일)
PERIOD = (date(2026, 3, 2), date(2026, 3, 31))


def _run_once(
    repo: DuckDBAdapter,
    domain: str,
    run_idx: int,
    total: int,
) -> str:
    print(f"\n{'=' * 60}")
    print(f"[run {run_idx}/{total}] 분석 시작 ({domain})")
    print("=" * 60)

    metrics = MetricsCollector()
    agents = AgentBundle.create(domain, repo=repo)
    orch = PipelineOrchestrator(repo, agents=agents)
    entity_id = DOMAINS[domain].ui_labels.entity_default_id
    report = orch.run(entity_id, PERIOD, metrics=metrics)

    print(f"\n[run {run_idx}/{total}] 결과 요약")
    print(f"  segmentable 분석: {len(report.analyzed)}")
    print(f"  미지원 지표: {len(report.unanalyzed)}")
    for a in report.analyzed:
        chain_len = len(a.root_cause.root_cause.chain)
        rc_summary = a.root_cause.root_cause.summary
        print(f"  - {a.anomaly.metric}: 체인 {chain_len}단계 / {rc_summary}")
    print(f"  메트릭 로그: .logs/{metrics.run_id}.jsonl")
    return metrics.run_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--domain",
        type=str,
        default="game",
        choices=sorted(DOMAINS.keys()),
        help="분석할 도메인 (default: game)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=2,
        help="연속 실행 횟수 (default: 2 — 캐시 효과 비교)",
    )
    args = parser.parse_args()

    run_ids = []
    with make_repository(args.domain) as repo:
        for i in range(1, args.runs + 1):
            run_ids.append(_run_once(repo, args.domain, i, args.runs))

    print(f"\n{'=' * 60}")
    print("측정 종료. 다음 명령으로 캐시 사용량 비교:")
    print("=" * 60)
    for rid in run_ids:
        print(f"  cat .logs/{rid}.jsonl")


if __name__ == "__main__":
    main()
