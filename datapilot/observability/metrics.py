"""분석 파이프라인 측정.

분석 1회의 단계별 latency / 토큰 / cache 적중률을 in-memory 로 누적하고,
종료 시 콘솔 + .logs/<run_id>.jsonl 파일로 출력한다.

Java 비유:
    Micrometer Timer + Prometheus Counter 를 합친 단순 버전.
    `with collector.span("agent")` 가 try-with-resources 처럼 작동하며,
    LangChain BaseCallbackHandler 를 구현하므로 chain.invoke 의
    callbacks 로 전달하면 LLM 응답 usage_metadata 가 자동 추출된다.

설계 원칙:
- in-memory 누적 + 분석 종료 시 1회 batch flush (측정 부하 ≤ 100ms 보장)
- degraded mode: 측정 실패가 분석을 중단시키지 않는다 (try/except + stderr 1회)
- 민감 데이터 미수록: SQL 본문 / hypothesis 텍스트는 저장하지 않는다.
  span tags 는 스칼라만, usage_metadata 는 화이트리스트 키만 기록.
"""

from __future__ import annotations

import json
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult

_DEFAULT_LOG_DIR = Path(".logs")

# usage_metadata 에서 메트릭 로그에 기록을 허용하는 키 화이트리스트.
# 다른 키 (request id 등) 는 기록하지 않는다.
_USAGE_KEYS: tuple[str, ...] = (
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
)


class MetricsCollector(BaseCallbackHandler):
    """단계별 latency + LLM 토큰/cache 사용량을 누적해 분석 종료 시 flush 한다.

    LangChain BaseCallbackHandler 를 구현하므로
    ``chain.invoke(input, config={"callbacks": [collector]})`` 로
    전달하면 ``on_llm_end`` 에서 usage_metadata 가 자동 추출된다.

    Java 비유: Micrometer Timer (span) + Prometheus Counter (usage)
    를 단일 클래스에 합친 형태.
    """

    def __init__(
        self,
        *,
        run_id: str | None = None,
        log_dir: Path | None = None,
    ) -> None:
        self.run_id = run_id or datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )
        self._log_dir = log_dir if log_dir is not None else _DEFAULT_LOG_DIR
        self._spans: list[dict[str, Any]] = []
        self._llm_calls: list[dict[str, Any]] = []
        self._partial = False
        self._flushed = False
        self._degraded_warned = False

    # ── 공개 인터페이스 ────────────────────────────────────────

    @contextmanager
    def span(self, name: str, **tags: Any) -> Iterator[None]:
        """블록 실행 시간을 측정해 span 으로 기록한다.

        Args:
            name: 단계 이름 (예: "bottleneck", "validator_round").
            tags: 추가 메타정보 (예: ``metric="revenue"``).
                  스칼라(str/int/float/bool) 만 기록되며,
                  SQL/hypothesis 본문 같은 민감 데이터는 자동으로 걸러진다.
        """
        start = time.perf_counter()
        ok = True
        try:
            yield
        except Exception:
            ok = False
            raise
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            self._record({
                "name": name,
                "elapsed_ms": round(elapsed_ms, 3),
                "ok": ok,
                **{
                    k: v
                    for k, v in tags.items()
                    if isinstance(v, (str, int, float, bool))
                },
            })

    def mark_partial(self) -> None:
        """분석 중단/예외 발생 시 호출. flush 출력에 partial 플래그를 남긴다."""
        self._partial = True

    @property
    def partial(self) -> bool:
        return self._partial

    def flush(self) -> None:
        """누적된 메트릭을 콘솔 + 파일에 batch 출력한다.

        - 콘솔 출력 실패 시 stderr 경고 후 무시 (분석 결과 보호)
        - 파일 I/O 실패 시 stderr 경고 후 콘솔 출력은 유지
        - 멱등: 두 번 호출되면 두 번째는 no-op
        """
        if self._flushed:
            return
        self._flushed = True
        summary = self._summarize()
        self._print_summary(summary)
        self._write_jsonl(summary)

    # ── LangChain 콜백 ────────────────────────────────────────

    def on_llm_end(self, response: LLMResult, **_: Any) -> None:
        """LangChain 이 LLM 호출 종료 시 자동 호출. usage_metadata 추출."""
        try:
            for gen_list in response.generations:
                for gen in gen_list:
                    usage = self._extract_usage(gen)
                    if usage:
                        self._llm_calls.append(usage)
        except Exception as e:  # degraded
            self._warn_once(f"on_llm_end failed: {e}")

    # ── 내부 헬퍼 ─────────────────────────────────────────────

    def _record(self, span: dict[str, Any]) -> None:
        try:
            self._spans.append(span)
        except Exception as e:  # 사실상 도달 불가지만 degraded 일관성 유지
            self._warn_once(f"span record failed: {e}")

    def _extract_usage(self, generation: Any) -> dict[str, int] | None:
        """LangChain Generation 에서 Anthropic usage 화이트리스트 키만 추출.

        키 이름 정규화:
        - LangChain ``usage_metadata.input_token_details`` 는 짧은 키를 사용
          (``cache_creation`` / ``cache_read``).
        - Anthropic raw API 의 ``response_metadata.usage`` 는 긴 키를 사용
          (``cache_creation_input_tokens`` / ``cache_read_input_tokens``).
        본 메서드는 어느 경로든 후자 (Anthropic raw 키) 로 정규화한다.
        """
        msg = getattr(generation, "message", None)
        if msg is None:
            return None
        # 우선순위: AIMessage.usage_metadata (LangChain 0.2+)
        usage: Any = getattr(msg, "usage_metadata", None)
        if not isinstance(usage, dict):
            # fallback: response_metadata.usage (구버전 / Anthropic raw)
            meta = getattr(msg, "response_metadata", None) or {}
            usage = meta.get("usage", {}) if isinstance(meta, dict) else {}
        if not isinstance(usage, dict):
            return None

        details = usage.get("input_token_details") or {}
        if not isinstance(details, dict):
            details = {}

        # 캐시 키 추출: 긴 키 우선, 없으면 nested 짧은 키
        cache_creation = (
            usage.get("cache_creation_input_tokens")
            or details.get("cache_creation")
            or 0
        )
        cache_read = (
            usage.get("cache_read_input_tokens")
            or details.get("cache_read")
            or 0
        )
        return {
            "input_tokens": int(usage.get("input_tokens", 0) or 0),
            "output_tokens": int(usage.get("output_tokens", 0) or 0),
            "cache_creation_input_tokens": int(cache_creation or 0),
            "cache_read_input_tokens": int(cache_read or 0),
        }

    def _summarize(self) -> dict[str, Any]:
        totals = {
            k: sum(c.get(k, 0) for c in self._llm_calls) for k in _USAGE_KEYS
        }
        return {
            "run_id": self.run_id,
            "partial": self._partial,
            "spans": list(self._spans),
            "llm_calls": list(self._llm_calls),
            "totals": {
                "spans": len(self._spans),
                "llm_calls": len(self._llm_calls),
                **totals,
            },
        }

    def _print_summary(self, summary: dict[str, Any]) -> None:
        try:
            tag = " (partial)" if summary["partial"] else ""
            print(f"\n=== metrics {summary['run_id']}{tag} ===")
            for span in summary["spans"]:
                ok = "OK " if span["ok"] else "ERR"
                extras = " ".join(
                    f"{k}={v}"
                    for k, v in span.items()
                    if k not in ("name", "elapsed_ms", "ok")
                )
                line = f"  {ok} {span['name']:<20} {span['elapsed_ms']:>9.1f} ms"
                if extras:
                    line += f"  {extras}"
                print(line)
            t = summary["totals"]
            print(
                f"  -- llm_calls={t['llm_calls']} "
                f"input={t['input_tokens']} output={t['output_tokens']} "
                f"cache_create={t['cache_creation_input_tokens']} "
                f"cache_read={t['cache_read_input_tokens']}"
            )
        except Exception as e:
            self._warn_once(f"print failed: {e}")

    def _write_jsonl(self, summary: dict[str, Any]) -> None:
        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)
            path = self._log_dir / f"{summary['run_id']}.jsonl"
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(summary, ensure_ascii=False) + "\n")
        except Exception as e:
            self._warn_once(f"file write failed: {e}")

    def _warn_once(self, msg: str) -> None:
        if not self._degraded_warned:
            print(f"[metrics] degraded: {msg}", file=sys.stderr)
            self._degraded_warned = True


class NullMetricsCollector(BaseCallbackHandler):
    """No-op MetricsCollector. metrics=None 인 경우 회귀 방지용 더미.

    BaseCallbackHandler 의 기본 메서드는 모두 no-op 이므로
    callbacks 리스트에 들어가도 부하나 영향이 없다.
    """

    run_id = "null"
    partial = False

    @contextmanager
    def span(self, name: str, **tags: Any) -> Iterator[None]:
        yield

    def mark_partial(self) -> None:
        pass

    def flush(self) -> None:
        pass


# 모듈 전역 no-op 인스턴스. 매번 인스턴스화 비용 회피.
NULL_METRICS: NullMetricsCollector = NullMetricsCollector()
