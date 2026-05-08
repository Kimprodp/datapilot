"""MetricsCollector 단위 테스트.

검증 범위:
- span() 정상/예외 분기 (elapsed 기록 + ok 플래그)
- on_llm_end() usage_metadata 추출 (cache 키 포함, nested 구조 펼침)
- flush() 콘솔 + 파일 출력, 파일 I/O 실패 시 degraded
- mark_partial() / partial 플래그
- degraded mode (record/on_llm_end 예외 시 분석 계속 + stderr 1회)
- 민감 데이터 미수록 (SQL/hypothesis 본문 → flush 결과에 포함되지 않음)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from datapilot.observability.metrics import (
    NULL_METRICS,
    MetricsCollector,
    NullMetricsCollector,
)


# ─── 헬퍼 ────────────────────────────────────────────────────────────
# LangChain UsageMetadata TypedDict 가 strict 검증을 하므로
# 화이트리스트 외 키 / nested 구조 검증을 위해 duck-typed fake 사용.


@dataclass
class _FakeMessage:
    usage_metadata: dict[str, Any] | None = None
    response_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class _FakeGeneration:
    message: Any


@dataclass
class _FakeLLMResult:
    generations: list[list[Any]]


def _llm_result(usage_metadata: dict[str, Any] | None) -> Any:
    """usage_metadata 경로용 fake LLMResult."""
    return _FakeLLMResult(
        generations=[[_FakeGeneration(_FakeMessage(usage_metadata=usage_metadata))]]
    )


def _llm_result_raw_usage(usage: dict[str, Any]) -> Any:
    """response_metadata.usage 경로 fallback 검증용."""
    return _FakeLLMResult(
        generations=[[
            _FakeGeneration(_FakeMessage(response_metadata={"usage": usage}))
        ]]
    )


def _llm_result_empty_generations() -> Any:
    return _FakeLLMResult(generations=[[]])


# ════════════════════════════════════════════════════════════════════
# 1. span()
# ════════════════════════════════════════════════════════════════════


class TestSpan:
    def test_normal_span_records_elapsed_and_ok(self, tmp_path):
        m = MetricsCollector(log_dir=tmp_path)
        with m.span("bottleneck"):
            pass
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        spans = log["spans"]
        assert len(spans) == 1
        assert spans[0]["name"] == "bottleneck"
        assert spans[0]["ok"] is True
        assert spans[0]["elapsed_ms"] >= 0

    def test_span_with_tags_keeps_scalar_values(self, tmp_path):
        m = MetricsCollector(log_dir=tmp_path)
        with m.span("segmentation", metric="revenue", round_idx=3):
            pass
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        spans = log["spans"]
        assert spans[0]["metric"] == "revenue"
        assert spans[0]["round_idx"] == 3

    def test_span_exception_records_ok_false_and_propagates(self, tmp_path):
        m = MetricsCollector(log_dir=tmp_path)
        with pytest.raises(ValueError, match="boom"):
            with m.span("hypothesis"):
                raise ValueError("boom")
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        spans = log["spans"]
        assert spans[0]["ok"] is False

    def test_validator_round_span_records_per_round(self, tmp_path):
        """④ Validator 가 라운드마다 span 호출하는 패턴."""
        m = MetricsCollector(log_dir=tmp_path)
        for _ in range(3):
            with m.span("validator_round"):
                pass
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        rounds = [s for s in log["spans"] if s["name"] == "validator_round"]
        assert len(rounds) == 3


# ════════════════════════════════════════════════════════════════════
# 2. on_llm_end() — usage_metadata 추출
# ════════════════════════════════════════════════════════════════════


class TestOnLlmEnd:
    def test_extracts_all_whitelisted_usage_keys(self, tmp_path):
        m = MetricsCollector(log_dir=tmp_path)
        m.on_llm_end(_llm_result({
            "input_tokens": 100,
            "output_tokens": 50,
            "cache_creation_input_tokens": 1000,
            "cache_read_input_tokens": 200,
        }))
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        call = log["llm_calls"][0]
        assert call["input_tokens"] == 100
        assert call["output_tokens"] == 50
        assert call["cache_creation_input_tokens"] == 1000
        assert call["cache_read_input_tokens"] == 200

    def test_filters_out_non_whitelisted_keys(self, tmp_path):
        """request_id 같은 메타정보는 기록되지 않는다."""
        m = MetricsCollector(log_dir=tmp_path)
        m.on_llm_end(_llm_result({
            "input_tokens": 10,
            "output_tokens": 5,
            "request_id": "should_not_appear",
            "system_fingerprint": "should_not_appear",
        }))
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        call = log["llm_calls"][0]
        assert "request_id" not in call
        assert "system_fingerprint" not in call

    def test_unfolds_nested_input_token_details(self, tmp_path):
        """LangChain 0.2+ 의 nested cache 키 구조 (input_token_details) 펼침."""
        m = MetricsCollector(log_dir=tmp_path)
        m.on_llm_end(_llm_result({
            "input_tokens": 100,
            "output_tokens": 50,
            "input_token_details": {
                "cache_creation_input_tokens": 800,
                "cache_read_input_tokens": 200,
            },
        }))
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        call = log["llm_calls"][0]
        assert call["cache_creation_input_tokens"] == 800
        assert call["cache_read_input_tokens"] == 200

    def test_falls_back_to_response_metadata_usage(self, tmp_path):
        """usage_metadata 가 없으면 response_metadata.usage 에서 추출."""
        m = MetricsCollector(log_dir=tmp_path)
        m.on_llm_end(_llm_result_raw_usage({
            "input_tokens": 7,
            "output_tokens": 3,
        }))
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        call = log["llm_calls"][0]
        assert call["input_tokens"] == 7
        assert call["output_tokens"] == 3

    def test_missing_usage_keys_default_to_zero(self, tmp_path):
        m = MetricsCollector(log_dir=tmp_path)
        m.on_llm_end(_llm_result({"input_tokens": 10}))
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        call = log["llm_calls"][0]
        assert call["output_tokens"] == 0
        assert call["cache_creation_input_tokens"] == 0
        assert call["cache_read_input_tokens"] == 0

    def test_empty_generations_records_nothing(self, tmp_path):
        """generations 가 비어 있으면 기록 X."""
        m = MetricsCollector(log_dir=tmp_path)
        m.on_llm_end(_llm_result_empty_generations())
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        assert log["llm_calls"] == []


# ════════════════════════════════════════════════════════════════════
# 3. flush()
# ════════════════════════════════════════════════════════════════════


class TestFlush:
    def test_creates_log_dir_if_missing(self, tmp_path):
        log_dir = tmp_path / "nested" / ".logs"
        m = MetricsCollector(log_dir=log_dir)
        m.flush()
        assert log_dir.exists()
        assert (log_dir / f"{m.run_id}.jsonl").exists()

    def test_jsonl_file_contains_summary(self, tmp_path):
        m = MetricsCollector(log_dir=tmp_path)
        with m.span("bottleneck"):
            pass
        m.on_llm_end(_llm_result({"input_tokens": 10, "output_tokens": 5}))
        m.flush()
        path = tmp_path / f"{m.run_id}.jsonl"
        log = json.loads(path.read_text("utf-8"))
        assert log["run_id"] == m.run_id
        assert log["partial"] is False
        assert log["totals"]["spans"] == 1
        assert log["totals"]["llm_calls"] == 1
        assert log["totals"]["input_tokens"] == 10

    def test_partial_flag_in_summary(self, tmp_path):
        m = MetricsCollector(log_dir=tmp_path)
        m.mark_partial()
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        assert log["partial"] is True

    def test_console_shows_partial_tag(self, tmp_path, capsys):
        m = MetricsCollector(log_dir=tmp_path)
        m.mark_partial()
        m.flush()
        out = capsys.readouterr().out
        assert "(partial)" in out

    def test_flush_is_idempotent(self, tmp_path):
        m = MetricsCollector(log_dir=tmp_path)
        with m.span("x"):
            pass
        m.flush()
        m.flush()  # 두 번째는 no-op
        path = tmp_path / f"{m.run_id}.jsonl"
        # 파일 내 jsonl 라인 수가 1 이어야 (append 중복 X)
        lines = path.read_text("utf-8").strip().splitlines()
        assert len(lines) == 1

    def test_file_io_failure_keeps_console_output(
        self, tmp_path, capsys, monkeypatch,
    ):
        """파일 쓰기가 실패해도 콘솔은 출력되고 분석은 죽지 않는다."""
        m = MetricsCollector(log_dir=tmp_path)
        with m.span("x"):
            pass

        def _raise_oserror(*_a, **_kw):
            raise OSError("disk full")

        monkeypatch.setattr(Path, "open", _raise_oserror)
        m.flush()  # 예외 전파 X
        captured = capsys.readouterr()
        assert "x" in captured.out  # 콘솔 출력 유지
        assert "degraded" in captured.err  # stderr 경고


# ════════════════════════════════════════════════════════════════════
# 4. degraded mode
# ════════════════════════════════════════════════════════════════════


class TestDegradedMode:
    def test_on_llm_end_exception_does_not_raise(self, tmp_path, capsys):
        """on_llm_end 가 잘못된 입력을 받아도 분석은 죽지 않는다."""
        m = MetricsCollector(log_dir=tmp_path)
        # 일부러 LLMResult 가 아닌 객체 전달
        m.on_llm_end(object())  # type: ignore[arg-type]
        m.flush()
        captured = capsys.readouterr()
        assert "degraded" in captured.err

    def test_degraded_warning_only_once(self, tmp_path, capsys):
        m = MetricsCollector(log_dir=tmp_path)
        for _ in range(5):
            m.on_llm_end(object())  # type: ignore[arg-type]
        captured = capsys.readouterr()
        # "degraded" 라는 단어가 한 번만 등장
        assert captured.err.count("degraded") == 1


# ════════════════════════════════════════════════════════════════════
# 5. 민감 데이터 미수록
# ════════════════════════════════════════════════════════════════════


class TestSensitiveDataNotRecorded:
    def test_sql_in_span_tags_is_filtered(self, tmp_path):
        """span tags 에 SQL 문자열을 넣어도 스칼라만 통과 — 단,
        SQL 자체가 str(스칼라) 라 통과되지 않는 안전장치는 호출자의 책임.
        본 테스트는 의도되지 않은 dict/list 를 silently 차단함을 검증."""
        m = MetricsCollector(log_dir=tmp_path)
        # dict / list 같은 non-scalar 는 자동으로 걸러진다
        with m.span(
            "validation",
            metric="revenue",
            sql_payload={"query": "SELECT * FROM payments"},
            results_list=[{"row": 1}],
        ):
            pass
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        span = log["spans"][0]
        assert "sql_payload" not in span
        assert "results_list" not in span
        assert span["metric"] == "revenue"  # 스칼라는 통과

    def test_llm_call_only_contains_whitelisted_usage_keys(self, tmp_path):
        """on_llm_end 가 SQL 결과 / 메시지 본문 / 가설 텍스트를 저장하지 않음."""
        m = MetricsCollector(log_dir=tmp_path)
        m.on_llm_end(_llm_result({
            "input_tokens": 10,
            "output_tokens": 5,
            "raw_response_text": "Sensitive hypothesis text...",
            "executed_sql": "SELECT * FROM payments",
        }))
        m.flush()
        log = json.loads((tmp_path / f"{m.run_id}.jsonl").read_text("utf-8"))
        call = log["llm_calls"][0]
        # 화이트리스트 4 키만 존재
        assert set(call.keys()) == {
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        }


# ════════════════════════════════════════════════════════════════════
# 6. NullMetricsCollector
# ════════════════════════════════════════════════════════════════════


class TestNullMetricsCollector:
    def test_span_is_noop_context_manager(self):
        with NULL_METRICS.span("anything"):
            pass  # 예외 없이 통과

    def test_flush_is_noop(self):
        NULL_METRICS.flush()  # 예외 없이 통과

    def test_mark_partial_is_noop(self):
        NULL_METRICS.mark_partial()
        assert NULL_METRICS.partial is False

    def test_module_singleton_is_null_metrics_collector(self):
        assert isinstance(NULL_METRICS, NullMetricsCollector)
