"""BigQueryAdapter 스텁 테스트.

모든 메서드가 NotImplementedError를 발생시키는지 검증한다.
parametrize로 5개 메서드를 한 번에 커버한다.

Java 비유:
    각 메서드가 UnsupportedOperationException을 던지는
    미구현 Repository 구현체를 검증하는 테스트.
"""

from __future__ import annotations

from datetime import date

import pytest

from datapilot.repository.bigquery_adapter import BigQueryAdapter

# ──────────────────────────────────────────────────────────────────
# Fixture
# ──────────────────────────────────────────────────────────────────

GAME_ID = "pizza_ready"
PERIOD = (date(2026, 3, 1), date(2026, 3, 31))


@pytest.fixture(scope="module")
def bq_adapter():
    """BigQueryAdapter 스텁 인스턴스 (실제 GCP 연결 없음)."""
    return BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")


# ──────────────────────────────────────────────────────────────────
# 메서드별 호출 인자 정의
# ──────────────────────────────────────────────────────────────────

def _call_get_daily_kpi(adapter):
    adapter.get_daily_kpi(GAME_ID, PERIOD)


def _call_get_metric_by_segments(adapter):
    adapter.get_metric_by_segments(GAME_ID, "revenue", PERIOD, ["platform"])


def _call_get_available_dimensions(adapter):
    adapter.get_available_dimensions(GAME_ID)


def _call_get_available_schema(adapter):
    adapter.get_available_schema(GAME_ID)


def _call_execute_readonly_sql(adapter):
    adapter.execute_readonly_sql("SELECT 1")


_STUB_METHODS = [
    ("get_daily_kpi",              _call_get_daily_kpi),
    ("get_metric_by_segments",     _call_get_metric_by_segments),
    ("get_available_dimensions",   _call_get_available_dimensions),
    ("get_available_schema",       _call_get_available_schema),
    ("execute_readonly_sql",       _call_execute_readonly_sql),
]


# ──────────────────────────────────────────────────────────────────
# 테스트
# ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("method_name,call_fn", _STUB_METHODS)
def test_raises_not_implemented_error(bq_adapter, method_name, call_fn):
    """BigQueryAdapter의 모든 메서드는 NotImplementedError를 발생시켜야 한다."""
    with pytest.raises(NotImplementedError):
        call_fn(bq_adapter)


@pytest.mark.parametrize("method_name,call_fn", _STUB_METHODS)
def test_not_implemented_error_message_contains_method_name(bq_adapter, method_name, call_fn):
    """NotImplementedError 메시지에 메서드명이 포함되어야 한다."""
    with pytest.raises(NotImplementedError, match=method_name):
        call_fn(bq_adapter)


def test_adapter_stores_project_and_dataset_ids():
    """생성자에 전달된 project_id와 dataset_id를 속성으로 보관해야 한다."""
    adapter = BigQueryAdapter(project_id="my-project", dataset_id="my_dataset")

    assert adapter.project_id == "my-project"
    assert adapter.dataset_id == "my_dataset"


def test_adapter_is_instance_of_game_data_repository():
    """BigQueryAdapter는 GameDataRepository를 구현해야 한다 (is-a 관계)."""
    from datapilot.repository.port import GameDataRepository

    adapter = BigQueryAdapter(project_id="p", dataset_id="d")

    assert isinstance(adapter, GameDataRepository)