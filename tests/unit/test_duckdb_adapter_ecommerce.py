"""DuckDBAdapter 의 이커머스 도메인 동작 단위 테스트.

검증 범위:
- get_daily_kpi: 이커머스 5 KPI 컬럼 (gmv / orders / conversion / visitors /
  payment_success_rate) 반환
- get_available_dimensions: customers 테이블의 세그먼트 컬럼
- get_metric_by_segments: gmv / orders 분해
- 시나리오 B/C 검증 SQL 가능 (orders ↓ from D-7, gmv ↓ from D-3)
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from datapilot.domain import DOMAINS
from datapilot.repository.duckdb_adapter import _PROJECT_ROOT, DuckDBAdapter

ECOMMERCE_DB_PATH = _PROJECT_ROOT / DOMAINS["ecommerce"].db_path

# 이커머스 mock 의 데이터 기간 — seed_mock_data.py 와 일치
PERIOD_FULL = (date(2026, 3, 2), date(2026, 3, 31))
PERIOD_LAST10 = (date(2026, 3, 22), date(2026, 3, 31))

ECOMMERCE_ENTITY_ID = "ecommerce_demo"


@pytest.fixture(scope="module")
def ecommerce_adapter():
    if not ECOMMERCE_DB_PATH.exists():
        pytest.skip(f"이커머스 mock DB 없음: {ECOMMERCE_DB_PATH}")
    adapter = DuckDBAdapter(domain="ecommerce")
    yield adapter
    adapter.close()


# ════════════════════════════════════════════════════════════════════
# 1. get_daily_kpi (이커머스)
# ════════════════════════════════════════════════════════════════════


class TestGetDailyKpiEcommerce:
    def test_returns_top_level_keys(self, ecommerce_adapter):
        result = ecommerce_adapter.get_daily_kpi(
            ECOMMERCE_ENTITY_ID, PERIOD_FULL,
        )
        assert set(result.keys()) == {"entity_id", "period", "daily"}
        assert result["entity_id"] == ECOMMERCE_ENTITY_ID

    def test_returns_30_days(self, ecommerce_adapter):
        result = ecommerce_adapter.get_daily_kpi(
            ECOMMERCE_ENTITY_ID, PERIOD_FULL,
        )
        assert len(result["daily"]) == 30

    def test_daily_row_has_ecommerce_kpis(self, ecommerce_adapter):
        result = ecommerce_adapter.get_daily_kpi(
            ECOMMERCE_ENTITY_ID, PERIOD_FULL,
        )
        row = result["daily"][0]
        expected_keys = {
            "date",
            "gmv",
            "orders",
            "conversion",
            "visitors",
            "payment_success_rate",
        }
        assert set(row.keys()) == expected_keys

    def test_daily_row_no_game_kpis(self, ecommerce_adapter):
        """이커머스 daily_kpi 에 게임 컬럼 (dau/mau/revenue/...) 없음."""
        result = ecommerce_adapter.get_daily_kpi(
            ECOMMERCE_ENTITY_ID, PERIOD_FULL,
        )
        row = result["daily"][0]
        forbidden = {"dau", "mau", "revenue", "arppu", "d7_retention"}
        for k in forbidden:
            assert k not in row, f"이커머스 daily 에 게임 컬럼 {k} 등장"


# ════════════════════════════════════════════════════════════════════
# 2. 시나리오 B/C 신호 검증
# ════════════════════════════════════════════════════════════════════


class TestScenarioSignals:
    def test_scenario_b_orders_drop_from_d_minus_7(self, ecommerce_adapter):
        """시나리오 B: D-7 (2026-03-24) 부터 orders 약 12.5% 감소."""
        result = ecommerce_adapter.get_daily_kpi(
            ECOMMERCE_ENTITY_ID, PERIOD_LAST10,
        )
        # D-7 이전 (2026-03-23 까지): orders 평균 ~500
        # D-7 이후 (2026-03-24 부터): orders 평균 ~437
        before = [
            r["orders"]
            for r in result["daily"]
            if r["date"] < "2026-03-24"
        ]
        after = [
            r["orders"]
            for r in result["daily"]
            if r["date"] >= "2026-03-24"
        ]
        avg_before = sum(before) / len(before)
        avg_after = sum(after) / len(after)
        drop_ratio = (avg_before - avg_after) / avg_before
        # 약 12.5% 감소 (kitchen 카테고리 50% ↓ → 전체 1/4 의 50%)
        assert 0.08 < drop_ratio < 0.20, (
            f"시나리오 B 신호 강도 이상: D-7 전후 orders 변화율 {drop_ratio:.1%}"
        )



# ════════════════════════════════════════════════════════════════════
# 3. get_available_dimensions (이커머스)
# ════════════════════════════════════════════════════════════════════


class TestGetAvailableDimensionsEcommerce:
    def test_returns_customers_segment_columns(self, ecommerce_adapter):
        dims = ecommerce_adapter.get_available_dimensions(ECOMMERCE_ENTITY_ID)
        # customers 테이블의 세그먼트 컬럼 (customer_id 제외)
        expected = {"country", "customer_type", "device"}
        assert set(dims) == expected

    def test_no_customer_id_in_dimensions(self, ecommerce_adapter):
        dims = ecommerce_adapter.get_available_dimensions(ECOMMERCE_ENTITY_ID)
        assert "customer_id" not in dims


# ════════════════════════════════════════════════════════════════════
# 4. get_metric_by_segments (이커머스)
# ════════════════════════════════════════════════════════════════════


class TestGetMetricBySegmentsEcommerce:
    def test_gmv_by_country(self, ecommerce_adapter):
        result = ecommerce_adapter.get_metric_by_segments(
            ECOMMERCE_ENTITY_ID,
            metric="gmv",
            period=PERIOD_FULL,
            dimensions=["country"],
        )
        assert "segments" in result
        assert "country" in result["segments"]
        # 4 국가
        countries = result["segments"]["country"]
        assert set(countries.keys()) == {"korea", "usa", "japan", "germany"}

    def test_orders_by_customer_type(self, ecommerce_adapter):
        result = ecommerce_adapter.get_metric_by_segments(
            ECOMMERCE_ENTITY_ID,
            metric="orders",
            period=PERIOD_FULL,
            dimensions=["customer_type"],
        )
        assert "customer_type" in result["segments"]
        types = result["segments"]["customer_type"]
        assert set(types.keys()) == {"new", "returning", "vip"}

    def test_unsupported_metric_raises(self, ecommerce_adapter):
        with pytest.raises(ValueError) as exc_info:
            ecommerce_adapter.get_metric_by_segments(
                ECOMMERCE_ENTITY_ID,
                metric="dau",  # 게임 metric — 이커머스 지원 X
                period=PERIOD_FULL,
                dimensions=["country"],
            )
        assert "이커머스" in str(exc_info.value) or "지원하지 않는" in str(exc_info.value)


# ════════════════════════════════════════════════════════════════════
# 5. domain 인자 무결성
# ════════════════════════════════════════════════════════════════════


class TestDomainAttribute:
    def test_ecommerce_adapter_has_domain_set(self, ecommerce_adapter):
        assert ecommerce_adapter._domain == "ecommerce"
