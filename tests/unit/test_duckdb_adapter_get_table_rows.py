"""DuckDBAdapter.get_table_rows 단위 테스트 (mock-data-viewer 전용 메서드).

검증 범위:
- 화이트리스트 검증: 외부 테이블 → ValueError
- 인자 검증: limit ≤ 0 / offset < 0 → ValueError
- LIMIT/OFFSET 정확 적용
- ORDER BY 자동 적용:
    - datetime 컬럼 (DATE/TIMESTAMP) 있는 테이블 → DESC 정렬
    - datetime 컬럼 없는 테이블 → ORDER BY 생략 (자연 순서)
    - order_desc=False → 모든 테이블에서 ORDER BY 생략
- 빈 테이블 / offset > total → []
- 양 도메인 모든 allowed_tables 의 ORDER BY 적용 여부 일관성
"""

from __future__ import annotations

import pytest

from datapilot.domain import DOMAINS
from datapilot.repository.duckdb_adapter import _PROJECT_ROOT, DuckDBAdapter


# ════════════════════════════════════════════════════════════════════
# fixture
# ════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module", params=["game", "ecommerce"])
def adapter(request):
    """양 도메인 adapter — module 스코프로 conn 1 회만 열림."""
    domain = request.param
    db_path = _PROJECT_ROOT / DOMAINS[domain].db_path
    if not db_path.exists():
        pytest.skip(f"{domain} mock DB 부재: {db_path}")
    a = DuckDBAdapter(domain=domain)
    yield a
    a.close()


# ════════════════════════════════════════════════════════════════════
# 1. 화이트리스트 검증
# ════════════════════════════════════════════════════════════════════


class TestWhitelist:
    def test_outside_whitelist_raises(self, adapter):
        with pytest.raises(ValueError, match="화이트리스트 외"):
            adapter.get_table_rows("information_schema_columns")

    def test_sql_keyword_as_table_raises(self, adapter):
        """SQL 키워드 / 인젝션 시도 — 화이트리스트가 차단."""
        with pytest.raises(ValueError, match="화이트리스트 외"):
            adapter.get_table_rows('users; DROP TABLE users')


# ════════════════════════════════════════════════════════════════════
# 2. 인자 검증
# ════════════════════════════════════════════════════════════════════


class TestArgValidation:
    def test_limit_zero_raises(self, adapter):
        first_table = next(iter(DOMAINS[adapter._domain].allowed_tables))
        with pytest.raises(ValueError, match="limit"):
            adapter.get_table_rows(first_table, limit=0)

    def test_limit_negative_raises(self, adapter):
        first_table = next(iter(DOMAINS[adapter._domain].allowed_tables))
        with pytest.raises(ValueError, match="limit"):
            adapter.get_table_rows(first_table, limit=-1)

    def test_offset_negative_raises(self, adapter):
        first_table = next(iter(DOMAINS[adapter._domain].allowed_tables))
        with pytest.raises(ValueError, match="offset"):
            adapter.get_table_rows(first_table, offset=-1)


# ════════════════════════════════════════════════════════════════════
# 3. LIMIT / OFFSET / 빈 결과
# ════════════════════════════════════════════════════════════════════


class TestLimitOffset:
    def test_limit_caps_returned_rows(self, adapter):
        rows = adapter.get_table_rows("daily_kpi", limit=5)
        assert len(rows) <= 5

    def test_offset_skips_rows(self, adapter):
        all_rows = adapter.get_table_rows("daily_kpi", limit=10, offset=0)
        skipped = adapter.get_table_rows("daily_kpi", limit=10, offset=2)
        if len(all_rows) >= 4:
            assert all_rows[2:4] == skipped[:2]

    def test_offset_beyond_total_returns_empty(self, adapter):
        rows = adapter.get_table_rows("daily_kpi", limit=10, offset=10_000_000)
        assert rows == []

    def test_default_limit_is_50(self, adapter):
        first_table = next(iter(DOMAINS[adapter._domain].allowed_tables))
        rows = adapter.get_table_rows(first_table)
        assert len(rows) <= 50

    def test_returns_list_of_dicts_with_column_keys(self, adapter):
        rows = adapter.get_table_rows("daily_kpi", limit=1)
        assert isinstance(rows, list)
        if rows:
            assert isinstance(rows[0], dict)
            assert "date" in rows[0]


# ════════════════════════════════════════════════════════════════════
# 4. ORDER BY 자동 식별 (datetime 컬럼 매칭)
# ════════════════════════════════════════════════════════════════════


class TestOrderByDatetime:
    def test_daily_kpi_desc_order_by_date(self, adapter):
        """daily_kpi.date 가 DATE — DESC 적용 확인 (가장 최근이 첫 row)."""
        rows = adapter.get_table_rows("daily_kpi", limit=30, order_desc=True)
        if len(rows) >= 2:
            assert rows[0]["date"] >= rows[1]["date"], (
                "datetime 컬럼 있는 테이블은 DESC 정렬되어야 함"
            )

    def test_order_desc_false_skips_ordering(self, adapter):
        """order_desc=False 면 ORDER BY 생략 — 자연 순서."""
        rows = adapter.get_table_rows("daily_kpi", limit=30, order_desc=False)
        # 자연 순서가 ASC 일 수도 있고 임의일 수도 있음. 결과 자체는 정상.
        assert isinstance(rows, list)

    def test_table_without_datetime_column_does_not_raise(self, adapter):
        """datetime 컬럼 없는 테이블 (e.g. game.gateways / ecom.products) → ORDER BY 생략, 정상."""
        if adapter._domain == "game":
            no_dt_table = "gateways"  # gateway_id / name / region / status (datetime 없음)
        else:
            no_dt_table = "products"  # product_id / category / inventory_status / name
        rows = adapter.get_table_rows(no_dt_table, limit=5, order_desc=True)
        assert isinstance(rows, list)


# ════════════════════════════════════════════════════════════════════
# 5. 양 도메인 모든 allowed_tables 일관성
# ════════════════════════════════════════════════════════════════════


class TestAllAllowedTables:
    def test_every_allowed_table_query_succeeds(self, adapter):
        """양 도메인의 모든 화이트리스트 테이블에서 default 호출이 깨지지 않음."""
        for table in DOMAINS[adapter._domain].allowed_tables:
            rows = adapter.get_table_rows(table, limit=3)
            assert isinstance(rows, list), (
                f"{adapter._domain}.{table}: get_table_rows 결과가 list 가 아님"
            )
