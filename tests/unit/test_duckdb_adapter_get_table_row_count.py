"""DuckDBAdapter.get_table_row_count 단위 테스트 (mock-data-viewer 전용).

검증 범위:
- 화이트리스트 검증: 외부 테이블 → ValueError
- 정상 테이블 → 양수 row 수 반환
- 양 도메인의 모든 allowed_tables 호출 일관성
"""

from __future__ import annotations

import pytest

from datapilot.domain import DOMAINS
from datapilot.repository.duckdb_adapter import _PROJECT_ROOT, DuckDBAdapter


@pytest.fixture(scope="module", params=["game", "ecommerce"])
def adapter(request):
    domain = request.param
    db_path = _PROJECT_ROOT / DOMAINS[domain].db_path
    if not db_path.exists():
        pytest.skip(f"{domain} mock DB 부재: {db_path}")
    a = DuckDBAdapter(domain=domain)
    yield a
    a.close()


class TestWhitelist:
    def test_outside_whitelist_raises(self, adapter):
        with pytest.raises(ValueError, match="화이트리스트 외"):
            adapter.get_table_row_count("information_schema_columns")

    def test_sql_keyword_as_table_raises(self, adapter):
        with pytest.raises(ValueError, match="화이트리스트 외"):
            adapter.get_table_row_count('users; DROP TABLE users')


class TestRowCount:
    def test_daily_kpi_row_count_matches_30_days(self, adapter):
        """daily_kpi 는 30 일치 mock — row 30."""
        n = adapter.get_table_row_count("daily_kpi")
        assert n == 30, f"{adapter._domain}.daily_kpi row 수 30 기대, 실제 {n}"

    def test_returns_int(self, adapter):
        first_table = next(iter(DOMAINS[adapter._domain].allowed_tables))
        n = adapter.get_table_row_count(first_table)
        assert isinstance(n, int)
        assert n >= 0

    def test_every_allowed_table_returns_count(self, adapter):
        for table in DOMAINS[adapter._domain].allowed_tables:
            n = adapter.get_table_row_count(table)
            assert isinstance(n, int) and n >= 0, (
                f"{adapter._domain}.{table}: 결과가 비음수 int 가 아님"
            )
