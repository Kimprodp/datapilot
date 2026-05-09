"""viewer._filter_to_allowed_tables 단위 테스트.

이중 안전 필터 — get_available_schema 결과에서 화이트리스트 외 테이블 제거.
mock DB 시드가 미래에 변경되어 화이트리스트 외 테이블이 들어와도 UI 노출 0 보장.
"""

from __future__ import annotations

from datapilot.viewer import _filter_to_allowed_tables


ALLOWED = frozenset({"daily_kpi", "users", "products"})


class TestFilterToAllowedTables:
    def test_keeps_only_allowed_tables(self):
        schema = {
            "tables": [
                {"name": "daily_kpi", "columns": ["date"]},
                {"name": "users", "columns": ["user_id"]},
                {"name": "secret_table", "columns": ["leak"]},
                {"name": "products", "columns": ["product_id"]},
            ]
        }
        result = _filter_to_allowed_tables(schema, ALLOWED)
        names = {t["name"] for t in result}
        assert names == {"daily_kpi", "users", "products"}

    def test_removes_all_when_none_allowed(self):
        schema = {
            "tables": [
                {"name": "internal_log"},
                {"name": "tmp_cache"},
            ]
        }
        result = _filter_to_allowed_tables(schema, ALLOWED)
        assert result == []

    def test_empty_schema_returns_empty(self):
        assert _filter_to_allowed_tables({"tables": []}, ALLOWED) == []

    def test_none_schema_returns_empty(self):
        assert _filter_to_allowed_tables(None, ALLOWED) == []

    def test_schema_without_tables_key_returns_empty(self):
        assert _filter_to_allowed_tables({}, ALLOWED) == []

    def test_table_without_name_key_skipped(self):
        """방어 — name 키 없는 항목은 자연 제거 (in 비교 결과 False)."""
        schema = {"tables": [{"columns": ["x"]}, {"name": "users"}]}
        result = _filter_to_allowed_tables(schema, ALLOWED)
        assert [t["name"] for t in result] == ["users"]
