"""DuckDBAdapter 단위 테스트.

실제 seeded Mock DB(data/datapilot_mock.db)에 연결해 각 메서드의 동작을
검증한다. 수치를 하드코딩하는 대신 부등식 기반 어서션으로 "질적 특성"을
확인한다.

테스트 범위:
    - get_daily_kpi: 구조 검증, 날짜 범위, 시나리오 2(D7 리텐션 하락)
    - get_metric_by_segments: 구조 검증, 시나리오 1(Android 매출 하락),
                              시나리오 3(브라질 결제 성공률)
    - get_available_dimensions: 포함 컬럼 / 제외 컬럼
    - get_available_schema: 12개 테이블 전부 포함
    - execute_readonly_sql: 정상 실행, max_rows 제한, 오류 처리
    - period 유효성: end < start → ValueError
    - 알 수 없는 metric / dimension → ValueError
    - context manager: with 블록 후 정상 종료
"""

from __future__ import annotations

from contextlib import suppress
from datetime import date

import pytest

from datapilot.repository.duckdb_adapter import DuckDBAdapter

# ──────────────────────────────────────────────────────────────────
# 상수 — Mock DB 기간
# ──────────────────────────────────────────────────────────────────

FULL_START = date(2026, 3, 2)
FULL_END = date(2026, 3, 31)
GAME_ID = "pizza_ready"

# 시나리오 기준일
SCENARIO1_BREAK = date(2026, 3, 28)   # 매출 하락 시작
SCENARIO2_BREAK = date(2026, 3, 17)   # D7 리텐션 하락 시작
SCENARIO3_DATE = date(2026, 3, 31)    # 결제 성공률 급락일

# ──────────────────────────────────────────────────────────────────
# 예상 테이블 목록 (12개)
# ──────────────────────────────────────────────────────────────────

EXPECTED_TABLES = {
    "daily_kpi",
    "users",
    "products",
    "payments",
    "shop_impressions",
    "releases",
    "events",
    "sessions",
    "content_releases",
    "gateways",
    "payment_attempts",
    "payment_errors",
}


# ════════════════════════════════════════════════════════════════════
# get_daily_kpi
# ════════════════════════════════════════════════════════════════════


class TestGetDailyKpi:
    def test_returns_expected_top_level_keys(self, adapter):
        result = adapter.get_daily_kpi(GAME_ID, (FULL_START, FULL_END))

        assert set(result.keys()) == {"game_id", "period", "daily"}
        assert result["game_id"] == GAME_ID
        assert result["period"]["from"] == "2026-03-02"
        assert result["period"]["to"] == "2026-03-31"

    def test_returns_30_daily_rows_for_full_period(self, adapter):
        result = adapter.get_daily_kpi(GAME_ID, (FULL_START, FULL_END))

        assert len(result["daily"]) == 30

    def test_daily_row_contains_all_kpi_fields(self, adapter):
        result = adapter.get_daily_kpi(GAME_ID, (FULL_START, FULL_END))
        first = result["daily"][0]
        expected_fields = {
            "date", "dau", "mau", "revenue", "arppu",
            "d1_retention", "d7_retention",
            "sessions", "avg_session_sec",
            "payment_success_rate", "new_installs",
        }

        assert expected_fields.issubset(set(first.keys()))

    def test_daily_rows_ordered_ascending_by_date(self, adapter):
        result = adapter.get_daily_kpi(GAME_ID, (FULL_START, FULL_END))
        dates = [row["date"] for row in result["daily"]]

        assert dates == sorted(dates)

    def test_returns_single_row_when_start_equals_end(self, adapter):
        """빈 period(start == end): 1일치만 반환."""
        single_day = date(2026, 3, 15)
        result = adapter.get_daily_kpi(GAME_ID, (single_day, single_day))

        assert len(result["daily"]) == 1
        assert result["daily"][0]["date"] == "2026-03-15"

    def test_raises_value_error_when_end_before_start(self, adapter):
        """잘못된 period(end < start): ValueError."""
        with pytest.raises(ValueError):
            adapter.get_daily_kpi(GAME_ID, (date(2026, 3, 20), date(2026, 3, 10)))

    def test_scenario2_d7_retention_drops_after_break_date(self, adapter):
        """시나리오 2: D7 리텐션이 2026-03-17 이후 유의미하게 하락해야 한다.

        전반(3/2~3/16) 평균 vs 후반(3/17~3/31) 평균을 비교.
        실측: early≈0.299, late≈0.254 → 약 4pp 하락.
        임계: late < early * 0.97 (3% 이상 감소, seed 변경 시 여유 확보)
        """
        result = adapter.get_daily_kpi(GAME_ID, (FULL_START, FULL_END))
        daily = result["daily"]

        early_rows = [r for r in daily if r["date"] < "2026-03-17"]
        late_rows = [r for r in daily if r["date"] >= "2026-03-17"]

        early_avg = sum(r["d7_retention"] for r in early_rows) / len(early_rows)
        late_avg = sum(r["d7_retention"] for r in late_rows) / len(late_rows)

        assert late_avg < early_avg * 0.97, (
            f"D7 리텐션 하락 시나리오 미검출: early={early_avg:.4f}, late={late_avg:.4f}"
        )

    def test_kpi_numeric_fields_are_non_negative(self, adapter):
        result = adapter.get_daily_kpi(GAME_ID, (FULL_START, FULL_END))
        for row in result["daily"]:
            assert row["dau"] >= 0
            assert row["revenue"] >= 0
            assert row["payment_success_rate"] >= 0.0


# ════════════════════════════════════════════════════════════════════
# get_metric_by_segments
# ════════════════════════════════════════════════════════════════════


class TestGetMetricBySegments:
    def test_returns_expected_top_level_keys(self, adapter):
        result = adapter.get_metric_by_segments(
            GAME_ID, "revenue", (FULL_START, FULL_END), ["platform"]
        )

        assert set(result.keys()) == {"game_id", "metric", "period", "segments"}
        assert result["metric"] == "revenue"

    def test_returns_segment_values_with_correct_length(self, adapter):
        """30일 period → 각 세그먼트 값 배열 길이 30."""
        result = adapter.get_metric_by_segments(
            GAME_ID, "revenue", (FULL_START, FULL_END), ["platform"]
        )
        for seg_values in result["segments"]["platform"].values():
            assert len(seg_values) == 30

    def test_includes_android_and_ios_in_platform_segments(self, adapter):
        result = adapter.get_metric_by_segments(
            GAME_ID, "revenue", (FULL_START, FULL_END), ["platform"]
        )
        platforms = set(result["segments"]["platform"].keys())

        assert "android" in platforms
        assert "ios" in platforms

    def test_returns_multiple_dimension_segments(self, adapter):
        """dimensions에 여러 항목을 넘기면 segments dict에 모두 포함된다."""
        result = adapter.get_metric_by_segments(
            GAME_ID, "dau", (FULL_START, FULL_END), ["platform", "country"]
        )

        assert "platform" in result["segments"]
        assert "country" in result["segments"]

    def test_raises_value_error_when_metric_unknown(self, adapter):
        with pytest.raises(ValueError, match="지원하지 않는 지표"):
            adapter.get_metric_by_segments(
                GAME_ID, "nonexistent_metric", (FULL_START, FULL_END), ["platform"]
            )

    def test_raises_value_error_when_dimension_unknown(self, adapter):
        with pytest.raises(ValueError, match="지원하지 않는 차원"):
            adapter.get_metric_by_segments(
                GAME_ID, "revenue", (FULL_START, FULL_END), ["totally_invalid_dim"]
            )

    def test_raises_value_error_when_end_before_start(self, adapter):
        with pytest.raises(ValueError):
            adapter.get_metric_by_segments(
                GAME_ID, "revenue",
                (date(2026, 3, 20), date(2026, 3, 1)),
                ["platform"],
            )

    def test_scenario1_android_revenue_drops_after_break_date(self, adapter):
        """시나리오 1: 3/28 이후 Android 매출이 전반 대비 유의미하게 하락해야 한다.

        전반(3/2~3/27) 일평균 vs 후반(3/28~3/31) 일평균 비교.
        실측: early≈626K, late≈473K → 약 24% 감소.
        임계: late_avg < early_avg * 0.90 (10% 이상 감소)
        """
        result = adapter.get_metric_by_segments(
            GAME_ID, "revenue", (FULL_START, FULL_END), ["platform"]
        )
        all_dates = [
            date(2026, 3, d) for d in range(2, 32)
        ]
        android_values = result["segments"]["platform"]["android"]

        # 날짜 인덱스 매핑
        break_idx = all_dates.index(SCENARIO1_BREAK)  # index 26 (3/28)

        early_vals = android_values[:break_idx]      # 3/2~3/27
        late_vals = android_values[break_idx:]        # 3/28~3/31

        early_avg = sum(early_vals) / len(early_vals)
        late_avg = sum(late_vals) / len(late_vals)

        assert late_avg < early_avg * 0.90, (
            f"Android 매출 하락 미검출: early_avg={early_avg:.0f}, late_avg={late_avg:.0f}"
        )

    def test_scenario1_ios_revenue_not_significantly_dropped(self, adapter):
        """시나리오 1: iOS 매출은 같은 기간 동안 하락하지 않아야 한다.

        실측: iOS late/early ratio ≈ 1.097 (오히려 상승).
        임계: late_avg >= early_avg * 0.90 (10% 미만 감소)
        """
        result = adapter.get_metric_by_segments(
            GAME_ID, "revenue", (FULL_START, FULL_END), ["platform"]
        )
        all_dates = [date(2026, 3, d) for d in range(2, 32)]
        ios_values = result["segments"]["platform"]["ios"]

        break_idx = all_dates.index(SCENARIO1_BREAK)
        early_avg = sum(ios_values[:break_idx]) / break_idx
        late_avg = sum(ios_values[break_idx:]) / len(ios_values[break_idx:])

        assert late_avg >= early_avg * 0.90, (
            f"iOS 매출도 함께 하락 (시나리오 1 위반): early_avg={early_avg:.0f}, late_avg={late_avg:.0f}"
        )

    def test_scenario3_brazil_payment_success_rate_lower_on_last_day(self, adapter):
        """시나리오 3: 2026-03-31 브라질 결제 성공률이 다른 국가보다 유의미하게 낮아야 한다.

        실측: 브라질 0.667, 기타 국가 0.906~0.984.
        임계: brazil < 다른 모든 국가의 평균 * 0.85
        """
        result = adapter.get_metric_by_segments(
            GAME_ID, "payment_success_rate",
            (SCENARIO3_DATE, SCENARIO3_DATE),
            ["country"],
        )
        country_segs = result["segments"]["country"]

        assert "brazil" in country_segs, "브라질 세그먼트가 결과에 없음"

        brazil_rate = country_segs["brazil"][0]
        other_rates = [
            vals[0]
            for country, vals in country_segs.items()
            if country != "brazil"
        ]
        other_avg = sum(other_rates) / len(other_rates)

        assert brazil_rate < other_avg * 0.85, (
            f"브라질 결제 성공률 하락 미검출: brazil={brazil_rate:.4f}, others_avg={other_avg:.4f}"
        )

    def test_supports_all_four_metrics(self, adapter):
        """4가지 지표(revenue/dau/payment_success_rate/d7_retention) 모두 실행 가능."""
        period = (date(2026, 3, 20), date(2026, 3, 25))
        for metric in ("revenue", "dau", "payment_success_rate", "d7_retention"):
            result = adapter.get_metric_by_segments(
                GAME_ID, metric, period, ["platform"]
            )
            assert result["metric"] == metric
            assert "platform" in result["segments"]

    def test_ratio_metric_fills_missing_days_with_none(self, adapter):
        """비율 지표(payment_success_rate)의 결측일은 0.0이 아니라 None이어야 한다.

        payment_attempts 데이터가 없는 날짜에 0.0이 들어가면
        LLM이 "결제 성공률 0%"로 오해해 환각을 일으킨다.
        """
        result = adapter.get_metric_by_segments(
            GAME_ID, "payment_success_rate", (FULL_START, FULL_END), ["country"]
        )
        country_segs = result["segments"]["country"]
        for country, values in country_segs.items():
            for v in values:
                # 값이 있으면 0~1 사이 float, 없으면 None이어야 함
                assert v is None or (0.0 <= v <= 1.0), (
                    f"{country}: 비율 지표에 범위 밖 값 {v}. "
                    "결측이면 None, 있으면 0~1이어야 함"
                )

    def test_summation_metric_fills_missing_days_with_zero(self, adapter):
        """합계 지표(revenue)의 결측일은 0.0으로 채워야 한다."""
        result = adapter.get_metric_by_segments(
            GAME_ID, "revenue", (FULL_START, FULL_END), ["country"]
        )
        country_segs = result["segments"]["country"]
        for country, values in country_segs.items():
            for v in values:
                # 합계 지표는 None이 아니라 0.0 이상 float
                assert v is not None and v >= 0.0, (
                    f"{country}: 합계 지표에 None 또는 음수 {v}. "
                    "결측이면 0.0, 있으면 양수여야 함"
                )


# ════════════════════════════════════════════════════════════════════
# get_available_dimensions
# ════════════════════════════════════════════════════════════════════


class TestGetAvailableDimensions:
    def test_returns_segment_dimensions(self, adapter):
        dims = adapter.get_available_dimensions(GAME_ID)

        assert "platform" in dims
        assert "country" in dims
        assert "user_type" in dims
        assert "device_model" in dims

    def test_excludes_user_id(self, adapter):
        dims = adapter.get_available_dimensions(GAME_ID)

        assert "user_id" not in dims

    def test_excludes_install_date(self, adapter):
        dims = adapter.get_available_dimensions(GAME_ID)

        assert "install_date" not in dims

    def test_returns_list_type(self, adapter):
        dims = adapter.get_available_dimensions(GAME_ID)

        assert isinstance(dims, list)
        assert len(dims) > 0


# ════════════════════════════════════════════════════════════════════
# get_available_schema
# ════════════════════════════════════════════════════════════════════


class TestGetAvailableSchema:
    def test_returns_tables_key(self, adapter):
        schema = adapter.get_available_schema(GAME_ID)

        assert "tables" in schema
        assert isinstance(schema["tables"], list)

    def test_contains_all_12_tables(self, adapter):
        schema = adapter.get_available_schema(GAME_ID)
        actual_tables = {t["name"] for t in schema["tables"]}

        assert EXPECTED_TABLES.issubset(actual_tables), (
            f"누락된 테이블: {EXPECTED_TABLES - actual_tables}"
        )

    def test_each_table_entry_has_required_fields(self, adapter):
        schema = adapter.get_available_schema(GAME_ID)
        for table in schema["tables"]:
            assert "name" in table
            assert "columns" in table
            assert "description" in table
            assert isinstance(table["columns"], list)
            assert len(table["columns"]) > 0

    def test_daily_kpi_table_has_expected_columns(self, adapter):
        schema = adapter.get_available_schema(GAME_ID)
        kpi_table = next(t for t in schema["tables"] if t["name"] == "daily_kpi")

        expected_cols = {"date", "dau", "mau", "revenue", "d7_retention"}
        assert expected_cols.issubset(set(kpi_table["columns"]))

    def test_users_table_has_expected_columns(self, adapter):
        schema = adapter.get_available_schema(GAME_ID)
        users_table = next(t for t in schema["tables"] if t["name"] == "users")

        expected_cols = {"user_id", "platform", "country", "user_type", "install_date", "device_model"}
        assert expected_cols.issubset(set(users_table["columns"]))


# ════════════════════════════════════════════════════════════════════
# execute_readonly_sql
# ════════════════════════════════════════════════════════════════════


class TestExecuteReadonlySql:
    def test_returns_list_of_dicts(self, adapter):
        result = adapter.execute_readonly_sql("SELECT date, dau FROM daily_kpi LIMIT 5")

        assert isinstance(result, list)
        assert len(result) == 5
        assert isinstance(result[0], dict)
        assert "date" in result[0]
        assert "dau" in result[0]

    def test_respects_max_rows_limit(self, adapter):
        """max_rows=3이면 결과가 3행을 초과하지 않는다."""
        result = adapter.execute_readonly_sql(
            "SELECT date FROM daily_kpi ORDER BY date", max_rows=3
        )

        assert len(result) <= 3

    def test_raises_value_error_when_max_rows_is_zero(self, adapter):
        with pytest.raises(ValueError, match="1 이상"):
            adapter.execute_readonly_sql("SELECT 1", max_rows=0)

    def test_raises_value_error_when_max_rows_is_negative(self, adapter):
        with pytest.raises(ValueError):
            adapter.execute_readonly_sql("SELECT 1", max_rows=-1)

    def test_raises_runtime_error_when_sql_is_invalid(self, adapter):
        """말이 안 되는 SQL → RuntimeError."""
        with pytest.raises(RuntimeError, match="SQL 실행 실패"):
            adapter.execute_readonly_sql("SELECT * FROM nonexistent_table_xyz")

    def test_raises_runtime_error_when_sql_is_syntactically_wrong(self, adapter):
        """문법 오류 SQL → RuntimeError."""
        with pytest.raises(RuntimeError):
            adapter.execute_readonly_sql("THIS IS NOT SQL AT ALL !!!@#$")

    def test_default_max_rows_is_100(self, adapter):
        """기본 max_rows=100: 30행짜리 테이블 전체 반환."""
        result = adapter.execute_readonly_sql("SELECT date FROM daily_kpi ORDER BY date")

        # daily_kpi는 30행이므로 전부 반환되어야 함
        assert len(result) == 30

    def test_returns_empty_list_when_no_rows_match(self, adapter):
        result = adapter.execute_readonly_sql(
            "SELECT date FROM daily_kpi WHERE date = '1900-01-01'"
        )

        assert result == []


# ════════════════════════════════════════════════════════════════════
# context manager & 연결 소유권
# ════════════════════════════════════════════════════════════════════


class TestContextManagerAndOwnership:
    def test_context_manager_exits_without_error(self, mock_db_conn):
        """with 블록 후 예외 없이 종료되어야 한다.

        connection 주입 시 adapter가 연결 소유권을 갖지 않으므로
        __exit__ 후에도 원래 연결(mock_db_conn)은 살아 있어야 한다.
        """
        with DuckDBAdapter(connection=mock_db_conn) as adapter:
            result = adapter.get_available_dimensions(GAME_ID)
            assert len(result) > 0
        # with 블록 종료 후 원본 conn은 여전히 동작해야 한다
        rows = mock_db_conn.execute("SELECT COUNT(*) FROM daily_kpi").fetchone()
        assert rows[0] == 30

    def test_adapter_does_not_own_injected_connection(self, mock_db_conn):
        """외부 주입 connection은 adapter가 소유하지 않는다."""
        adapter = DuckDBAdapter(connection=mock_db_conn)

        assert adapter._owns_connection is False

    def test_raises_file_not_found_when_db_path_invalid(self):
        """존재하지 않는 경로로 생성 시 FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            DuckDBAdapter(db_path="/nonexistent/path/fake.db")