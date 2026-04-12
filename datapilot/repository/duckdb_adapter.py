"""DuckDB 기반 GameDataRepository 구현.

데모/개발 환경에서 사용되는 Adapter. `data/datapilot_mock.db` 파일을
read-only 모드로 열어 에이전트의 조회 요청을 처리한다.

Java 비유:
    @Repository
    public class DuckDBGameDataRepository implements GameDataRepository { ... }

생성자에 `connection`을 주입할 수 있어 테스트에서는 in-memory DuckDB를
그대로 넘길 수 있다 (생성자 주입 = Constructor Injection).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

from datapilot.repository.port import GameDataRepository

# ──────────────────────────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────────────────────────

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "data" / "datapilot_mock.db"

#: users 테이블에서 세그먼트 차원으로 노출하지 않을 컬럼
_USER_META_COLUMNS = frozenset({"user_id", "install_date"})

#: 비율 지표 — 결측 시 0.0이 아닌 None(JSON null)으로 채워야 함
#: 이유: "결제 시도 0건"과 "결제 성공률 0%"는 완전히 다른 의미
_RATIO_METRICS = frozenset({"payment_success_rate", "d7_retention"})

#: 테이블별 한글 설명. information_schema에는 COMMENT가 없어 수동 매핑.
_TABLE_DESCRIPTIONS: dict[str, str] = {
    "daily_kpi": "일별 KPI 집계 (Bottleneck Detector 입력)",
    "users": "유저 마스터 (세그먼트 차원 4종 보유)",
    "products": "상품 마스터 (price_tier, category)",
    "payments": "결제 이벤트 (status: success/failed/refunded)",
    "shop_impressions": "상점 진열 노출 로그 (slot_order)",
    "releases": "빌드 배포 이력 (build_notes)",
    "events": "인게임 이벤트 로그 (login/stage_clear/purchase/event_participate)",
    "sessions": "세션 로그",
    "content_releases": "컨텐츠/이벤트 스케줄 (season_event 포함)",
    "gateways": "PG사 정보",
    "payment_attempts": "결제 시도 로그 (gateway별 success/failed)",
    "payment_errors": "결제 에러 상세",
}


# ──────────────────────────────────────────────────────────────────
# Adapter
# ──────────────────────────────────────────────────────────────────


class DuckDBAdapter(GameDataRepository):
    """DuckDB 파일을 read-only로 연 GameDataRepository 구현.

    현재 Mock DB는 단일 게임(pizza_ready) 전용이므로 `game_id` 파라미터는
    인터페이스 호환성을 위해 받기만 하고 필터링에는 사용하지 않는다.
    BigQueryAdapter에서는 `WHERE game_id = @game_id`로 사용될 예정.
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        *,
        connection: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        """Adapter 초기화.

        Args:
            db_path: DuckDB 파일 경로. None이면 `data/datapilot_mock.db` 사용.
            connection: 이미 연 연결을 주입할 때 사용 (테스트용).
                주입 시 `db_path`는 무시되고, `close()`도 소유하지 않는다.
        """
        if connection is not None:
            self._conn = connection
            self._owns_connection = False
        else:
            path = Path(db_path) if db_path else DEFAULT_DB_PATH
            if not path.exists():
                raise FileNotFoundError(
                    f"DuckDB 파일을 찾을 수 없습니다: {path}. "
                    "`uv run python scripts/seed_mock_data.py` 로 먼저 생성하세요."
                )
            self._conn = duckdb.connect(str(path), read_only=True)
            self._owns_connection = True

    def close(self) -> None:
        """연결을 닫는다 (직접 생성한 경우에만)."""
        if self._owns_connection:
            self._conn.close()

    def __enter__(self) -> "DuckDBAdapter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        self.close()

    # ──────────────────────────────────────────────────────────
    # ① 일별 KPI 시계열
    # ──────────────────────────────────────────────────────────

    def get_daily_kpi(
        self,
        game_id: str,
        period: tuple[date, date],
    ) -> dict[str, Any]:
        start, end = period
        _validate_period(start, end)
        rows = self._conn.execute(
            """
            SELECT
                date, dau, mau, revenue, arppu,
                d1_retention, d7_retention,
                sessions, avg_session_sec,
                payment_success_rate, new_installs
            FROM daily_kpi
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """,
            [start, end],
        ).fetchall()

        daily = [
            {
                "date": row[0].isoformat(),
                "dau": int(row[1]),
                "mau": int(row[2]),
                "revenue": float(row[3]),
                "arppu": float(row[4]),
                "d1_retention": float(row[5]),
                "d7_retention": float(row[6]),
                "sessions": int(row[7]),
                "avg_session_sec": int(row[8]),
                "payment_success_rate": float(row[9]),
                "new_installs": int(row[10]),
            }
            for row in rows
        ]

        return {
            "game_id": game_id,
            "period": {"from": start.isoformat(), "to": end.isoformat()},
            "daily": daily,
        }

    # ──────────────────────────────────────────────────────────
    # ② 세그먼트 분해
    # ──────────────────────────────────────────────────────────

    def get_available_dimensions(self, game_id: str) -> list[str]:
        rows = self._conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'users'
            ORDER BY ordinal_position
            """
        ).fetchall()
        return [row[0] for row in rows if row[0] not in _USER_META_COLUMNS]

    def get_metric_by_segments(
        self,
        game_id: str,
        metric: str,
        period: tuple[date, date],
        dimensions: list[str],
    ) -> dict[str, Any]:
        start, end = period
        all_dates = _date_range_inclusive(start, end)

        # 차원 이름 화이트리스트 검증 (SQL identifier 인젝션 방지)
        allowed_dims = set(self.get_available_dimensions(game_id))
        unknown = [d for d in dimensions if d not in allowed_dims]
        if unknown:
            raise ValueError(
                f"지원하지 않는 차원: {unknown}. 사용 가능: {sorted(allowed_dims)}"
            )

        fill = None if metric in _RATIO_METRICS else 0.0

        segments: dict[str, dict[str, list[float | None]]] = {}
        for dim in dimensions:
            rows = self._run_segmented_metric(metric, dim, start, end)
            segments[dim] = _pivot_to_timeseries(rows, all_dates, fill_value=fill)

        return {
            "game_id": game_id,
            "metric": metric,
            "period": {"from": start.isoformat(), "to": end.isoformat()},
            "segments": segments,
        }

    def _run_segmented_metric(
        self,
        metric: str,
        dimension: str,
        start: date,
        end: date,
    ) -> list[tuple[date, str, float]]:
        """지표 × 단일 차원별 (date, segment_value, value) 행을 반환한다."""
        # `dimension`은 호출 직전에 화이트리스트 검증이 끝난 상태라고 가정
        if metric == "revenue":
            sql = f"""
                SELECT
                    CAST(p.timestamp AS DATE) AS date,
                    u.{dimension}              AS segment,
                    COALESCE(SUM(p.amount), 0) AS value
                FROM payments p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.status = 'success'
                  AND CAST(p.timestamp AS DATE) BETWEEN ? AND ?
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
            rows = self._conn.execute(sql, [start, end]).fetchall()

        elif metric == "dau":
            sql = f"""
                SELECT
                    CAST(s.session_start AS DATE) AS date,
                    u.{dimension}                 AS segment,
                    COUNT(DISTINCT s.user_id)     AS value
                FROM sessions s
                JOIN users u ON s.user_id = u.user_id
                WHERE CAST(s.session_start AS DATE) BETWEEN ? AND ?
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
            rows = self._conn.execute(sql, [start, end]).fetchall()

        elif metric == "payment_success_rate":
            sql = f"""
                SELECT
                    CAST(pa.attempt_time AS DATE) AS date,
                    u.{dimension}                 AS segment,
                    SUM(CASE WHEN pa.status = 'success' THEN 1 ELSE 0 END) * 1.0
                        / NULLIF(COUNT(*), 0)     AS value
                FROM payment_attempts pa
                JOIN users u ON pa.user_id = u.user_id
                WHERE CAST(pa.attempt_time AS DATE) BETWEEN ? AND ?
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
            rows = self._conn.execute(sql, [start, end]).fetchall()

        elif metric == "d7_retention":
            # 날짜 d 의 D7 리텐션 = "설치일이 d-7인 유저 중 d에 세션 있는 비율"
            sql = f"""
                WITH cohorts AS (
                    SELECT
                        CAST(u.install_date AS DATE) + INTERVAL 7 DAY AS return_date,
                        u.user_id,
                        u.{dimension} AS segment
                    FROM users u
                    WHERE CAST(u.install_date AS DATE) + INTERVAL 7 DAY BETWEEN ? AND ?
                ),
                returned AS (
                    SELECT DISTINCT user_id, CAST(session_start AS DATE) AS date
                    FROM sessions
                )
                SELECT
                    CAST(c.return_date AS DATE) AS date,
                    c.segment                   AS segment,
                    COUNT(DISTINCT CASE WHEN r.user_id IS NOT NULL THEN c.user_id END) * 1.0
                        / NULLIF(COUNT(DISTINCT c.user_id), 0) AS value
                FROM cohorts c
                LEFT JOIN returned r
                    ON r.user_id = c.user_id
                   AND r.date    = CAST(c.return_date AS DATE)
                GROUP BY 1, 2
                ORDER BY 1, 2
            """
            rows = self._conn.execute(sql, [start, end]).fetchall()

        else:
            raise ValueError(
                f"지원하지 않는 지표: {metric!r}. "
                "현재 'revenue' / 'dau' / 'payment_success_rate' / 'd7_retention' 만 지원."
            )

        return [(row[0], str(row[1]), float(row[2] or 0.0)) for row in rows]

    # ──────────────────────────────────────────────────────────
    # ③ 가용 스키마
    # ──────────────────────────────────────────────────────────

    def get_available_schema(self, game_id: str) -> dict[str, Any]:
        rows = self._conn.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'main'
            ORDER BY table_name, ordinal_position
            """
        ).fetchall()

        # table_name → [column_name, ...]
        grouped: dict[str, list[str]] = {}
        for table_name, column_name in rows:
            grouped.setdefault(table_name, []).append(column_name)

        tables = [
            {
                "name": name,
                "columns": columns,
                "description": _TABLE_DESCRIPTIONS.get(name, ""),
            }
            for name, columns in sorted(grouped.items())
        ]
        return {"tables": tables}

    # ──────────────────────────────────────────────────────────
    # ④ 읽기 전용 SQL (Data Validator Tool Use용)
    # ──────────────────────────────────────────────────────────

    def execute_readonly_sql(
        self,
        query: str,
        max_rows: int = 100,
    ) -> list[dict[str, Any]]:
        if max_rows <= 0:
            raise ValueError("max_rows는 1 이상이어야 합니다")

        try:
            cursor = self._conn.execute(query)
        except duckdb.Error as e:
            raise RuntimeError(f"SQL 실행 실패: {e}") from e

        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchmany(max_rows)
        return [dict(zip(columns, row)) for row in rows]


# ──────────────────────────────────────────────────────────────────
# 내부 유틸
# ──────────────────────────────────────────────────────────────────


def _validate_period(start: date, end: date) -> None:
    """period(시작일, 종료일) 유효성 검증. end < start면 ValueError."""
    if end < start:
        raise ValueError(f"period end({end}) < start({start})")


def _date_range_inclusive(start: date, end: date) -> list[date]:
    """[start, end] 양끝 포함 날짜 리스트."""
    _validate_period(start, end)
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def _pivot_to_timeseries(
    rows: list[tuple[date, str, float]],
    all_dates: list[date],
    fill_value: float | None = 0.0,
) -> dict[str, list[float | None]]:
    """(date, segment, value) 롱포맷 → {segment: [v_day0, v_day1, ...]} 와이드포맷.

    쿼리 결과에 없는 날짜는 fill_value로 채워 배열 길이를 period 크기에 맞춘다.
    - 합계 지표(revenue, dau): fill_value=0.0 — "그날 0원/0명"이 의미상 맞음
    - 비율 지표(payment_success_rate, d7_retention): fill_value=None — "데이터 없음"
      (0.0으로 채우면 LLM이 "성공률 0%"로 오해해 환각 유발)
    """
    # segment -> date -> value
    nested: dict[str, dict[date, float]] = {}
    for d, seg, v in rows:
        nested.setdefault(seg, {})[d] = v

    result: dict[str, list[float | None]] = {}
    for segment, date_map in nested.items():
        result[segment] = [date_map.get(d, fill_value) for d in all_dates]
    return result