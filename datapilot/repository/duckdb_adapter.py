"""DuckDB 기반 DataRepository 구현.

데모/개발 환경에서 사용되는 Adapter. `data/datapilot_mock.db` 파일을
read-only 모드로 열어 에이전트의 조회 요청을 처리한다.

Java 비유:
    @Repository
    public class DuckDBDataRepository implements DataRepository { ... }

생성자에 `connection`을 주입할 수 있어 테스트에서는 in-memory DuckDB를
그대로 넘길 수 있다 (생성자 주입 = Constructor Injection).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

from datapilot.domain import DOMAINS
from datapilot.repository.port import DataRepository

# ──────────────────────────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────────────────────────

#: 프로젝트 루트 (`data/mock/<domain>.db` 경로 해석에 사용)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]

#: 도메인별 세그먼트 차원 테이블 (② SegmentationAnalyzer 의 GROUP BY 대상)
_SEGMENT_TABLE_BY_DOMAIN: dict[str, str] = {
    "game": "users",
    "ecommerce": "customers",
}

#: 도메인별 세그먼트 차원에서 제외할 메타 컬럼 (PK·날짜 등)
_SEGMENT_META_COLUMNS_BY_DOMAIN: dict[str, frozenset[str]] = {
    "game": frozenset({"user_id", "install_date"}),
    "ecommerce": frozenset({"customer_id"}),
}

#: 비율 지표 — 결측 시 0.0이 아닌 None(JSON null)으로 채워야 함
#: 이유: "결제 시도 0건"과 "결제 성공률 0%"는 완전히 다른 의미
_RATIO_METRICS = frozenset({"payment_success_rate", "d7_retention", "conversion"})

# 테이블 설명은 ``DOMAINS[domain].table_descriptions`` 으로 옮김 (도메인 정의 SoT).


# ──────────────────────────────────────────────────────────────────
# Adapter
# ──────────────────────────────────────────────────────────────────


class DuckDBAdapter(DataRepository):
    """DuckDB 파일을 read-only로 연 DataRepository 구현.

    도메인별 mock DB 파일을 ``domain`` 인자로 분기한다 (게임 = ``data/mock/game.db`` /
    이커머스 = ``data/mock/ecommerce.db``). 단일 mock 단위라 ``entity_id`` 파라미터는
    인터페이스 호환성을 위해 받기만 하고 필터링에는 사용하지 않는다.
    BigQueryAdapter 에서는 ``WHERE entity_id = @entity_id`` 로 사용될 예정.
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        *,
        domain: str = "game",
        connection: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        """Adapter 초기화.

        Args:
            db_path: DuckDB 파일 경로 (호환용 — 명시 시 ``domain`` 디폴트 경로 무시).
                None 이면 ``DOMAINS[domain].db_path`` 사용.
            domain: 도메인 식별자 (``"game"`` / ``"ecommerce"``). 기본 ``"game"``
                (백워드 호환). ``DOMAINS`` 에 등록된 값만 허용.
            connection: 이미 연 연결을 주입할 때 사용 (테스트용).
                주입 시 ``db_path`` 와 ``domain`` 디폴트 경로는 무시되고,
                ``close()`` 도 소유하지 않는다.

        Raises:
            ValueError: ``db_path`` 와 ``connection`` 모두 없는데 ``domain`` 이
                ``DOMAINS`` 에 없을 때.
            FileNotFoundError: 해석된 DB 파일이 존재하지 않을 때.
        """
        self._domain = domain
        if connection is not None:
            self._conn = connection
            self._owns_connection = False
            return

        if db_path is not None:
            path = Path(db_path)
        else:
            if domain not in DOMAINS:
                raise ValueError(
                    f"unsupported domain: {domain!r}. "
                    f"supported: {sorted(DOMAINS.keys())}"
                )
            path = _PROJECT_ROOT / DOMAINS[domain].db_path

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
        entity_id: str,
        period: tuple[date, date],
    ) -> dict[str, Any]:
        start, end = period
        _validate_period(start, end)

        # 도메인별 daily_kpi 컬럼 구성이 다르므로 분기.
        # 게임 = 11 KPI / 이커머스 = 5 KPI.
        if self._domain == "ecommerce":
            rows = self._conn.execute(
                """
                SELECT
                    date, gmv, orders, conversion,
                    visitors, payment_success_rate
                FROM daily_kpi
                WHERE date BETWEEN ? AND ?
                ORDER BY date
                """,
                [start, end],
            ).fetchall()
            daily = [
                {
                    "date": row[0].isoformat(),
                    "gmv": float(row[1]),
                    "orders": int(row[2]),
                    "conversion": float(row[3]),
                    "visitors": int(row[4]),
                    "payment_success_rate": float(row[5]),
                }
                for row in rows
            ]
        else:
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
            "entity_id": entity_id,
            "period": {"from": start.isoformat(), "to": end.isoformat()},
            "daily": daily,
        }

    # ──────────────────────────────────────────────────────────
    # ② 세그먼트 분해
    # ──────────────────────────────────────────────────────────

    def get_available_dimensions(self, entity_id: str) -> list[str]:
        # 도메인별 세그먼트 테이블 (게임=users / 이커머스=customers).
        segment_table = _SEGMENT_TABLE_BY_DOMAIN.get(self._domain, "users")
        meta_columns = _SEGMENT_META_COLUMNS_BY_DOMAIN.get(
            self._domain, frozenset()
        )
        rows = self._conn.execute(
            f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{segment_table}'
            ORDER BY ordinal_position
            """
        ).fetchall()
        return [row[0] for row in rows if row[0] not in meta_columns]

    def get_metric_by_segments(
        self,
        entity_id: str,
        metric: str,
        period: tuple[date, date],
        dimensions: list[str],
    ) -> dict[str, Any]:
        start, end = period
        all_dates = _date_range_inclusive(start, end)

        # 차원 이름 화이트리스트 검증 (SQL identifier 인젝션 방지)
        allowed_dims = set(self.get_available_dimensions(entity_id))
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
            "entity_id": entity_id,
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
        """지표 × 단일 차원별 (date, segment_value, value) 행을 반환한다.

        도메인별로 raw 테이블 / JOIN 키가 달라 self._domain 으로 분기.
        """
        # `dimension`은 호출 직전에 화이트리스트 검증이 끝난 상태라고 가정

        # ── 이커머스 도메인 ───────────────────────────────────
        if self._domain == "ecommerce":
            if metric == "gmv":
                sql = f"""
                    SELECT
                        CAST(o.paid_at AS DATE) AS date,
                        c.{dimension}           AS segment,
                        COALESCE(SUM(o.amount), 0) AS value
                    FROM orders o
                    JOIN customers c ON o.customer_id = c.customer_id
                    WHERE CAST(o.paid_at AS DATE) BETWEEN ? AND ?
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                """
                rows = self._conn.execute(sql, [start, end]).fetchall()
            elif metric == "orders":
                sql = f"""
                    SELECT
                        CAST(o.paid_at AS DATE) AS date,
                        c.{dimension}           AS segment,
                        COUNT(*)                AS value
                    FROM orders o
                    JOIN customers c ON o.customer_id = c.customer_id
                    WHERE CAST(o.paid_at AS DATE) BETWEEN ? AND ?
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                """
                rows = self._conn.execute(sql, [start, end]).fetchall()
            else:
                raise ValueError(
                    f"이커머스 지원하지 않는 지표: {metric!r}. "
                    "현재 'gmv' / 'orders' 만 지원."
                )
            return [(row[0], str(row[1]), float(row[2] or 0.0)) for row in rows]

        # ── 게임 도메인 (기본) ────────────────────────────────
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

    def get_available_schema(self, entity_id: str) -> dict[str, Any]:
        # Mock DB는 단일 게임 전용. BigQueryAdapter에서는 dataset 기준 필터 추가.
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

        # 도메인의 테이블 설명 dict (DOMAINS SoT)
        descriptions = (
            DOMAINS[self._domain].table_descriptions
            if self._domain in DOMAINS
            else {}
        )

        tables = [
            {
                "name": name,
                "columns": columns,
                "description": descriptions.get(name, ""),
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