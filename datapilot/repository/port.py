"""DataRepository — 데이터 접근 Port(인터페이스) 정의.

헥사고날 아키텍처(Port/Adapter)의 Port에 해당한다. 에이전트는 구체적인
저장소 구현(DuckDB/BigQuery)을 모른 채 이 추상 클래스만 의존하고,
구현체는 런타임에 주입(inject)된다.

Java 비유:
    public interface DataRepository {
        Map<String, Object> getDailyKpi(String entityId, LocalDate from, LocalDate to);
        ...
    }

Python의 ``ABC`` + ``@abstractmethod`` 조합이 Java ``interface``와 동일한 역할을
한다. 추상 메서드가 하나라도 남은 클래스는 인스턴스화하면 TypeError가 발생한다.

domain-extension 묶음에서 ``GameDataRepository`` → ``DataRepository`` rename +
``game_id`` 인자 → ``entity_id`` rename (도메인 무관 의미 일반화).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

# ──────────────────────────────────────────────────────────────────
# 세그먼트 분해 지원 metric 계약
# ──────────────────────────────────────────────────────────────────


def get_supported_segment_metrics(domain: str) -> frozenset[str]:
    """도메인별 segmentable KPI 셋 조회.

    구 모듈 상수 ``SUPPORTED_SEGMENT_METRICS`` 의 도메인 일반화 버전.
    도메인별 정의는 ``datapilot.domain.DOMAINS[domain].supported_segment_metrics``
    에 모여 있다.

    Args:
        domain: 도메인 식별자 (예: ``"game"`` / ``"ecommerce"``).

    Returns:
        해당 도메인이 ② SegmentationAnalyzer 로 분해할 수 있는 KPI 코드 집합.

    Raises:
        ValueError: 알 수 없는 ``domain``.
    """
    # 순환 import 회피 — 함수 안에서 lazy import
    from datapilot.domain import DOMAINS

    if domain not in DOMAINS:
        raise ValueError(
            f"unsupported domain: {domain!r}. supported: {sorted(DOMAINS.keys())}"
        )
    return DOMAINS[domain].supported_segment_metrics


class DataRepository(ABC):
    """KPI 데이터 조회 Port (도메인 무관).

    파이프라인의 에이전트들은 이 인터페이스에만 의존한다. 데모 환경에서는
    ``DuckDBAdapter`` 가, 운영 환경에서는 ``BigQueryAdapter`` 가 주입된다.

    반환 형식은 모두 JSON 직렬화 가능한 dict/list 로 통일한다
    (LLM 프롬프트에 ``json.dumps(...)`` 로 바로 삽입되기 때문).

    ``entity_id`` 인자는 도메인의 분석 대상 식별자다. 게임 도메인에서는 게임 ID
    (예: ``"pizza_ready"``), 이커머스 도메인에서는 스토어/사이트 ID
    (예: ``"ecommerce_demo"``) 가 된다.
    """

    # ────────────────────────────────────────────────────────────────
    # ① Bottleneck Detector — 일별 KPI 시계열
    # ────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_daily_kpi(
        self,
        entity_id: str,
        period: tuple[date, date],
    ) -> dict[str, Any]:
        """기간 내 일별 KPI 시계열을 조회한다.

        Args:
            entity_id: 도메인의 분석 대상 식별자 (예: ``"pizza_ready"`` / ``"ecommerce_demo"``).
            period: (시작일, 종료일) inclusive.

        Returns:
            도메인별 KPI 컬럼이 포함된 dict. 게임 예::

                {
                    "entity_id": str,
                    "period": {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"},
                    "daily": [
                        {
                            "date": "YYYY-MM-DD",
                            "dau": int, "mau": int, "revenue": float,
                            "arppu": float, "d1_retention": float, "d7_retention": float,
                            "sessions": int, "avg_session_sec": int,
                            "payment_success_rate": float, "new_installs": int,
                        },
                        ...
                    ],
                }
        """
        ...

    # ────────────────────────────────────────────────────────────────
    # ② Segmentation Analyzer — 세그먼트 차원 탐지 + 지표 분해
    # ────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_available_dimensions(self, entity_id: str) -> list[str]:
        """세그먼트 분석에 사용 가능한 차원 컬럼을 자동 탐지한다.

        도메인별 세그먼트 테이블 (게임=``users``, 이커머스=``customers``) 에서
        세그먼트 성격의 컬럼만 추출한다 (PK·날짜 제외).
        Mock DB 에 컬럼을 추가하기만 하면 에이전트가 자동으로 인식한다.

        Returns:
            예: ``["platform", "country", "user_type", "device_model"]``
        """
        ...

    @abstractmethod
    def get_metric_by_segments(
        self,
        entity_id: str,
        metric: str,
        period: tuple[date, date],
        dimensions: list[str],
    ) -> dict[str, Any]:
        """이상 지표를 세그먼트 차원별로 분해한 시계열을 조회한다.

        지원 metric 은 ``get_supported_segment_metrics(domain)`` 참조.
        미지원 metric 이 들어오면 ValueError.

        Args:
            entity_id: 도메인의 분석 대상 식별자.
            metric: 이상 지표명 — 도메인의 segmentable KPI 에 포함된 값만 허용.
            period: (시작일, 종료일) inclusive.
            dimensions: 분해할 차원 목록.

        Returns:
            ``{"entity_id": str, "metric": str, "period": ..., "segments": {...}}``
        """
        ...

    # ────────────────────────────────────────────────────────────────
    # ③ Hypothesis Generator — 가용 테이블 스키마
    # ────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_available_schema(self, entity_id: str) -> dict[str, Any]:
        """가용 테이블 목록과 컬럼 스키마를 조회한다.

        Hypothesis Generator 가 프롬프트에 주입해, LLM 이 가용 테이블 밖의
        가설은 ``required_data`` (자연어) 로만 기재하게 유도한다.

        Returns:
            ``{"tables": [{"name": str, "columns": list[str], "description": str}, ...]}``
        """
        ...

    # ────────────────────────────────────────────────────────────────
    # ④ Data Validator — Tool Use용 읽기 전용 SQL 실행
    # ────────────────────────────────────────────────────────────────

    @abstractmethod
    def execute_readonly_sql(
        self,
        query: str,
        max_rows: int = 100,
    ) -> list[dict[str, Any]]:
        """읽기 전용 SQL을 실행해 결과를 dict 리스트로 반환한다.

        Data Validator 에이전트의 Tool Use(bind_tools)에서 사용한다.

        보안 주의:
            이 메서드는 "연결 수준의 안전 조건"만 책임진다.
            - read-only 모드 연결 (쓰기/DDL은 DB 레벨에서 거부)
            - 결과 행 수 ``max_rows`` 제한 (Python fetchmany 레벨)

            "쿼리 내용 수준의 검증"(SELECT-only 정규식, 위험 키워드 블랙리스트,
            테이블 화이트리스트)은 Data Validator의 Tool 래퍼가 담당한다.

            비용 주의:
            ``max_rows`` 는 반환 행 수만 제한하며 **스캔 비용은 제한하지 않는다**.
            DuckDB(로컬)에서는 무해하지만, BigQueryAdapter 구현 시에는
            ``QueryJobConfig(maximum_bytes_billed=...)`` 등 별도 비용 상한 설정이 필요하다.

        Args:
            query: 실행할 SQL.
            max_rows: 반환 최대 행 수 (기본 100).

        Returns:
            ``[{컬럼명: 값, ...}, ...]``

        Raises:
            RuntimeError: 쿼리 실행 중 DB 오류 발생 시.
        """
        ...