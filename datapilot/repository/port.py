"""GameDataRepository — 데이터 접근 Port(인터페이스) 정의.

헥사고날 아키텍처(Port/Adapter)의 Port에 해당한다. 에이전트는 구체적인
저장소 구현(DuckDB/BigQuery)을 모른 채 이 추상 클래스만 의존하고,
구현체는 런타임에 주입(inject)된다.

Java 비유:
    public interface GameDataRepository {
        Map<String, Object> getDailyKpi(String gameId, LocalDate from, LocalDate to);
        ...
    }

Python의 `ABC` + `@abstractmethod` 조합이 Java `interface`와 동일한 역할을 한다.
추상 메서드가 하나라도 남은 클래스는 인스턴스화하면 TypeError가 발생한다.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any


class GameDataRepository(ABC):
    """게임 KPI 데이터 조회 Port.

    파이프라인의 에이전트들은 이 인터페이스에만 의존한다. 데모 환경에서는
    `DuckDBAdapter`가, 운영 환경에서는 `BigQueryAdapter`가 주입된다.

    반환 형식은 모두 JSON 직렬화 가능한 dict/list로 통일한다.
    (LLM 프롬프트에 `json.dumps(...)`로 바로 삽입되기 때문)
    """

    # ────────────────────────────────────────────────────────────────
    # ① Bottleneck Detector — 일별 KPI 시계열
    # ────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_daily_kpi(
        self,
        game_id: str,
        period: tuple[date, date],
    ) -> dict[str, Any]:
        """기간 내 일별 KPI 시계열을 조회한다.

        Args:
            game_id: 게임 식별자 (예: "pizza_ready")
            period: (시작일, 종료일) inclusive

        Returns:
            {
                "game_id": str,
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
                ]
            }
        """
        ...

    # ────────────────────────────────────────────────────────────────
    # ② Segmentation Analyzer — 지표 × 세그먼트 분해
    # ────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_metric_by_segments(
        self,
        game_id: str,
        metric: str,
        period: tuple[date, date],
        dimensions: list[str],
    ) -> dict[str, Any]:
        """이상 지표를 세그먼트 차원별로 분해한 시계열을 조회한다.

        Args:
            game_id: 게임 식별자
            metric: 이상 지표명 (예: "revenue", "d7_retention", "payment_success_rate", "dau")
            period: (시작일, 종료일) inclusive
            dimensions: 분해할 차원 목록 (예: ["platform", "country", "user_type", "device_model"])

        Returns:
            {
                "game_id": str,
                "metric": str,
                "period": {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"},
                "segments": {
                    "<dimension>": {
                        "<value>": [v0, v1, ...]  # period 날짜 수만큼 정렬된 일별 값
                    }
                }
            }
        """
        ...

    @abstractmethod
    def get_available_dimensions(self, game_id: str) -> list[str]:
        """세그먼트 분석에 사용 가능한 차원 컬럼을 자동 탐지한다.

        `users` 테이블에서 세그먼트 성격의 컬럼만 추출한다 (PK·날짜 제외).
        Mock DB에 컬럼을 추가하기만 하면 에이전트가 자동으로 인식한다.

        Returns:
            예: ["platform", "country", "user_type", "device_model"]
        """
        ...

    # ────────────────────────────────────────────────────────────────
    # ③ Hypothesis Generator — 가용 테이블 스키마
    # ────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_available_schema(self, game_id: str) -> dict[str, Any]:
        """가용 테이블 목록과 컬럼 스키마를 조회한다.

        Hypothesis Generator가 프롬프트에 주입해, LLM이 가용 테이블 밖의
        가설은 `required_data`(자연어)로만 기재하게 유도한다.

        Returns:
            {
                "tables": [
                    {
                        "name": str,
                        "columns": list[str],
                        "description": str,  # 없으면 ""
                    },
                    ...
                ]
            }
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
            - 결과 행 수 `max_rows` 제한

            "쿼리 내용 수준의 검증"(SELECT-only 정규식, 위험 키워드 블랙리스트,
            테이블 화이트리스트)은 Phase 6 Data Validator의 Tool 래퍼가 담당한다.

        Args:
            query: 실행할 SQL
            max_rows: 반환 최대 행 수 (기본 100)

        Returns:
            [{컬럼명: 값, ...}, ...]

        Raises:
            RuntimeError: 쿼리 실행 중 DB 오류 발생 시
        """
        ...