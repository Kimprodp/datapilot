"""BigQuery 기반 GameDataRepository 스텁 (미구현).

Phase 5 시점에서는 스텁만 제공한다. Port/Adapter 패턴이 "어댑터 교체로
DuckDB → BigQuery 전환 가능"임을 코드로 증명하는 목적이다.

실제 구현 시점에는 다음 작업이 필요하다:
    1. `google-cloud-bigquery` 의존성 추가
    2. 서비스 계정 키 또는 ADC 기반 인증
    3. 각 메서드를 BigQuery Standard SQL로 재작성
       (DuckDB와 방언 차이: CAST / INTERVAL / DATE_SUB / ARRAY_AGG 등)
    4. 파라미터 바인딩은 `bigquery.ScalarQueryParameter` 사용

Java 비유:
    public class BigQueryGameDataRepository implements GameDataRepository {
        // 구현 전까지는 모든 메서드가 UnsupportedOperationException 던짐
    }
"""

from __future__ import annotations

from datetime import date
from typing import Any

from datapilot.repository.port import GameDataRepository


class BigQueryAdapter(GameDataRepository):
    """운영 환경용 BigQueryAdapter 스텁.

    Example 사용 예시 (구현 시):
        adapter = BigQueryAdapter(
            project_id="supercent-game-analytics",
            dataset_id="pizza_ready_prod",
        )
        repo: GameDataRepository = adapter
    """

    def __init__(self, project_id: str, dataset_id: str) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id

    def _not_implemented(self, method: str) -> NotImplementedError:
        return NotImplementedError(
            f"BigQueryAdapter.{method}는 아직 구현되지 않았습니다. "
            f"프로젝트={self.project_id}, 데이터셋={self.dataset_id}. "
            "운영 전환 시 `google-cloud-bigquery` 클라이언트로 재구현 필요."
        )

    def get_daily_kpi(
        self,
        game_id: str,
        period: tuple[date, date],
    ) -> dict[str, Any]:
        # 구현 예시 (BigQuery Standard SQL):
        #   SELECT date, dau, mau, revenue, ...
        #   FROM `{project}.{dataset}.daily_kpi`
        #   WHERE game_id = @game_id
        #     AND date BETWEEN @start AND @end
        #   ORDER BY date
        raise self._not_implemented("get_daily_kpi")

    def get_available_dimensions(self, game_id: str) -> list[str]:
        # 구현 예시:
        #   SELECT column_name
        #   FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS
        #   WHERE table_name = 'users'
        raise self._not_implemented("get_available_dimensions")

    def get_metric_by_segments(
        self,
        game_id: str,
        metric: str,
        period: tuple[date, date],
        dimensions: list[str],
    ) -> dict[str, Any]:
        # BigQuery에서 `users` 테이블의 세그먼트 컬럼을 동적으로 참조하는 패턴은
        # 파라미터 바인딩으로 안 되고 쿼리 템플릿 문자열 치환 + 화이트리스트 검증이 필요.
        # DuckDBAdapter와 동일한 안전 로직을 재사용해야 한다.
        raise self._not_implemented("get_metric_by_segments")

    def get_available_schema(self, game_id: str) -> dict[str, Any]:
        # 구현 예시:
        #   SELECT table_name, column_name
        #   FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS
        #   ORDER BY table_name, ordinal_position
        raise self._not_implemented("get_available_schema")

    def execute_readonly_sql(
        self,
        query: str,
        max_rows: int = 100,
    ) -> list[dict[str, Any]]:
        # BigQuery 권한 모델:
        #   - 서비스 계정에 `roles/bigquery.dataViewer` + `roles/bigquery.jobUser`만 부여
        #   - 쓰기/DDL은 IAM 레벨에서 차단되어 Python 레벨 정규식 필터가 2차 방어
        #   - `QueryJobConfig(maximum_bytes_billed=...)`로 비용 상한도 설정
        raise self._not_implemented("execute_readonly_sql")