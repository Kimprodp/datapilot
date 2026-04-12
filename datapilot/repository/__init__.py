"""Port/Adapter 계층.

에이전트가 의존하는 데이터 접근 인터페이스(Port)와 구현(Adapter)을 제공한다.
"""

from datapilot.repository.duckdb_adapter import DuckDBAdapter
from datapilot.repository.port import SUPPORTED_SEGMENT_METRICS, GameDataRepository

__all__ = ["GameDataRepository", "DuckDBAdapter", "SUPPORTED_SEGMENT_METRICS"]