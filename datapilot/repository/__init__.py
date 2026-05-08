"""Port/Adapter 계층.

에이전트가 의존하는 데이터 접근 인터페이스(Port)와 구현(Adapter)을 제공한다.
"""

from datapilot.repository.duckdb_adapter import DuckDBAdapter
from datapilot.repository.port import DataRepository, get_supported_segment_metrics


def make_repository(domain: str) -> DataRepository:
    """도메인 인자에 따라 ``DataRepository`` 인스턴스를 생성하는 팩토리.

    순수 함수라 테스트 친화. Streamlit UI 에서는 ``app.py`` 가 별도로
    ``@st.cache_resource`` 로 wrap 해 도메인별 1 개 인스턴스를 보관한다.

    Args:
        domain: ``DOMAINS`` dict 의 키. 현재 ``"game"`` / ``"ecommerce"``.

    Returns:
        해당 도메인의 read-only ``DuckDBAdapter``.

    Raises:
        ValueError: 알 수 없는 ``domain``.
        FileNotFoundError: 해당 도메인의 DB 파일이 없음 (seed 미실행).
    """
    return DuckDBAdapter(domain=domain)


__all__ = [
    "DataRepository",
    "DuckDBAdapter",
    "get_supported_segment_metrics",
    "make_repository",
]
