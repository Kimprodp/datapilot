"""공통 pytest fixture.

세션 스코프 read-only DuckDB 연결을 한 번 열어 모든 테스트에서 재사용한다.
DB 파일이 없으면 해당 fixture를 사용하는 테스트 전체를 skip한다.

Java 비유:
    @BeforeAll static void setUpDb() { ... }  // 클래스 단위가 아닌 세션 전체
"""

from __future__ import annotations

import duckdb
import pytest

from datapilot.repository.duckdb_adapter import DEFAULT_DB_PATH, DuckDBAdapter


@pytest.fixture(scope="session")
def mock_db_conn():
    """실제 Mock DB를 read-only로 연 DuckDB 연결. 세션 내 한 번만 열린다."""
    if not DEFAULT_DB_PATH.exists():
        pytest.skip(f"Mock DB 파일 없음: {DEFAULT_DB_PATH}. seed_mock_data.py 먼저 실행 필요.")
    conn = duckdb.connect(str(DEFAULT_DB_PATH), read_only=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def adapter(mock_db_conn):
    """세션 스코프 DuckDBAdapter. connection 주입 방식으로 생성."""
    return DuckDBAdapter(connection=mock_db_conn)