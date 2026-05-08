"""make_repository 팩토리 단위 테스트.

검증:
- 정상: 등록된 도메인 (game / ecommerce) 으로 DuckDBAdapter 인스턴스 반환
- 예외: 알 수 없는 도메인 → ValueError + 메시지에 가능한 도메인 나열
- 예외: DB 파일 없음 → FileNotFoundError + seed 안내 메시지
- DataRepository 인터페이스 충족 (isinstance 검증)

Streamlit 의 ``@st.cache_resource`` 적용은 ``app.py`` 책임이라 본 테스트는
순수 함수 ``make_repository`` 동작만 검증.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from datapilot.domain import DOMAINS
from datapilot.repository import DuckDBAdapter, make_repository
from datapilot.repository.port import DataRepository


# ════════════════════════════════════════════════════════════════════
# 1. 정상 경로 — 등록된 도메인
# ════════════════════════════════════════════════════════════════════


class TestMakeRepositoryHappy:
    def test_game_returns_duckdb_adapter(self):
        repo = make_repository("game")
        try:
            assert isinstance(repo, DuckDBAdapter)
            assert isinstance(repo, DataRepository)
        finally:
            repo.close()

    def test_ecommerce_returns_duckdb_adapter(self):
        """이커머스 DB 는 빈 스키마라도 파일 자체는 존재해야 한다."""
        repo = make_repository("ecommerce")
        try:
            assert isinstance(repo, DuckDBAdapter)
            assert isinstance(repo, DataRepository)
        finally:
            repo.close()

    def test_returns_distinct_instances(self):
        """팩토리 자체는 매번 새 인스턴스. 캐싱은 app.py 책임."""
        repo_a = make_repository("game")
        repo_b = make_repository("game")
        try:
            assert repo_a is not repo_b
        finally:
            repo_a.close()
            repo_b.close()


# ════════════════════════════════════════════════════════════════════
# 2. 예외 경로
# ════════════════════════════════════════════════════════════════════


class TestMakeRepositoryErrors:
    def test_unknown_domain_raises_value_error(self):
        with pytest.raises(ValueError) as exc_info:
            make_repository("unknown_domain_xxx")
        msg = str(exc_info.value)
        assert "unsupported domain" in msg
        # 메시지에 가능한 도메인 모두 나열되어 있어야 함
        for name in DOMAINS:
            assert name in msg, f"에러 메시지에 도메인 '{name}' 누락"

    def test_unknown_domain_lists_supported_alphabetically(self):
        with pytest.raises(ValueError) as exc_info:
            make_repository("xxx")
        # supported 도메인 목록이 정렬되어 출력되는지 (가독성)
        msg = str(exc_info.value)
        sorted_names = sorted(DOMAINS.keys())
        # 정렬된 순서대로 나타나는지 (앞쪽이 뒤쪽보다 먼저)
        positions = [msg.find(n) for n in sorted_names]
        assert all(p >= 0 for p in positions)
        assert positions == sorted(positions), (
            f"도메인 정렬 출력 X: 위치 {positions}"
        )

    def test_db_file_missing_raises_file_not_found(self, tmp_path: Path):
        """DB 파일이 없는 도메인을 만들면 FileNotFoundError + seed 안내."""
        # DOMAINS 에 가짜 도메인 추가 + db_path 가 없는 곳
        from datapilot.domain.base import (
            DomainConfig,
            DomainKeywords,
            UILabels,
        )

        fake_domain = DomainConfig(
            name="fake",
            db_path=str(tmp_path / "nonexistent.db"),
            allowed_tables=frozenset({"foo"}),
            supported_segment_metrics=frozenset({"bar"}),
            ui_labels=UILabels(
                industry_name="가짜",
                entity_default_id="fake_demo",
                kpi_korean={"bar": "테스트"},
                scenario_descriptions=("가짜 시나리오",),
            ),
            agent_keywords=DomainKeywords(
                persona="테스터",
                role_descriptor="테스트 분석가",
                primary_kpis=("bar",),
            ),
        )

        with patch.dict(DOMAINS, {"fake": fake_domain}):
            with pytest.raises(FileNotFoundError) as exc_info:
                make_repository("fake")
            msg = str(exc_info.value)
            assert "seed_mock_data.py" in msg, (
                "에러 메시지에 seed 안내가 없음"
            )


# ════════════════════════════════════════════════════════════════════
# 3. DuckDBAdapter 직접 호출도 동일 동작
# ════════════════════════════════════════════════════════════════════


class TestDuckDBAdapterDomainArg:
    def test_domain_game_default(self):
        """디폴트 domain='game' 호환."""
        repo = DuckDBAdapter()
        try:
            assert repo._domain == "game"
        finally:
            repo.close()

    def test_domain_ecommerce_explicit(self):
        repo = DuckDBAdapter(domain="ecommerce")
        try:
            assert repo._domain == "ecommerce"
        finally:
            repo.close()

    def test_unknown_domain_value_error(self):
        with pytest.raises(ValueError):
            DuckDBAdapter(domain="xxx_unknown")
