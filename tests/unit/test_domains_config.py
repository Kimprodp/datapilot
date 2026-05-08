"""도메인 패키지 (datapilot/domain/) 정합성 단위 테스트.

검증 범위 (1차 — 본 task):
- DOMAINS dict 의 모든 항목이 DomainConfig 의 필드를 채움
- name 이 dict key 와 일치
- 각 필드의 타입/모양 (frozenset / dict / tuple / dataclass)
- DomainKeywords / UILabels 의 필수 항목 누락 없음

검증 범위 (2차 — DB 파일 분리 task 이후 별도 테스트):
- allowed_tables ⊆ 실제 DuckDB 의 테이블 목록 (실제 conn 필요)
"""

from __future__ import annotations

import pytest

from datapilot.domain import DOMAINS, ECOMMERCE, GAME, DomainConfig
from datapilot.domain.base import DomainKeywords, UILabels


# ════════════════════════════════════════════════════════════════════
# 1. DOMAINS dict 자체
# ════════════════════════════════════════════════════════════════════


class TestDomainsRegistry:
    def test_has_game_and_ecommerce(self):
        assert "game" in DOMAINS
        assert "ecommerce" in DOMAINS

    def test_no_extra_domains_in_first_release(self):
        """본 묶음은 게임 + 이커머스 2 도메인만. 추가 시 본 테스트 갱신."""
        assert set(DOMAINS.keys()) == {"game", "ecommerce"}

    def test_each_value_is_domain_config(self):
        for name, cfg in DOMAINS.items():
            assert isinstance(cfg, DomainConfig), f"{name} 값이 DomainConfig 가 아님"

    def test_dict_key_matches_config_name(self):
        for key, cfg in DOMAINS.items():
            assert cfg.name == key, (
                f"DOMAINS dict key='{key}' 와 DomainConfig.name='{cfg.name}' 불일치"
            )


# ════════════════════════════════════════════════════════════════════
# 2. DomainConfig 필드 무결성 (모든 도메인)
# ════════════════════════════════════════════════════════════════════


@pytest.fixture(params=list(DOMAINS.items()), ids=lambda p: p[0])
def domain(request) -> tuple[str, DomainConfig]:
    return request.param


class TestDomainConfigFields:
    def test_name_is_lowercase_alpha(self, domain):
        name, cfg = domain
        assert cfg.name.replace("_", "").isalnum() and cfg.name == cfg.name.lower(), (
            f"{name}: name 이 소문자 영문(+_) 가 아님: {cfg.name!r}"
        )

    def test_db_path_is_under_data_mock(self, domain):
        name, cfg = domain
        assert cfg.db_path.startswith("data/mock/"), (
            f"{name}: db_path 가 data/mock/ 아래가 아님: {cfg.db_path!r}"
        )
        assert cfg.db_path.endswith(".db"), f"{name}: db_path 확장자가 .db 가 아님"

    def test_allowed_tables_is_non_empty_frozenset(self, domain):
        name, cfg = domain
        assert isinstance(cfg.allowed_tables, frozenset), (
            f"{name}: allowed_tables 가 frozenset 이 아님"
        )
        assert len(cfg.allowed_tables) >= 1, f"{name}: allowed_tables 가 비어 있음"

    def test_supported_segment_metrics_is_non_empty_frozenset(self, domain):
        name, cfg = domain
        assert isinstance(cfg.supported_segment_metrics, frozenset)
        assert len(cfg.supported_segment_metrics) >= 1, (
            f"{name}: supported_segment_metrics 가 비어 있음"
        )

    def test_ui_labels_is_ui_labels_instance(self, domain):
        name, cfg = domain
        assert isinstance(cfg.ui_labels, UILabels)

    def test_agent_keywords_is_domain_keywords_instance(self, domain):
        name, cfg = domain
        assert isinstance(cfg.agent_keywords, DomainKeywords)


# ════════════════════════════════════════════════════════════════════
# 3. UILabels 필드 무결성
# ════════════════════════════════════════════════════════════════════


class TestUILabels:
    def test_industry_name_non_empty(self, domain):
        name, cfg = domain
        assert cfg.ui_labels.industry_name.strip(), (
            f"{name}: industry_name 이 비어 있음"
        )

    def test_entity_default_id_non_empty(self, domain):
        name, cfg = domain
        assert cfg.ui_labels.entity_default_id.strip()

    def test_kpi_korean_non_empty_dict(self, domain):
        name, cfg = domain
        assert isinstance(cfg.ui_labels.kpi_korean, dict)
        assert len(cfg.ui_labels.kpi_korean) >= 1, f"{name}: kpi_korean 이 비어 있음"

    def test_kpi_korean_values_non_empty(self, domain):
        name, cfg = domain
        for code, label in cfg.ui_labels.kpi_korean.items():
            assert label.strip(), f"{name}: kpi_korean[{code!r}] 가 비어 있음"

    def test_supported_metrics_subset_of_kpi_korean(self, domain):
        """segmentable KPI 는 한글 라벨이 있어야 화면에 표시된다."""
        name, cfg = domain
        missing = cfg.supported_segment_metrics - set(cfg.ui_labels.kpi_korean.keys())
        assert not missing, (
            f"{name}: segmentable KPI {missing} 의 한글 라벨이 ui_labels.kpi_korean 에 없음"
        )

    def test_scenario_descriptions_non_empty_tuple(self, domain):
        name, cfg = domain
        assert isinstance(cfg.ui_labels.scenario_descriptions, tuple)
        assert len(cfg.ui_labels.scenario_descriptions) >= 1


# ════════════════════════════════════════════════════════════════════
# 4. DomainKeywords 필드 무결성
# ════════════════════════════════════════════════════════════════════


class TestDomainKeywords:
    def test_persona_non_empty(self, domain):
        name, cfg = domain
        assert cfg.agent_keywords.persona.strip()

    def test_role_descriptor_non_empty(self, domain):
        name, cfg = domain
        assert cfg.agent_keywords.role_descriptor.strip()

    def test_primary_kpis_non_empty_tuple(self, domain):
        name, cfg = domain
        assert isinstance(cfg.agent_keywords.primary_kpis, tuple)
        assert len(cfg.agent_keywords.primary_kpis) >= 1


# ════════════════════════════════════════════════════════════════════
# 5. 도메인 간 격리
# ════════════════════════════════════════════════════════════════════


class TestDomainIsolation:
    def test_db_paths_distinct(self):
        paths = [cfg.db_path for cfg in DOMAINS.values()]
        assert len(paths) == len(set(paths)), "두 도메인이 같은 DB 파일을 공유함"

    def test_industry_names_distinct(self):
        names = [cfg.ui_labels.industry_name for cfg in DOMAINS.values()]
        assert len(names) == len(set(names)), "두 도메인이 같은 industry_name 사용"

    def test_personas_distinct(self):
        personas = [cfg.agent_keywords.persona for cfg in DOMAINS.values()]
        assert len(personas) == len(set(personas)), (
            "두 도메인이 같은 persona 사용 — ③⑤⑥ 키워드 주입이 도메인 차이를 못 만듦"
        )


# ════════════════════════════════════════════════════════════════════
# 6. 게임 / 이커머스 개별 sanity
# ════════════════════════════════════════════════════════════════════


class TestGameDomain:
    def test_entity_default_id_is_pizza_ready(self):
        assert GAME.ui_labels.entity_default_id == "pizza_ready"

    def test_has_legacy_segment_metrics(self):
        """analysis-performance 묶음이 정비한 4 개 segmentable KPI 보존."""
        assert GAME.supported_segment_metrics == frozenset({
            "revenue",
            "dau",
            "payment_success_rate",
            "d7_retention",
        })

    def test_has_12_mock_tables(self):
        """현재 게임 mock DB 의 테이블 12 개 (mock-data-schema.md 기준)."""
        assert len(GAME.allowed_tables) == 12


class TestEcommerceDomain:
    def test_entity_default_id_is_ecommerce_demo(self):
        assert ECOMMERCE.ui_labels.entity_default_id == "ecommerce_demo"

    def test_has_gmv_in_segment_metrics(self):
        assert "gmv" in ECOMMERCE.supported_segment_metrics

    def test_allowed_tables_contains_scenario_targets(self):
        """시나리오 B (재고) / C (프로모션) 검증 SQL 이 가리킬 테이블 포함."""
        required = {
            "category_daily_revenue",  # 시나리오 B
            "products",                 # 시나리오 B
            "promotions",               # 시나리오 C
        }
        assert required <= ECOMMERCE.allowed_tables, (
            f"이커머스 시나리오 검증에 필수 테이블 누락: "
            f"{required - ECOMMERCE.allowed_tables}"
        )