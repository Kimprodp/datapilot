"""AgentBundle.create 단위 테스트.

검증:
- 정상: 등록된 도메인으로 6 에이전트 묶음 생성
- 예외: 알 수 없는 도메인 → ValueError
- 도메인 키워드 전파: 6 에이전트 모두 ``_domain_keywords`` 주입
- ④ DataValidator 의 ``_domain_allowed_tables`` 가 도메인 화이트리스트와 일치
- 게임 / 이커머스 도메인이 다른 인스턴스 생성
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from datapilot.agents import AgentBundle
from datapilot.agents.action_recommender import ActionRecommender
from datapilot.agents.bottleneck_detector import BottleneckDetector
from datapilot.agents.data_validator import DataValidator
from datapilot.agents.hypothesis_generator import HypothesisGenerator
from datapilot.agents.root_cause_reasoner import RootCauseReasoner
from datapilot.agents.segmentation_analyzer import SegmentationAnalyzer
from datapilot.domain import DOMAINS, ECOMMERCE, GAME
from datapilot.repository.port import DataRepository


@pytest.fixture
def fake_repo() -> DataRepository:
    return MagicMock(spec=DataRepository)


# ════════════════════════════════════════════════════════════════════
# 1. 정상 경로 — 등록된 도메인
# ════════════════════════════════════════════════════════════════════


class TestCreateHappy:
    def test_game_returns_bundle(self, fake_repo):
        bundle = AgentBundle.create("game", repo=fake_repo)
        assert isinstance(bundle, AgentBundle)

    def test_ecommerce_returns_bundle(self, fake_repo):
        bundle = AgentBundle.create("ecommerce", repo=fake_repo)
        assert isinstance(bundle, AgentBundle)

    def test_six_agents_correct_types(self, fake_repo):
        bundle = AgentBundle.create("game", repo=fake_repo)
        assert isinstance(bundle.bottleneck, BottleneckDetector)
        assert isinstance(bundle.segmenter, SegmentationAnalyzer)
        assert isinstance(bundle.hypothesis, HypothesisGenerator)
        assert isinstance(bundle.validator, DataValidator)
        assert isinstance(bundle.reasoner, RootCauseReasoner)
        assert isinstance(bundle.recommender, ActionRecommender)


# ════════════════════════════════════════════════════════════════════
# 2. 예외 경로
# ════════════════════════════════════════════════════════════════════


class TestCreateErrors:
    def test_unknown_domain_raises_value_error(self, fake_repo):
        with pytest.raises(ValueError) as exc_info:
            AgentBundle.create("unknown_xxx", repo=fake_repo)
        msg = str(exc_info.value)
        assert "unsupported domain" in msg
        for name in DOMAINS:
            assert name in msg


# ════════════════════════════════════════════════════════════════════
# 3. 도메인 키워드 전파
# ════════════════════════════════════════════════════════════════════


class TestDomainKeywordsPropagation:
    def test_game_keywords_propagated_to_all_six(self, fake_repo):
        bundle = AgentBundle.create("game", repo=fake_repo)
        expected = GAME.agent_keywords
        assert bundle.bottleneck._domain_keywords is expected
        assert bundle.segmenter._domain_keywords is expected
        assert bundle.hypothesis._domain_keywords is expected
        assert bundle.validator._domain_keywords is expected
        assert bundle.reasoner._domain_keywords is expected
        assert bundle.recommender._domain_keywords is expected

    def test_ecommerce_keywords_propagated_to_all_six(self, fake_repo):
        bundle = AgentBundle.create("ecommerce", repo=fake_repo)
        expected = ECOMMERCE.agent_keywords
        assert bundle.bottleneck._domain_keywords is expected
        assert bundle.segmenter._domain_keywords is expected
        assert bundle.hypothesis._domain_keywords is expected
        assert bundle.validator._domain_keywords is expected
        assert bundle.reasoner._domain_keywords is expected
        assert bundle.recommender._domain_keywords is expected

    def test_game_and_ecommerce_keywords_differ(self, fake_repo):
        game_bundle = AgentBundle.create("game", repo=fake_repo)
        ec_bundle = AgentBundle.create("ecommerce", repo=fake_repo)
        assert (
            game_bundle.hypothesis._domain_keywords
            is not ec_bundle.hypothesis._domain_keywords
        )


# ════════════════════════════════════════════════════════════════════
# 4. ④ DataValidator 화이트리스트 주입
# ════════════════════════════════════════════════════════════════════


class TestValidatorAllowedTables:
    def test_game_validator_has_game_allowed_tables(self, fake_repo):
        bundle = AgentBundle.create("game", repo=fake_repo)
        assert bundle.validator._domain_allowed_tables == GAME.allowed_tables

    def test_ecommerce_validator_has_ecommerce_allowed_tables(self, fake_repo):
        bundle = AgentBundle.create("ecommerce", repo=fake_repo)
        assert (
            bundle.validator._domain_allowed_tables == ECOMMERCE.allowed_tables
        )

    def test_validator_has_repo_injected(self, fake_repo):
        bundle = AgentBundle.create("game", repo=fake_repo)
        assert bundle.validator._repo is fake_repo


# ════════════════════════════════════════════════════════════════════
# 5. PipelineOrchestrator 와의 통합 (agents=None legacy 동작)
# ════════════════════════════════════════════════════════════════════


class TestPipelineLegacyDefault:
    def test_pipeline_without_agents_uses_game_default(self, fake_repo):
        from datapilot.pipeline import PipelineOrchestrator

        # agents=None → 내부에서 AgentBundle.create("game", repo=repo) 호출
        pipeline = PipelineOrchestrator(fake_repo)
        # 게임 디폴트 키워드가 ③⑤⑥ 에 주입됐는지 spot-check
        assert (
            pipeline._hypothesis_gen._domain_keywords
            is GAME.agent_keywords
        )

    def test_pipeline_with_explicit_agents(self, fake_repo):
        from datapilot.pipeline import PipelineOrchestrator

        agents = AgentBundle.create("ecommerce", repo=fake_repo)
        pipeline = PipelineOrchestrator(fake_repo, agents=agents)
        assert (
            pipeline._hypothesis_gen._domain_keywords
            is ECOMMERCE.agent_keywords
        )
