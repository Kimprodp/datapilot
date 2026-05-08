"""③⑤⑥ user template 의 도메인 키워드 placeholder 검증.

검증 범위:
- ③ HypothesisGenerator USER_PROMPT_TEMPLATE 에 `{role_descriptor}`, `{primary_kpis}`,
  `{persona}`, `{entity_id}` placeholder 존재
- ⑤ RootCauseReasoner USER_PROMPT_TEMPLATE 에 `{role_descriptor}`, `{primary_kpis}`
- ⑥ ActionRecommender USER_PROMPT_TEMPLATE 에 `{role_descriptor}`, `{persona}`
- ① ② ④ user template 에는 도메인 키워드 placeholder 없음 (도메인 무관)
- 게임/이커머스 키워드 주입 시 출력 텍스트가 도메인별로 달라짐
- system cache 영역 (build_system_content / _SYSTEM_BLOCKS) 에는 도메인 키워드 미등장
  (Prompt Caching cache 영역 보존)
"""

from __future__ import annotations

import pytest

from datapilot.agents import (
    action_recommender as ar,
    bottleneck_detector as bd,
    data_validator as dv,
    hypothesis_generator as hg,
    root_cause_reasoner as rc,
    segmentation_analyzer as sa,
)
from datapilot.domain import ECOMMERCE, GAME


# ════════════════════════════════════════════════════════════════════
# 1. ③⑤⑥ user template 에 도메인 키워드 placeholder 존재
# ════════════════════════════════════════════════════════════════════


class TestPlaceholdersPresent:
    def test_hypothesis_has_role_kpis_persona_entity(self):
        tpl = hg.USER_PROMPT_TEMPLATE
        for p in ("{role_descriptor}", "{primary_kpis}", "{persona}", "{entity_id}"):
            assert p in tpl, f"③ user template 에 {p} 누락"

    def test_root_cause_has_role_kpis(self):
        tpl = rc.USER_PROMPT_TEMPLATE
        for p in ("{role_descriptor}", "{primary_kpis}"):
            assert p in tpl, f"⑤ user template 에 {p} 누락"

    def test_action_has_role_persona(self):
        tpl = ar.USER_PROMPT_TEMPLATE
        for p in ("{role_descriptor}", "{persona}"):
            assert p in tpl, f"⑥ user template 에 {p} 누락"


# ════════════════════════════════════════════════════════════════════
# 2. ① ② ④ user template 에 도메인 키워드 placeholder 없음
# ════════════════════════════════════════════════════════════════════


class TestNonDomainAgentsHaveNoKeywordPlaceholders:
    """① 시계열 수치 비교 / ② 세그먼트 패턴 / ④ SQL 검증 — 도메인 무관."""

    @pytest.mark.parametrize(
        ("name", "tpl"),
        [
            ("bottleneck_detector", bd.USER_PROMPT_TEMPLATE),
            ("segmentation_analyzer", sa.USER_PROMPT_TEMPLATE),
        ],
    )
    def test_no_domain_keyword_placeholders(self, name, tpl):
        for p in ("{role_descriptor}", "{primary_kpis}", "{persona}"):
            assert p not in tpl, (
                f"{name}: 도메인 무관 에이전트인데 {p} placeholder 가 있음"
            )

    def test_data_validator_user_template_no_keyword_placeholders(self):
        # ④ 의 user 템플릿은 가설 텍스트 동적 빌드라 USER_PROMPT_TEMPLATE 상수 X.
        # 대신 모듈에 도메인 키워드 placeholder 가 박혀있는지 회귀로 확인.
        import inspect

        src = inspect.getsource(dv)
        for p in ("{role_descriptor}", "{primary_kpis}", "{persona}"):
            assert p not in src, (
                f"data_validator 모듈에 도메인 키워드 placeholder {p} 가 박혀 있음 "
                "— ④ 는 도메인 무관이라 주입 X"
            )


# ════════════════════════════════════════════════════════════════════
# 3. 도메인 키워드 주입 시 텍스트가 도메인별로 달라짐
# ════════════════════════════════════════════════════════════════════


def _format_hypothesis(kw):
    return hg.USER_PROMPT_TEMPLATE.format(
        role_descriptor=kw.role_descriptor,
        primary_kpis=", ".join(kw.primary_kpis),
        persona=kw.persona,
        entity_id="test_entity",
        anomaly_json="{}",
        segmentation_json="{}",
    )


def _format_root_cause(kw):
    return rc.USER_PROMPT_TEMPLATE.format(
        role_descriptor=kw.role_descriptor,
        primary_kpis=", ".join(kw.primary_kpis),
        anomaly_json="{}",
        segmentation_json="{}",
        supported_json="[]",
        rejected_json="[]",
        unverified_json="[]",
    )


def _format_action(kw):
    return ar.USER_PROMPT_TEMPLATE.format(
        role_descriptor=kw.role_descriptor,
        persona=kw.persona,
        anomaly="test",
        root_cause_json="{}",
        additional_investigation_json="[]",
        is_unknown_cause="False",
    )


@pytest.mark.parametrize(
    ("agent_name", "fmt"),
    [
        ("hypothesis_generator", _format_hypothesis),
        ("root_cause_reasoner", _format_root_cause),
        ("action_recommender", _format_action),
    ],
)
class TestDomainKeywordsAffectText:
    def test_game_keywords_appear_in_output(self, agent_name, fmt):
        text = fmt(GAME.agent_keywords)
        assert GAME.agent_keywords.role_descriptor in text

    def test_ecommerce_keywords_appear_in_output(self, agent_name, fmt):
        text = fmt(ECOMMERCE.agent_keywords)
        assert ECOMMERCE.agent_keywords.role_descriptor in text

    def test_game_and_ecommerce_outputs_differ(self, agent_name, fmt):
        game_text = fmt(GAME.agent_keywords)
        ec_text = fmt(ECOMMERCE.agent_keywords)
        assert game_text != ec_text, (
            f"{agent_name}: 게임/이커머스 키워드가 같은 출력을 만듦 — 주입 작동 안 함"
        )


# ════════════════════════════════════════════════════════════════════
# 4. system cache 영역에 도메인 키워드 미등장 (Prompt Caching 보존)
# ════════════════════════════════════════════════════════════════════


class TestSystemCacheAreaCleanOfDomainKeywords:
    """직전 묶음 (analysis-performance) 의 Prompt Caching 효과를 깨지 않도록
    system 의 cache_control 부착 영역에 도메인 키워드가 들어가면 안 된다.
    """

    # 검증 의도: 본 task 가 도메인 키워드를 user 메시지에만 주입했고 system cache
    # 영역에 박지 않았다는 것. 기존 system 프롬프트 본문에 게임 페르소나 표현이
    # 일부 남아 있는 건 직전 (analysis-performance) 묶음의 산출물이라 본 task 영역 X
    # → 이커머스 도메인 키워드만 검증 (게임 디폴트 fallback 이라 게임 키워드는
    # 자연 등장 가능, 이커머스는 본 task 이전엔 어디에도 없어야 정상).

    @pytest.mark.parametrize(
        ("name", "blocks"),
        [
            ("bottleneck_detector", bd._SYSTEM_BLOCKS),
            ("segmentation_analyzer", sa._SYSTEM_BLOCKS),
            ("root_cause_reasoner", rc._SYSTEM_BLOCKS),
            ("action_recommender", ar._SYSTEM_BLOCKS),
        ],
    )
    def test_static_system_blocks_have_no_ecommerce_keywords(self, name, blocks):
        text = "".join(b.get("text", "") for b in blocks)
        forbidden = (
            ECOMMERCE.agent_keywords.persona,
            ECOMMERCE.agent_keywords.role_descriptor,
        )
        for kw in forbidden:
            assert kw not in text, (
                f"{name}: system cache 영역에 이커머스 키워드 {kw!r} 등장 → "
                "도메인 전환 시 cache miss 폭증 위험"
            )

    def test_hypothesis_dynamic_system_clean_of_ecommerce(self):
        """③ 의 동적 system 빌드도 이커머스 키워드 미오염."""
        light_schema = {"tables": [{"name": "t", "description": "d"}]}
        blocks = hg.build_system_content(light_schema)
        text = "".join(b.get("text", "") for b in blocks)
        forbidden = (
            ECOMMERCE.agent_keywords.persona,
            ECOMMERCE.agent_keywords.role_descriptor,
        )
        for kw in forbidden:
            assert kw not in text, (
                f"③ build_system_content: 이커머스 키워드 {kw!r} 등장 → cache miss 위험"
            )

    def test_data_validator_dynamic_system_clean_of_ecommerce(self):
        """④ 의 동적 system 빌드도 이커머스 키워드 미오염."""
        light_schema = {"tables": [{"name": "t", "columns": ["c1"]}]}
        blocks = dv.build_system_content(light_schema)
        text = "".join(b.get("text", "") for b in blocks)
        forbidden = (
            ECOMMERCE.agent_keywords.persona,
            ECOMMERCE.agent_keywords.role_descriptor,
        )
        for kw in forbidden:
            assert kw not in text, (
                f"④ build_system_content: 이커머스 키워드 {kw!r} 등장 → cache miss 위험"
            )
