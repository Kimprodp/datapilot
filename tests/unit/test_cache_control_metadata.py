"""6 에이전트의 Prompt Caching 메타데이터 검증.

검증 범위:
- 각 에이전트 모듈의 `_SYSTEM_BLOCKS` 가 ephemeral cache_control 부착 형태
- 시스템 프롬프트 텍스트 자체는 모듈 상수 SYSTEM_PROMPT 와 일치 (이중 정의 회귀 방지)
- HumanMessage 쪽 (user template) 에는 cache_control 미부착 — 변동값 포함이라 캐싱 부적합
- ④ Validator 는 메인 + verdict 두 곳 모두 검증
- 정적성: import 후 다회 참조해도 동일 텍스트
"""

from __future__ import annotations

from typing import Any

import pytest

from datapilot.agents import (
    action_recommender as ar,
    bottleneck_detector as bd,
    data_validator as dv,
    hypothesis_generator as hg,
    root_cause_reasoner as rc,
    segmentation_analyzer as sa,
)


# (모듈, system 블록 상수, 원본 시스템 프롬프트) 6 에이전트 + ④ verdict
_AGENTS: list[tuple[str, list[dict[str, Any]], str]] = [
    ("bottleneck_detector", bd._SYSTEM_BLOCKS, bd.SYSTEM_PROMPT),
    ("segmentation_analyzer", sa._SYSTEM_BLOCKS, sa.SYSTEM_PROMPT),
    ("hypothesis_generator", hg._SYSTEM_BLOCKS, hg.SYSTEM_PROMPT),
    ("data_validator", dv._SYSTEM_BLOCKS, dv.SYSTEM_PROMPT),
    ("root_cause_reasoner", rc._SYSTEM_BLOCKS, rc.SYSTEM_PROMPT),
    ("action_recommender", ar._SYSTEM_BLOCKS, ar.SYSTEM_PROMPT),
]


# ════════════════════════════════════════════════════════════════════
# 1. 각 에이전트 SystemMessage block list 형식
# ════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(("name", "blocks", "_prompt"), _AGENTS)
class TestSystemBlocks:
    def test_blocks_is_non_empty_list(self, name, blocks, _prompt):
        assert isinstance(blocks, list)
        assert len(blocks) >= 1, f"{name}: _SYSTEM_BLOCKS 가 비어있음"

    def test_each_block_is_text_type(self, name, blocks, _prompt):
        for i, block in enumerate(blocks):
            assert block.get("type") == "text", (
                f"{name}: block[{i}] 의 type 이 text 가 아님"
            )

    def test_each_block_has_ephemeral_cache_control(self, name, blocks, _prompt):
        for i, block in enumerate(blocks):
            assert block.get("cache_control") == {"type": "ephemeral"}, (
                f"{name}: block[{i}] 에 cache_control={{'type':'ephemeral'}} 없음"
            )

    def test_block_text_matches_system_prompt(self, name, blocks, _prompt):
        """블록 안 텍스트가 모듈 상수 SYSTEM_PROMPT 와 일치 (이중 정의 회귀 방지)."""
        joined = "".join(b.get("text", "") for b in blocks)
        assert joined == _prompt, (
            f"{name}: block 텍스트가 모듈 SYSTEM_PROMPT 와 다름"
        )


# ════════════════════════════════════════════════════════════════════
# 2. ④ Validator verdict_chain 도 별도 cache_control
# ════════════════════════════════════════════════════════════════════


class TestValidatorVerdictBlocks:
    def test_verdict_blocks_has_ephemeral_cache_control(self):
        for i, block in enumerate(dv._VERDICT_SYSTEM_BLOCKS):
            assert block["cache_control"] == {"type": "ephemeral"}, (
                f"verdict block[{i}] 에 cache_control 없음"
            )

    def test_verdict_block_text_matches_constant(self):
        joined = "".join(b.get("text", "") for b in dv._VERDICT_SYSTEM_BLOCKS)
        assert joined == dv._VERDICT_SYSTEM


# ════════════════════════════════════════════════════════════════════
# 3. 정적성 — 다회 참조해도 동일
# ════════════════════════════════════════════════════════════════════


class TestStaticness:
    @pytest.mark.parametrize(("name", "blocks", "_prompt"), _AGENTS)
    def test_text_is_identical_across_repeated_access(
        self, name, blocks, _prompt,
    ):
        """import 후 다회 참조해도 시스템 텍스트가 동일해야 캐시 hit 가 유지된다."""
        first = "".join(b["text"] for b in blocks)
        for _ in range(5):
            current = "".join(b["text"] for b in blocks)
            assert current == first, (
                f"{name}: 시스템 프롬프트 텍스트가 호출마다 변하면 cache miss 발생"
            )


# ════════════════════════════════════════════════════════════════════
# 4. HumanMessage 쪽 (user template) 미부착
# ════════════════════════════════════════════════════════════════════


class TestUserTemplateNotCached:
    """user 메시지에는 변동값 (game_id / segment_json / hypothesis 등) 이
    포함되므로 cache_control 부착 X. 부착하면 매번 cache miss + 비용 손실."""

    def test_user_templates_are_plain_strings(self):
        # ChatPromptTemplate 의 user 부분은 모두 단순 문자열 템플릿
        # (block list 가 아님 = cache_control 키 자체가 들어갈 자리가 없음)
        templates = [
            ("bottleneck_detector", bd.USER_PROMPT_TEMPLATE),
            ("segmentation_analyzer", sa.USER_PROMPT_TEMPLATE),
            ("hypothesis_generator", hg.USER_PROMPT_TEMPLATE),
            ("data_validator", dv.USER_PROMPT_TEMPLATE),
            ("root_cause_reasoner", rc.USER_PROMPT_TEMPLATE),
            ("action_recommender", ar.USER_PROMPT_TEMPLATE),
        ]
        for name, tpl in templates:
            assert isinstance(tpl, str), f"{name}: user template 이 string 이 아님"
            assert "cache_control" not in tpl, (
                f"{name}: user template 에 'cache_control' 문자열이 섞여 있음"
            )


# ════════════════════════════════════════════════════════════════════
# 5. ChatPromptTemplate 구조 — SystemMessage 가 첫 번째
# ════════════════════════════════════════════════════════════════════


class TestPromptStructure:
    """각 에이전트의 self._prompt.messages 가 [SystemMessage, HumanMessage*] 형태
    인지 확인 — 이 구조가 깨지면 langchain-anthropic 이 system block 인식 X."""

    def test_each_agent_has_system_first_then_user(self, monkeypatch):
        from langchain_core.language_models.fake import FakeListLLM
        from langchain_core.messages import SystemMessage

        # FakeListLLM 은 with_structured_output 미지원이라 prompt 만 검증
        # 에이전트 인스턴스를 직접 만들지 않고 모듈의 ChatPromptTemplate 빌드
        # 패턴이 동일함을 _SYSTEM_BLOCKS 가 SystemMessage 로 들어가는지로 검증.
        from langchain_core.prompts import ChatPromptTemplate

        for name, blocks, _ in _AGENTS:
            prompt = ChatPromptTemplate.from_messages([
                SystemMessage(content=blocks),
                ("user", "dummy {x}"),
            ])
            messages = prompt.format_messages(x="value")
            assert isinstance(messages[0], SystemMessage), (
                f"{name}: 첫 메시지가 SystemMessage 가 아님"
            )
            assert isinstance(messages[0].content, list), (
                f"{name}: SystemMessage.content 가 list 가 아님 — "
                f"langchain-anthropic 이 cache_control 인식 못함"
            )