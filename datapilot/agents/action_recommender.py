"""⑥ Action Recommender — 액션 추천기.

⑤ Root Cause Reasoner가 추론한 인과 체인을 받아
PM이 즉시 실행할 수 있는 액션을 단기/중기로 구분해 제안한다.

파이프라인의 마지막 에이전트로, 이 에이전트의 출력이 PM에게 전달되는
최종 의사결정 문서가 된다.

모델: Sonnet 4 -- 원인이 주어진 상태에서 액션 변환은 구조화 중심 작업.

Java 비유:
    @Service
    public class ActionRecommenderService {
        private final ChatModel llm;
        public ActionPlan recommend(RootCauseReport report) { ... }
    }
"""

from __future__ import annotations

import json
from typing import Any, Literal

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

from datapilot.agents.root_cause_reasoner import RootCauseReport
from datapilot.config import ANTHROPIC_API_KEY, MAX_TOKENS, SONNET_MODEL

# ------------------------------------------------------------------
# 출력 스키마 (Pydantic)
# ------------------------------------------------------------------


class Action(BaseModel):
    """단일 액션 제안."""

    priority: Literal["urgent", "short_term", "mid_term"] = Field(
        description="우선순위: urgent(긴급) / short_term(단기) / mid_term(중기)",
    )
    title: str = Field(
        description="구체적 액션 (예: Android v1.2.3 핫픽스로 상점 진열 순서 롤백)",
    )
    effect: str = Field(description="예상 효과")
    effort: str = Field(description="필요 리소스 (예: Android 개발자 1명, 2일)")
    related_cause_step: str | None = Field(
        default=None,
        description="대응하는 인과 단계 (없으면 null)",
    )


class ActionPlan(BaseModel):
    """Action Recommender 출력 전체."""

    anomaly: str = Field(description="대상 이상 지표명")
    actions: list[Action] = Field(
        default_factory=list, description="추천 액션 목록"
    )
    note: str | None = Field(
        default=None, description="원인 불명 등 보조 메시지"
    )


# ------------------------------------------------------------------
# 입력 준비 (순수 함수)
# ------------------------------------------------------------------


def prepare_input(root_cause_report: RootCauseReport) -> dict[str, Any]:
    """⑥에 전달할 입력을 구성한다.

    ``is_unknown_cause`` 플래그로 LLM이 "원인 불명" 분기를
    빠르게 인식하도록 보조한다.
    """
    return {
        "anomaly": root_cause_report.anomaly,
        "root_cause": root_cause_report.root_cause.model_dump(),
        "additional_investigation": [
            i.model_dump()
            for i in root_cause_report.additional_investigation
        ],
        "is_unknown_cause": len(root_cause_report.root_cause.chain) == 0,
    }


# ------------------------------------------------------------------
# 프롬프트
# ------------------------------------------------------------------

SYSTEM_PROMPT = (
    "너는 게임 PM의 의사결정을 돕는 액션 제안 전문가다. "
    "인과 체인 형태의 근본 원인을 받아 실행 가능한 액션을 "
    "우선순위별로 구분해 제안한다.\n\n"
    "작업 원칙:\n"
    "1. 각 액션의 priority는 다음 3가지 중 하나로 지정한다:\n"
    "   - urgent: 즉시 실행해 손실을 막는 긴급 조치 (핫픽스, 롤백)\n"
    "   - short_term: 1주 이내 실행 가능한 보완 조치\n"
    "   - mid_term: 재발 방지/프로세스 개선\n"
    "2. 각 액션에 effect(예상 효과), effort(필요 리소스)를 반드시 채운다.\n"
    "3. 액션은 인과 체인의 각 단계와 직접 매핑되어야 한다. "
    "인과 체인에 없는 임의의 액션을 만들지 않는다.\n"
    "4. 원인 불명 케이스(chain이 비어있음)인 경우, "
    "additional_investigation을 단서로 데이터 수집 액션을 제안한다.\n"
    "5. urgent 액션은 최소 1개 이상 포함한다 "
    "(PM이 오늘 결정할 수 있도록).\n"
    "6. 액션은 추상적 지시('개선한다', '검토한다')가 아닌 "
    "구체적 행동('X를 Y로 롤백한다')으로 작성한다.\n"
    "7. title은 20자 내외로 간결하게 작성한다.\n"
    "8. 의미 있는 액션만 최대 5개까지 제안한다. 억지로 5개를 맞출 필요는 없다.\n\n"
    "출력은 반드시 지정된 JSON 스키마를 따른다."
)

USER_PROMPT_TEMPLATE = (
    "다음은 이상 지표의 근본 원인 분석 결과다.\n\n"
    "[이상 지표]\n"
    "{anomaly}\n\n"
    "[근본 원인 인과 체인]\n"
    "{root_cause_json}\n\n"
    "[추가 검토 필요 (검증 불가 가설)]\n"
    "{additional_investigation_json}\n\n"
    "[원인 불명 여부]\n"
    "{is_unknown_cause}\n\n"
    "위 정보를 바탕으로 단기/중기 액션을 제안하라. "
    "각 액션에 effect, effort, priority를 반드시 채워라."
)


# ------------------------------------------------------------------
# Recommender
# ------------------------------------------------------------------


class ActionRecommender:
    """인과 체인을 실행 가능한 액션으로 변환하는 에이전트.

    원인이 명확한 경우 -> 단기/중기 액션 제안.
    원인 불명인 경우 -> 데이터 수집 액션 제안.

    Java 비유::

        public ActionRecommenderService(@Autowired ChatModel llm) {
            this.llm = llm;
        }
    """

    def __init__(self, *, llm: BaseChatModel | None = None) -> None:
        if llm is None:
            llm = ChatAnthropic(
                model=SONNET_MODEL,
                api_key=ANTHROPIC_API_KEY,
                max_tokens=MAX_TOKENS,
                temperature=0.3,
            )
        self._prompt = ChatPromptTemplate.from_messages([
            ("system", SYSTEM_PROMPT),
            ("user", USER_PROMPT_TEMPLATE),
        ])
        self._chain = self._prompt | llm.with_structured_output(ActionPlan)

    def recommend(
        self,
        root_cause_report: RootCauseReport,
    ) -> ActionPlan:
        """인과 체인을 받아 실행 가능한 액션을 제안한다.

        Args:
            root_cause_report: ⑤ 의 근본 원인 추론 결과.

        Returns:
            ActionPlan -- 단기/중기 액션 목록 + 보조 메시지.
        """
        prepared = prepare_input(root_cause_report)

        return self._chain.invoke({
            "anomaly": prepared["anomaly"],
            "root_cause_json": json.dumps(
                prepared["root_cause"], ensure_ascii=False,
            ),
            "additional_investigation_json": json.dumps(
                prepared["additional_investigation"], ensure_ascii=False,
            ),
            "is_unknown_cause": str(prepared["is_unknown_cause"]),
        })