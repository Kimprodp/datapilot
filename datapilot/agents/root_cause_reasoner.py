"""⑤ Root Cause Reasoner — 근본 원인 추론기.

④ Data Validator가 검증한 가설들을 종합해 "X → Y → Z" 형태의
인과관계 체인을 구성한다. 각 단계에 SQL 근거를 인용해
PM이 "왜 이 일이 일어났는가"를 한 눈에 이해할 수 있게 한다.

모델: Opus 4 — 여러 증거를 종합한 인과 추론은 깊은 추론이 필요.

환각 방어 3룰:
  1) 인과 단계마다 SQL 근거 인용 필수
  2) supported 가설 0개면 "원인 불명" 선언 (억지 체인 금지)
  3) unverified 가설은 인과 체인에서 제외, 별도 섹션으로 분리

Java 비유:
    @Service
    public class RootCauseReasonerService {
        private final ChatModel llm;  // Opus
        public RootCauseReport reason(...) { ... }
    }
"""

from __future__ import annotations

import json
from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

from datapilot.agents.bottleneck_detector import AnomalyItem
from datapilot.agents.data_validator import ValidationResult
from datapilot.agents.segmentation_analyzer import SegmentationReport
from datapilot.config import ANTHROPIC_API_KEY, MAX_TOKENS, OPUS_MODEL

# ──────────────────────────────────────────────────────────────────
# 출력 스키마 (Pydantic)
# ──────────────────────────────────────────────────────────────────


class CausalStep(BaseModel):
    """인과 체인의 단일 단계."""

    step: str = Field(description="이 단계에서 무슨 일이 발생했는지")
    evidence: str = Field(description="④ SQL 결과에서 인용한 근거")


class RootCause(BaseModel):
    """근본 원인 추론 결과."""

    chain: list[CausalStep] = Field(
        default_factory=list,
        description="인과 단계 배열. 원인 불명이면 빈 배열",
    )
    summary: str = Field(
        description="한 문장 요약 또는 '원인 불명'"
    )


class UnverifiedHypothesis(BaseModel):
    """검증되지 못한 가설 (추가 조사 필요)."""

    hypothesis: str = Field(description="가설 내용")
    required_data: str = Field(description="검증에 필요한 데이터 (자연어)")


class RootCauseReport(BaseModel):
    """Root Cause Reasoner 출력 전체."""

    anomaly: str = Field(description="대상 이상 지표명")
    root_cause: RootCause = Field(description="인과 체인 + 요약")
    additional_investigation: list[UnverifiedHypothesis] = Field(
        default_factory=list,
        description="추가 조사가 필요한 미검증 가설 목록",
    )


# ──────────────────────────────────────────────────────────────────
# 입력 준비 (순수 함수)
# ──────────────────────────────────────────────────────────────────


def prepare_input(
    anomaly: AnomalyItem,
    segmentation: SegmentationReport,
    validation_results: list[ValidationResult],
) -> dict[str, Any]:
    """⑤에 전달할 입력을 3상태로 분리한다.

    LLM이 프롬프트 상단에서 "supported가 몇 개인지"를 바로 인식해
    룰 2(원인 불명 판정)를 정확히 적용할 수 있도록 구조화한다.
    """
    return {
        "anomaly": anomaly.model_dump(),
        "segmentation": segmentation.model_dump(),
        "supported": [v for v in validation_results if v.status == "supported"],
        "rejected": [v for v in validation_results if v.status == "rejected"],
        "unverified": [v for v in validation_results if v.status == "unverified"],
    }


# ──────────────────────────────────────────────────────────────────
# 프롬프트
# ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
너는 게임 KPI 이상의 근본 원인을 인과 체인 형태로 추론하는 전문가다.

작업 원칙 (반드시 지킨다):
1. 근거 인용 필수: 인과 체인의 각 단계는 supported 가설의 SQL 증거를 \
직접 인용해야 한다. 근거 없는 단계는 만들지 않는다.
2. 원인 불명 선언: supported 가설이 하나도 없으면 인과 체인을 비우고 \
summary에 "원인 불명"을 명시한다. 억지로 인과 관계를 만들지 않는다.
3. unverified 분리: unverified 가설은 본문 인과 체인에 섞지 않고, \
additional_investigation 섹션에 별도로 기재한다. \
각 항목에 required_data를 그대로 유지한다. \
상위 3개까지만 포함하고, 괄호 안 부연 설명은 생략한다.

인과 체인은 "A 때문에 B가 발생했고, B 때문에 C가 발생했다" 형태로 구성한다. \
최종 단계는 반드시 원본 이상 지표로 수렴해야 한다.

출력 규칙:
- step: 한글 위주로 작성한다. 영문 코드명 대신 한글 지표명을 사용한다.
- evidence: 핵심 수치를 포함해 1문장 이내로 간결하게 작성한다.

출력은 반드시 지정된 JSON 스키마를 따른다."""

USER_PROMPT_TEMPLATE = """\
다음은 이상 분석 결과다.

[원본 이상 지표]
{anomaly_json}

[세그먼트 분석]
{segmentation_json}

[Supported 가설 (근거 있음)]
{supported_json}

[Rejected 가설 (반박됨)]
{rejected_json}

[Unverified 가설 (검증 불가)]
{unverified_json}

위 정보를 바탕으로 근본 원인 인과 체인을 구성하라. \
3가지 룰(근거 인용, 원인 불명 선언, unverified 분리)을 반드시 지켜야 한다."""


# ──────────────────────────────────────────────────────────────────
# Reasoner
# ──────────────────────────────────────────────────────────────────


class RootCauseReasoner:
    """근본 원인을 인과 체인으로 추론하는 에이전트.

    환각 인과 추론 3가지 차단 룰을 프롬프트에 명시하고,
    ``with_structured_output`` 으로 스키마를 강제한다.

    Java 비유::

        public RootCauseReasonerService(@Autowired ChatModel llm) {
            this.llm = llm;  // Opus
        }
    """

    def __init__(self, *, llm: BaseChatModel | None = None) -> None:
        if llm is None:
            llm = ChatAnthropic(
                model=OPUS_MODEL,
                api_key=ANTHROPIC_API_KEY,
                max_tokens=MAX_TOKENS,
                temperature=1.0,
                max_retries=3,
            )
        self._prompt = ChatPromptTemplate.from_messages([
            ("system", SYSTEM_PROMPT),
            ("user", USER_PROMPT_TEMPLATE),
        ])
        self._chain = self._prompt | llm.with_structured_output(
            RootCauseReport
        )

    def reason(
        self,
        anomaly: AnomalyItem,
        segmentation: SegmentationReport,
        validation_results: list[ValidationResult],
    ) -> RootCauseReport:
        """검증 결과를 종합해 근본 원인 인과 체인을 추론한다.

        Args:
            anomaly: ① 이 탐지한 이상 지표.
            segmentation: ② 의 세그먼트 분석 결과.
            validation_results: ④ 의 검증 결과 목록.

        Returns:
            RootCauseReport — 인과 체인 + 추가 조사 필요 목록.
        """
        prepared = prepare_input(anomaly, segmentation, validation_results)

        return self._chain.invoke({
            "anomaly_json": json.dumps(
                prepared["anomaly"], ensure_ascii=False,
            ),
            "segmentation_json": json.dumps(
                prepared["segmentation"], ensure_ascii=False,
            ),
            "supported_json": json.dumps(
                [v.model_dump() for v in prepared["supported"]],
                ensure_ascii=False,
            ),
            "rejected_json": json.dumps(
                [v.model_dump() for v in prepared["rejected"]],
                ensure_ascii=False,
            ),
            "unverified_json": json.dumps(
                [v.model_dump() for v in prepared["unverified"]],
                ensure_ascii=False,
            ),
        })