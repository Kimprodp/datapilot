"""③ Hypothesis Generator — 이상 원인 가설 발산기.

① 이상 지표 + ② 세그먼트 분석 결과를 받아 가능한 원인 가설을
폭넓게 발산한다. 각 가설에는 검증에 필요한 테이블 매핑을 포함해,
④ Data Validator가 LLM 호출 없이 verifiable/unverifiable을 분류하게 한다.

모델: Opus 4 — 가설 발산의 폭이 결과 품질을 좌우하므로 깊은 추론 필요.

Java 비유:
    @Service
    public class HypothesisGeneratorService {
        private final ChatModel llm;  // Opus
        public HypothesisList generate(...) { ... }
    }
"""

from __future__ import annotations

import json

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

from datapilot.agents.bottleneck_detector import AnomalyItem
from datapilot.agents.segmentation_analyzer import SegmentationReport
from datapilot.config import ANTHROPIC_API_KEY, MAX_TOKENS, OPUS_MODEL
from datapilot.repository.port import GameDataRepository

# ──────────────────────────────────────────────────────────────────
# 출력 스키마 (Pydantic)
# ──────────────────────────────────────────────────────────────────


class Hypothesis(BaseModel):
    """단일 가설."""

    hypothesis: str = Field(
        description="가설 제목 (예: Android 상점 UI 변경으로 프리미엄 패키지 노출 감소)"
    )
    reasoning: str = Field(
        description="이 가설이 그럴듯한 이유 한 문장"
    )
    required_tables: list[str] = Field(
        default_factory=list,
        description="검증에 필요한 가용 테이블명 배열. 가용 테이블 밖이면 빈 배열",
    )
    required_data: str | None = Field(
        default=None,
        description="가용 테이블 밖의 데이터가 필요할 때 자연어 설명",
    )


class HypothesisList(BaseModel):
    """Hypothesis Generator 출력 전체."""

    anomaly: str = Field(description="대상 이상 지표명")
    hypotheses: list[Hypothesis] = Field(
        default_factory=list, description="원인 가설 목록 (최대 5개)"
    )


# ──────────────────────────────────────────────────────────────────
# 프롬프트
# ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
너는 게임 KPI 이상 원인 가설 발산 전문가다. \
이상 지표와 세그먼트 집중 정보를 받아, \
가능한 원인 가설을 폭넓게 제안하는 역할을 한다.

판단 원칙:
1. 세그먼트가 집중된 경우("concentrated")와 전체 확산("spread")의 가설 분기가 다르다.
   - concentrated: 집중된 세그먼트에만 영향을 줄 수 있는 원인을 우선 탐색 \
(예: Android 전용 배포)
   - spread: 전체 유저에 영향을 줄 수 있는 원인을 우선 탐색 \
(예: 결제 시스템 장애, CDN 이슈)
2. 유력한 가설만 생성한다. 대부분 2~3개면 충분하며, \
5개를 채울 필요는 없다. 확신이 낮은 가설은 포함하지 않는다.
3. 각 가설에 대해 검증에 필요한 테이블 정보를 반드시 기재한다:
   - 가용 테이블로 부분 검증이라도 가능하면 반드시 required_tables에 해당 테이블명을 기재한다. \
외부 데이터가 추가로 필요하더라도 가용 테이블에 관련 데이터가 있으면 포함한다.
   - required_tables에는 가용 테이블 스키마의 테이블명을 정확히 복사한다. 추측하거나 변형하지 않는다.
   - 가용 테이블과 완전히 무관한 가설만 required_tables를 빈 배열로 두고, \
required_data에 자연어로 설명한다.
4. required_data가 가용 테이블 밖인 가설도 반드시 포함한다. \
PM이 "어떤 데이터를 추가 수집해야 하는지" 알 수 있게 해주는 것도 가치다.

출력은 반드시 지정된 JSON 스키마를 따른다."""

USER_PROMPT_TEMPLATE = """\
다음은 게임 {game_id}의 이상 분석 결과다.

[이상 지표]
{anomaly_json}

[세그먼트 분석]
{segmentation_json}

[가용 테이블 스키마]
{available_schema_json}

이 상황에서 가장 유력한 원인 가설을 도출하라 (필요한 경우 최대 5개 까지만). \
각 가설에 대해 "hypothesis", "reasoning", \
"required_tables", "required_data"(가용 테이블 밖인 경우에만) \
필드를 반드시 포함하라."""


# ──────────────────────────────────────────────────────────────────
# Generator
# ──────────────────────────────────────────────────────────────────


class HypothesisGenerator:
    """이상 원인 가설을 폭넓게 발산하는 에이전트.

    가용 테이블 스키마를 프롬프트에 사전 주입해,
    각 가설에 ``required_tables`` / ``required_data`` 를 포함시킨다.
    이를 통해 ④ Data Validator가 LLM 호출 없이 코드 레벨에서
    verifiable/unverifiable을 분류할 수 있다.

    Java 비유::

        public HypothesisGeneratorService(@Autowired ChatModel llm) {
            this.llm = llm;  // Opus
        }
    """

    def __init__(self, *, llm: BaseChatModel | None = None) -> None:
        if llm is None:
            llm = ChatAnthropic(
                model=OPUS_MODEL,
                api_key=ANTHROPIC_API_KEY,
                max_tokens=MAX_TOKENS,
            )
        self._prompt = ChatPromptTemplate.from_messages([
            ("system", SYSTEM_PROMPT),
            ("user", USER_PROMPT_TEMPLATE),
        ])
        self._chain = self._prompt | llm.with_structured_output(HypothesisList)

    def generate(
        self,
        game_id: str,
        anomaly: AnomalyItem,
        segmentation: SegmentationReport,
        repo: GameDataRepository,
    ) -> HypothesisList:
        """가설을 발산한다.

        Args:
            game_id: 게임 식별자.
            anomaly: ① 이 탐지한 이상 지표.
            segmentation: ② 의 세그먼트 분석 결과.
            repo: 가용 스키마 조회용 Port.

        Returns:
            HypothesisList — 최대 5개 가설 목록.
        """
        available_schema = repo.get_available_schema(game_id)

        return self._chain.invoke({
            "game_id": game_id,
            "anomaly_json": anomaly.model_dump_json(),
            "segmentation_json": segmentation.model_dump_json(),
            "available_schema_json": json.dumps(
                available_schema, ensure_ascii=False,
            ),
        })