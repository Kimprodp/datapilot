"""② Segmentation Analyzer — 이상 지표 세그먼트 분해기.

① Bottleneck Detector가 찾아낸 이상 지표를 세그먼트 차원
(플랫폼·국가·신규/기존·디바이스)으로 분해해
"어디에 집중되어 있는가"를 식별한다.

설계 원칙:
- 코드는 사용 가능한 차원을 자동 탐지하고 raw 시계열만 조회
- 변화율 계산, 집중 차원 식별, 요약 생성은 전적으로 LLM

Java 비유:
    @Service
    public class SegmentationAnalyzerService {
        private final ChatModel llm;
        private final GameDataRepository repo;  // analyze 호출 시 주입
        public SegmentationReport analyze(...) { ... }
    }
"""

from __future__ import annotations

import json
from datetime import date
from typing import Literal

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

from datapilot.agents.bottleneck_detector import AnomalyItem
from datapilot.config import ANTHROPIC_API_KEY, SONNET_MODEL
from datapilot.repository.port import GameDataRepository

# ──────────────────────────────────────────────────────────────────
# 출력 스키마 (Pydantic)
# ──────────────────────────────────────────────────────────────────


class SegmentConcentration(BaseModel):
    """가장 집중된 세그먼트 정보."""

    dimension: str = Field(description="집중 차원 (예: platform, country)")
    focus: str = Field(description="집중 세그먼트 값 (예: android, brazil)")
    change: float = Field(description="해당 세그먼트의 변화율 (예: -0.15)")


class SegmentationReport(BaseModel):
    """Segmentation Analyzer 출력 전체."""

    anomaly: str = Field(description="분석 대상 이상 지표명")
    concentration: SegmentConcentration = Field(
        description="가장 집중된 세그먼트"
    )
    breakdown: dict[str, dict[str, float]] = Field(
        description="전체 차원별 세그먼트 변화율"
    )
    summary: str = Field(
        description="가설 발산 힌트가 되는 요약 문장"
    )
    spread_type: Literal["concentrated", "spread", "crossed"] = Field(
        description="집중형 / 확산형 / 교차형"
    )


# ──────────────────────────────────────────────────────────────────
# 프롬프트
# ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
너는 게임 데이터 세그먼트 분석 전문가다. \
이상 지표가 전체 유저에게 고르게 퍼져 있는지, \
아니면 특정 세그먼트에 집중되어 있는지를 정확하게 판단하는 역할을 한다.

판단 원칙:
1. 각 세그먼트 차원(플랫폼·국가·신규/기존·디바이스)별로 변화율을 계산하고, \
"가장 집중된 차원"을 식별한다.
2. 한 세그먼트만 영향받고 나머지가 정상이면 → 집중형(concentrated)
3. 모든 세그먼트가 고르게 영향받으면 → 확산형(spread)
4. 두 차원이 교차하여 영향받는 경우(예: "신규 유저의 Android")도 포착한다.
5. summary는 다음 형식을 따른다:
   - 집중형: "{지표} 감소가 {세그먼트}에 집중 ({변화율}, {나머지}는 {수치}로 정상)"
     예: "매출 감소가 Android에 집중 (-18%, iOS는 +0.2%로 정상)"
   - 확산형: "{지표} 감소가 특정 세그먼트에 집중되지 않음 (전반적 하락)"
   구체적 수치를 반드시 포함한다.

출력은 반드시 지정된 JSON 스키마를 따른다."""

USER_PROMPT_TEMPLATE = """\
다음은 게임 {game_id}의 이상 지표 "{metric}"에 대한 세그먼트별 raw 시계열이다.

{segmented_json}

이 데이터에서 이상이 어떤 세그먼트에 집중되어 있는지 판단하라. \
각 세그먼트의 변화율을 계산하고, \
"집중된 차원과 값", "전체 분해 결과", \
"가설 발산 힌트가 되는 요약 문장"을 반환하라."""


# ──────────────────────────────────────────────────────────────────
# Analyzer
# ──────────────────────────────────────────────────────────────────


class SegmentationAnalyzer:
    """이상 지표를 세그먼트 차원으로 분해하는 에이전트.

    ``analyze()``가 호출될 때마다 Port/Adapter를 통해 세그먼트 시계열을
    조회하고, LLM에게 집중 차원 식별을 위임한다.

    Java 비유::

        public SegmentationAnalyzerService(@Autowired ChatModel llm) {
            this.llm = llm;
        }
    """

    def __init__(self, *, llm: BaseChatModel | None = None) -> None:
        if llm is None:
            llm = ChatAnthropic(
                model=SONNET_MODEL,
                api_key=ANTHROPIC_API_KEY,
            )
        self._prompt = ChatPromptTemplate.from_messages([
            ("system", SYSTEM_PROMPT),
            ("user", USER_PROMPT_TEMPLATE),
        ])
        self._chain = self._prompt | llm.with_structured_output(
            SegmentationReport
        )

    def analyze(
        self,
        game_id: str,
        anomaly: AnomalyItem,
        period: tuple[date, date],
        repo: GameDataRepository,
    ) -> SegmentationReport:
        """이상 지표를 세그먼트 차원으로 분해 분석한다.

        Args:
            game_id: 게임 식별자.
            anomaly: ① Bottleneck Detector가 탐지한 이상 지표.
            period: (시작일, 종료일) inclusive.
            repo: 데이터 조회용 Port.

        Returns:
            SegmentationReport — 집중 차원 + 전체 분해 + 요약.
        """
        # 1. 사용 가능한 차원 자동 탐지
        dimensions = repo.get_available_dimensions(game_id)

        # 2. 모든 차원에 대해 세그먼트별 시계열 조회
        segmented = repo.get_metric_by_segments(
            game_id=game_id,
            metric=anomaly.metric,
            period=period,
            dimensions=dimensions,
        )

        # 3. LLM에게 집중 차원 식별 요청
        return self._chain.invoke({
            "game_id": game_id,
            "metric": anomaly.metric,
            "segmented_json": json.dumps(segmented, ensure_ascii=False),
        })