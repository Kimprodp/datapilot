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

from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from datapilot.agents.bottleneck_detector import AnomalyItem
from datapilot.agents.segmentation_analyzer import SegmentationReport
from datapilot.config import ANTHROPIC_API_KEY, MAX_TOKENS, OPUS_MODEL
from datapilot.domain import GAME
from datapilot.domain.base import DomainKeywords
from datapilot.observability import NULL_METRICS
from datapilot.repository.port import DataRepository

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
너는 KPI 이상 원인 가설 발산 전문가다. \
이상 지표와 세그먼트 집중 정보를 받아, \
가능한 원인 가설을 폭넓게 제안하는 역할을 한다.

판단 원칙:
1. 세그먼트가 집중된 경우("concentrated")와 전체 확산("spread")의 가설 분기가 다르다.
   - concentrated: 집중된 세그먼트에만 영향을 줄 수 있는 원인을 우선 탐색 \
(예: Android 전용 배포, 특정 국가 PG 장애)
   - spread: 전체 유저에 영향을 줄 수 있는 원인을 우선 탐색 \
(예: 결제 시스템 장애, CDN 이슈, 인기 상품 품절)
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

[가설 발산 — 다양한 각도 시도]
KPI 변동의 원인은 보통 다음 카테고리 중 하나 이상에 속한다. 다만 \
이에 한정하지 말고 가용 스키마와 도메인 맥락이 시사하는 다른 카테고리도 자유롭게 발산하라:
- 외부 시스템 (결제 게이트웨이, 마케팅 채널, 외부 API)
- 제품/UI 변경 (빌드 배포, 진열 순서, UX 변경, 가격 조정)
- 재고/공급 (인기 상품 품절, 카테고리 단종, 신상품 미입고)
- 사용자 세그먼트 변화 (특정 그룹 이탈, 신규 유입 채널 변화)
- 프로모션/이벤트 (캠페인 종료, 시즌 이벤트 종료, 할인 종료)
- 시간/계절 효과 (주말/휴일, 계절성)

[가용 스키마 적극 활용]
가용 테이블 스키마의 description 필드를 자세히 읽어라. 각 테이블이 명시한 \
"분석 활용도" (예: "재고 부족 / 품절 영향 분석에 활용", "프로모션 시작/종료 시점과 매출 변동 연관 분석") \
가 가설 발산의 가장 강한 단서다. description 이 시사하는 분석 방향에서 가설을 적극 시도하라.

출력은 반드시 지정된 JSON 스키마를 따른다."""

USER_PROMPT_TEMPLATE = """\
[분석 도메인]
역할: {role_descriptor}
핵심 지표: {primary_kpis}

다음은 {persona} 가 운영하는 {entity_id} 의 이상 분석 결과다.

[이상 지표]
{anomaly_json}

[세그먼트 분석]
{segmentation_json}

이 상황에서 가장 유력한 원인 가설을 도출하라 (필요한 경우 최대 5개 까지만). \
각 가설에 대해 "hypothesis", "reasoning", \
"required_tables", "required_data"(가용 테이블 밖인 경우에만) \
필드를 반드시 포함하라."""


def build_system_content(light_schema: dict[str, Any]) -> list[dict[str, Any]]:
    """SYSTEM_PROMPT + 가용 스키마를 합친 system content blocks.

    Anthropic Prompt Caching 임계 1024 토큰을 채우기 위해 user 메시지의
    정적 prefix (가용 테이블 스키마) 를 system 으로 옮긴다.
    한 게임 분석 1회 안에서 schema 가 동일하므로 N=2~3 anomaly 호출 모두
    cache_read 효과를 받는다.
    """
    schema_text = json.dumps(light_schema, ensure_ascii=False)
    return [
        {"type": "text", "text": SYSTEM_PROMPT},
        {
            "type": "text",
            "text": f"\n\n[가용 테이블 스키마]\n{schema_text}",
            "cache_control": {"type": "ephemeral"},
        },
    ]


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

    def __init__(
        self,
        *,
        llm: BaseChatModel | None = None,
        domain_keywords: DomainKeywords | None = None,
    ) -> None:
        # 다음 task 의 user template placeholder 추가에서 활용 예정
        self._domain_keywords = domain_keywords
        if llm is None:
            llm = ChatAnthropic(
                model=OPUS_MODEL,
                api_key=ANTHROPIC_API_KEY,
                max_tokens=MAX_TOKENS,
                temperature=1.0,
                max_retries=3,
            )
        # 가용 스키마가 system 으로 들어가면서 ChatPromptTemplate 의 정적 빌드가
        # 부적합해짐 → invoke 시점에 messages 직접 구성.
        self._llm = llm.with_structured_output(HypothesisList)

    def generate(
        self,
        entity_id: str,
        anomaly: AnomalyItem,
        segmentation: SegmentationReport,
        repo: DataRepository,
        *,
        metrics: BaseCallbackHandler | None = None,
    ) -> HypothesisList:
        """가설을 발산한다.

        Args:
            entity_id: 분석 대상 식별자 (게임 ID / 스토어 ID 등).
            anomaly: ① 이 탐지한 이상 지표.
            segmentation: ② 의 세그먼트 분석 결과.
            repo: 가용 스키마 조회용 Port.
            metrics: LLM 호출 usage 측정용 callback. None 이면 no-op.

        Returns:
            HypothesisList — 최대 5개 가설 목록.
        """
        metrics = metrics or NULL_METRICS
        full_schema = repo.get_available_schema(entity_id)
        # 가설 생성에는 테이블명 + 설명만 전달 (토큰 절감)
        # 컬럼 상세는 ④ Data Validator가 SQL 작성 시 사용
        light_schema = {
            "tables": [
                {"name": t["name"], "description": t.get("description", "")}
                for t in full_schema["tables"]
            ]
        }

        kw = self._domain_keywords or GAME.agent_keywords
        messages = [
            SystemMessage(content=build_system_content(light_schema)),
            HumanMessage(content=USER_PROMPT_TEMPLATE.format(
                role_descriptor=kw.role_descriptor,
                primary_kpis=", ".join(kw.primary_kpis),
                persona=kw.persona,
                entity_id=entity_id,
                anomaly_json=anomaly.model_dump_json(),
                segmentation_json=segmentation.model_dump_json(),
            )),
        ]
        return self._llm.invoke(
            messages,
            config={"callbacks": [metrics]},
        )