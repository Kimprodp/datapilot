"""① Bottleneck Detector — 게임 KPI 이상 지표 탐지기.

게임 KPI 시계열에서 "지금 이 게임에서 무엇이 이상한지"를 판단하고,
이상 지표 목록과 심각도를 다음 에이전트(② Segmentation Analyzer)로 넘긴다.

설계 원칙: 코드는 데이터 조회만 담당하고, 변화율 계산·잡음 필터링 등
모든 분석 판단은 LLM이 수행한다.

Java 비유:
    @Service
    public class BottleneckDetectorService {
        private final ChatModel llm;           // 생성자 주입
        
        public AnomalyReport detect(KpiSeries series) { ... }
    }
"""

from __future__ import annotations

import json
from typing import Any, Literal

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

from datapilot.config import ANTHROPIC_API_KEY, MAX_TOKENS, SONNET_MODEL

# ──────────────────────────────────────────────────────────────────
# 출력 스키마 (Pydantic)
# ──────────────────────────────────────────────────────────────────


class AnomalyItem(BaseModel):
    """단일 이상 지표."""

    metric: str = Field(
        description="이상이 감지된 지표 코드명 (예: revenue, d7_retention)",
    )
    metric_label: str = Field(
        description="UI 표시용 한글 지표명 (예: '인앱결제 매출 (revenue)')",
    )
    change: float = Field(
        description="변화율 수치 (예: -0.08은 8% 감소). 코드 레벨 비교용",
    )
    change_display: str = Field(
        description=(
            "UI 표시용 변화 텍스트. "
            "절대량 지표(매출, DAU)는 상대 변화율: '-11%'. "
            "비율 지표(리텐션, 성공률)는 절대값 비교: '28% -> 24%'"
        ),
    )
    comparison_detail: str = Field(
        description=(
            "비교 구간 포함 상세 텍스트. "
            "예: '-11.1% (직전 4일 평균 351,200 -> 최근 4일 평균 312,400)'"
        ),
    )
    severity: Literal["HIGH", "MEDIUM", "LOW"] = Field(
        description="심각도 3단계",
    )
    reasoning: str = Field(
        description=(
            "판정 근거 상세 텍스트. 다음 구조를 따른다: "
            "시점 명시 -> 변화 패턴 -> 다른 지표와의 대조. "
            "예: 'D-3(3/28) Android 배포 이후 매출이 지속 하락. "
            "정상 변동 범위(+/-5%)를 초과하며 4일 연속 하향 추세. "
            "같은 기간 DAU/세션 수는 정상 -> 유저 감소가 아닌 결제 단가 문제로 추정.'"
        ),
    )


class AnomalyReport(BaseModel):
    """Bottleneck Detector 출력 전체."""

    anomalies: list[AnomalyItem] = Field(
        default_factory=list, description="이상 지표 목록"
    )
    normal: list[str] = Field(
        default_factory=list, description="정상으로 판단된 지표명 목록"
    )


# ──────────────────────────────────────────────────────────────────
# 프롬프트
# ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
너는 게임 KPI 이상 탐지 전문가다. \
게임 PM에게 "지금 이 게임에서 무엇이 이상한지"를 정확하게 짚어주는 역할을 한다.

판단 원칙:
1. 단순히 숫자가 변한 것이 아니라, "정상 변동 범위를 벗어난 이상"만 보고한다.
2. 주말·월말·시즌 이벤트 종료 같은 주기적 패턴은 이상이 아니다.
3. 여러 지표가 같이 움직이는 경우, 원인이 되는 지표 하나만 이상으로 보고한다. \
(예: DAU가 감소하면 매출도 자연히 감소하므로, DAU만 이상이고 매출은 정상으로 분류)
4. 4~5% 감소라도 7일 이상 연속 하락하면 이상으로 분류한다. \
단일 스파이크보다 추세가 중요하다.
5. 결제 성공률 같은 안정성 지표는 5% 미만 변동이라도 이상으로 본다.

출력 형식 규칙:
- metric_label: 한글명 + 영문 코드. 예: "인앱결제 매출 (revenue)", "D7 리텐션 (d7_retention)"
- change_display: 절대량 지표(매출, DAU 등)는 상대 변화율("-11%"), \
비율 지표(리텐션, 성공률)는 절대값 비교("28% -> 24%")
- comparison_detail: 비교 구간 포함 상세. \
예: "-11.1% (직전 4일 평균 351,200 -> 최근 4일 평균 312,400)"
- severity: HIGH / MEDIUM / LOW 3단계만 사용
- reasoning: 다문장 상세 텍스트. 구조: 시점 명시 -> 변화 패턴 -> 다른 지표와의 대조

중요: 분석 과정을 텍스트로 작성하지 말고, 즉시 도구(tool)를 호출해 결과를 반환하라. \
모든 필드를 빠짐없이 채워야 한다."""

USER_PROMPT_TEMPLATE = """\
다음은 게임 {game_id}의 최근 {days}일 KPI 시계열이다.

{kpi_series_json}

이 시계열에서 이상이 의심되는 지표를 찾아라. 각 이상 지표에 대해:
- metric: 지표 코드명 (예: revenue)
- metric_label: 한글 표시명 (예: "인앱결제 매출 (revenue)")
- change: 변화율 수치 (예: -0.08)
- change_display: UI 표시용 변화 텍스트
- comparison_detail: 비교 구간 포함 상세
- severity: HIGH / MEDIUM / LOW
- reasoning: 판정 근거 상세 (시점 -> 패턴 -> 대조)

함께 "정상" 지표 목록도 반환하라."""


# ──────────────────────────────────────────────────────────────────
# Detector
# ──────────────────────────────────────────────────────────────────


class BottleneckDetector:
    """KPI 시계열에서 이상 지표를 탐지하는 에이전트.

    생성자에 ``llm``을 주입할 수 있어, 테스트에서는 FakeChatModel 등을
    넘겨 실제 API 호출 없이 동작을 검증할 수 있다 (생성자 주입 패턴).

    Java 비유::

        public BottleneckDetectorService(@Autowired ChatModel llm) {
            this.llm = llm;
        }
    """

    def __init__(self, *, llm: BaseChatModel | None = None) -> None:
        if llm is None:
            llm = ChatAnthropic(
                model=SONNET_MODEL,
                api_key=ANTHROPIC_API_KEY,
                max_tokens=MAX_TOKENS,
            )
        self._prompt = ChatPromptTemplate.from_messages([
            ("system", SYSTEM_PROMPT),
            ("user", USER_PROMPT_TEMPLATE),
        ])
        self._chain = self._prompt | llm.with_structured_output(AnomalyReport)

    def detect(self, kpi_series: dict[str, Any]) -> AnomalyReport:
        """KPI 시계열을 분석해 이상 지표를 탐지한다.

        Args:
            kpi_series: ``GameDataRepository.get_daily_kpi()`` 반환값.

        Returns:
            AnomalyReport — anomalies(이상 목록) + normal(정상 목록).
        """
        return self._chain.invoke({
            "game_id": kpi_series["game_id"],
            "days": len(kpi_series["daily"]),
            "kpi_series_json": json.dumps(
                kpi_series["daily"], ensure_ascii=False,
            ),
        })