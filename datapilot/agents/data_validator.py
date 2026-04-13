"""④ Data Validator — 가설 데이터 검증기.

③ Hypothesis Generator가 뽑은 가설을 실제 데이터로 검증해
supported / rejected / unverified 3상태로 판정한다.

6종 에이전트 중 **유일하게 Tool Use를 사용**한다.
LLM이 bind_tools로 SQL 실행 권한을 받아 직접 DB에 쿼리하고,
결과를 해석해 가설을 판정한다.

보안: LLM이 생성한 SQL에 대해 4중 방어를 적용한다.
  1) SELECT only 정규식
  2) 위험 키워드 블랙리스트 (DROP, DELETE, …)
  3) 테이블 화이트리스트
  4) 읽기 전용 연결 + 결과 행 수 제한

Java 비유:
    @Service
    public class DataValidatorService {
        private final ChatModel llm;
        private final GameDataRepository repo;
        // LLM이 Tool Use로 repo.executeReadonlySql()를 호출
    }
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool
from pydantic import BaseModel, Field

from datapilot.agents.hypothesis_generator import Hypothesis, HypothesisList
from datapilot.config import ANTHROPIC_API_KEY, MAX_TOKENS, SONNET_MODEL
from datapilot.repository.port import GameDataRepository

# ──────────────────────────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────────────────────────

#: 에이전트 루프 최대 라운드 (무한 루프 방어)
MAX_TOOL_ROUNDS = 5

_DANGEROUS_PATTERN = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|CREATE"
    r"|ATTACH|DETACH|COPY|EXPORT|IMPORT|LOAD|INSTALL|CALL|PRAGMA)\b",
    re.IGNORECASE,
)


# ──────────────────────────────────────────────────────────────────
# 출력 스키마 (Pydantic)
# ──────────────────────────────────────────────────────────────────


class ValidationResult(BaseModel):
    """단일 가설의 검증 결과."""

    hypothesis: str = Field(description="검증 대상 가설")
    status: Literal["supported", "rejected", "unverified"] = Field(
        description="판정 상태"
    )
    evidence: str | None = Field(
        default=None, description="판정 근거 (SQL 결과 요약)"
    )
    queries_run: list[str] = Field(
        default_factory=list, description="실행한 SQL 목록"
    )
    query_results: list[Any] = Field(
        default_factory=list, description="SQL 실행 결과"
    )
    required_data: str | None = Field(
        default=None, description="unverified일 때 필요한 데이터 설명"
    )


class _Verdict(BaseModel):
    """에이전트 내부용: LLM 분석 결과에서 판정을 추출."""

    status: Literal["supported", "rejected", "evidence_insufficient"] = Field(
        description="판정 상태"
    )
    evidence: str = Field(description="판정 근거 요약")


# ──────────────────────────────────────────────────────────────────
# 순수 함수: 가설 분류 (LLM 호출 없음)
# ──────────────────────────────────────────────────────────────────


def classify(
    hypothesis: Hypothesis,
    available_tables: set[str] | frozenset[str],
) -> Literal["verifiable", "unverifiable"]:
    """가설을 코드 레벨에서 verifiable/unverifiable로 분류한다.

    ③에서 각 가설에 넣어둔 ``required_tables`` 필드를 기반으로 한다.
    LLM 호출 없이 즉시 판정해 API 비용을 절감한다.

    ANY match: required_tables 중 하나라도 가용 테이블에 있으면
    verifiable로 판정한다. LLM이 외부 테이블을 섞어 넣어도
    가용 테이블만으로 부분 검증을 시도할 수 있다.
    """
    if not hypothesis.required_tables:
        return "unverifiable"
    if set(hypothesis.required_tables) & available_tables:
        return "verifiable"
    return "unverifiable"


# ──────────────────────────────────────────────────────────────────
# SQL 보안 유틸
# ──────────────────────────────────────────────────────────────────


def _extract_table_names(sql: str) -> set[str]:
    """SQL에서 FROM/JOIN 뒤에 오는 테이블명을 추출한다.

    따옴표/백틱으로 감싼 식별자도 추출하고,
    CTE alias(WITH ... AS)는 실제 테이블이 아니므로 제외한다.
    간이 정규식 기반이며 읽기 전용 연결이 최종 방어선이다.
    """
    # CTE alias 추출 (실제 테이블이 아님)
    cte_pattern = re.compile(r"\bWITH\s+(\w+)\s+AS\s*\(", re.IGNORECASE)
    cte_aliases = set(cte_pattern.findall(sql))

    # FROM/JOIN 뒤 테이블명 추출 (따옴표/백틱 포함)
    table_pattern = re.compile(
        r'\b(?:FROM|JOIN)\s+(?:"([^"]+)"|`([^`]+)`|(\w+))',
        re.IGNORECASE,
    )
    names: set[str] = set()
    for match in table_pattern.finditer(sql):
        name = match.group(1) or match.group(2) or match.group(3)
        if name:
            names.add(name)

    return names - cte_aliases


# ──────────────────────────────────────────────────────────────────
# 프롬프트
# ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
너는 게임 데이터 가설 검증 전문가다. \
주어진 가설을 검증하기 위해 SQL을 생성·실행하고, \
결과를 해석해 가설의 참/거짓을 판정한다.

작업 원칙:
1. execute_sql 도구를 사용해 필요한 SQL을 실행할 수 있다. \
SELECT만 허용되며, 주어진 가용 테이블 안에서만 조회한다.
2. 한 가설당 1~3회의 SQL 실행을 권장한다. 불필요한 반복 호출을 피한다.
3. SQL 실행 결과를 근거로 가설이 데이터에 의해 \
지지되는가(supported), 반박되는가(rejected)를 판정한다.
4. 판정 근거는 핵심 수치 1~2개만 인용해 1~2문장으로 간결하게 명시한다. \
한 문장이 50자를 넘지 않도록 한다.
5. 결과가 애매하거나 증거가 부족하면 "evidence_insufficient"로 판정한다.
6. rejected 판정 시 evidence에 새로운 가설이나 추가 검증 제안을 포함하지 않는다. \
오직 기각 근거만 간결하고 명확하게 서술한다.

분석이 끝나면 최종 판정과 근거를 텍스트로 서술하라."""

USER_PROMPT_TEMPLATE = """\
다음 가설을 검증하라.

[가설]
{hypothesis_text}

[근거로 제시된 이유]
{hypothesis_reasoning}

[검증에 필요한 테이블]
{required_tables}

[가용 테이블 스키마]
{available_schema_json}

execute_sql 도구를 사용해 필요한 SQL을 실행한 뒤, \
가설의 상태를 판정하고 근거를 서술하라."""

_VERDICT_SYSTEM = "가설 검증 분석 결과를 구조화하라."
_VERDICT_USER = "가설: {hypothesis}\n\n분석 결과:\n{analysis}"


# ──────────────────────────────────────────────────────────────────
# Validator
# ──────────────────────────────────────────────────────────────────


class DataValidator:
    """가설을 데이터로 검증하는 에이전트 (Tool Use).

    2단계 처리:
      1. 코드 레벨 분류 — ``classify()`` 로 verifiable/unverifiable 즉시 판정
      2. LLM + Tool Use — verifiable 가설만 SQL 생성·실행·결과 해석

    Java 비유::

        public DataValidatorService(
            @Autowired ChatModel llm,
            @Autowired GameDataRepository repo
        ) { ... }
    """

    def __init__(
        self,
        *,
        llm: BaseChatModel | None = None,
        repo: GameDataRepository,
    ) -> None:
        self._repo = repo
        base_llm = llm or ChatAnthropic(
            model=SONNET_MODEL,
            api_key=ANTHROPIC_API_KEY,
            max_tokens=MAX_TOKENS,
        )

        # 동적으로 갱신되는 화이트리스트 (validate 호출 시 설정)
        self._allowed_tables: frozenset[str] = frozenset()

        self._execute_sql_tool = self._build_tool()
        self._llm_with_tools = base_llm.bind_tools([self._execute_sql_tool])
        self._verdict_chain = (
            ChatPromptTemplate.from_messages([
                ("system", _VERDICT_SYSTEM),
                ("user", _VERDICT_USER),
            ])
            | base_llm.with_structured_output(_Verdict)
        )

    # ── Tool 생성 ─────────────────────────────────────────────

    def _build_tool(self):  # noqa: ANN202
        """execute_sql Tool을 생성한다. 4중 보안 적용."""
        repo = self._repo
        # 클로저 캡처: validate() 호출 시 _allowed_tables가 동적 갱신됨
        validator = self

        @tool
        def execute_sql(query: str) -> str:
            """DuckDB에서 SELECT 쿼리를 실행한다.

            결과는 JSON 문자열로 반환된다.
            SELECT가 아니거나 허용되지 않은 테이블 접근 시 에러를 반환한다.
            """
            # 1) SELECT only
            stripped = query.strip()
            if not re.match(r"^SELECT\s", stripped, re.IGNORECASE):
                return json.dumps(
                    {"error": "SELECT 쿼리만 허용됩니다"},
                    ensure_ascii=False,
                )

            # 1-b) 세미콜론 다중 쿼리 차단
            if ";" in stripped.rstrip(";").strip():
                return json.dumps(
                    {"error": "다중 쿼리(세미콜론)는 허용되지 않습니다"},
                    ensure_ascii=False,
                )

            # 2) 위험 키워드 블랙리스트
            if _DANGEROUS_PATTERN.search(query):
                return json.dumps(
                    {"error": "DDL/DML 키워드가 포함된 쿼리는 차단됩니다"},
                    ensure_ascii=False,
                )

            # 3) 테이블 화이트리스트
            tables = _extract_table_names(query)
            if not tables:
                return json.dumps(
                    {"error": "테이블명을 추출할 수 없는 쿼리입니다"},
                    ensure_ascii=False,
                )
            disallowed = tables - validator._allowed_tables
            if disallowed:
                return json.dumps(
                    {"error": f"허용되지 않은 테이블: {sorted(disallowed)}"},
                    ensure_ascii=False,
                )

            # 4) 실행 (read-only 연결 + max_rows 제한)
            try:
                rows = repo.execute_readonly_sql(query, max_rows=100)
                return json.dumps(rows, default=str, ensure_ascii=False)
            except RuntimeError:
                # DB 내부 정보(경로, 스키마 등) 노출 방지
                return json.dumps(
                    {"error": "쿼리 실행 중 오류가 발생했습니다"},
                    ensure_ascii=False,
                )

        return execute_sql

    # ── 메인 진입점 ───────────────────────────────────────────

    def validate(
        self,
        hypothesis_list: HypothesisList,
        available_schema: dict[str, Any],
    ) -> list[ValidationResult]:
        """가설 목록을 검증한다.

        Args:
            hypothesis_list: ③ 의 출력.
            available_schema: ``repo.get_available_schema()`` 반환값.

        Returns:
            각 가설에 대한 ValidationResult 리스트.
        """
        self._allowed_tables = frozenset(
            t["name"] for t in available_schema["tables"]
        )

        results: list[ValidationResult] = []
        for h in hypothesis_list.hypotheses:
            if classify(h, self._allowed_tables) == "unverifiable":
                results.append(ValidationResult(
                    hypothesis=h.hypothesis,
                    status="unverified",
                    required_data=h.required_data,
                ))
            else:
                results.append(
                    self._llm_validate(h, available_schema),
                )
        return results

    # ── LLM + Tool Use 검증 ──────────────────────────────────

    def _llm_validate(
        self,
        hypothesis: Hypothesis,
        available_schema: dict[str, Any],
    ) -> ValidationResult:
        """verifiable 가설 1개를 LLM + Tool Use로 검증한다."""
        user_content = USER_PROMPT_TEMPLATE.format(
            hypothesis_text=hypothesis.hypothesis,
            hypothesis_reasoning=hypothesis.reasoning,
            required_tables=", ".join(hypothesis.required_tables),
            available_schema_json=json.dumps(
                available_schema, ensure_ascii=False,
            ),
        )
        messages: list[Any] = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=user_content),
        ]

        queries_run: list[str] = []
        query_results: list[Any] = []
        response = None  # 방어적 초기화

        # 에이전트 루프: LLM이 execute_sql을 호출할 때마다 실행
        for _ in range(MAX_TOOL_ROUNDS):
            response = self._llm_with_tools.invoke(messages)
            messages.append(response)

            if not response.tool_calls:
                break

            for tc in response.tool_calls:
                sql = tc["args"].get("query", "")
                queries_run.append(sql)
                result_str = self._execute_sql_tool.invoke(tc["args"])
                try:
                    query_results.append(json.loads(result_str))
                except json.JSONDecodeError:
                    query_results.append({"raw": result_str})
                messages.append(
                    ToolMessage(content=result_str, tool_call_id=tc["id"]),
                )

        # 라운드 소진: LLM이 MAX_TOOL_ROUNDS 후에도 tool 호출을 시도한 경우
        if response and response.tool_calls:
            return ValidationResult(
                hypothesis=hypothesis.hypothesis,
                status="unverified",
                evidence="가용 데이터로 검증할 수 없습니다 (추가 데이터 필요)",
                queries_run=queries_run,
                query_results=query_results,
            )

        # 판정 추출: 에이전트 루프 종료 후 별도 structured output 호출
        analysis_text = response.content if response else ""
        verdict = self._verdict_chain.invoke({
            "hypothesis": hypothesis.hypothesis,
            "analysis": analysis_text,
        })

        status: Literal["supported", "rejected", "unverified"]
        if verdict.status == "evidence_insufficient":
            status = "unverified"
        else:
            status = verdict.status

        return ValidationResult(
            hypothesis=hypothesis.hypothesis,
            status=status,
            evidence=verdict.evidence,
            queries_run=queries_run,
            query_results=query_results,
        )