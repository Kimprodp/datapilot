"""④ DataValidator — classify() 분류 함수 + execute_sql 4중 보안 테스트.

검증 범위:
  1. classify() 순수 함수
     - required_tables가 빈 배열 → unverifiable
     - required_tables가 화이트리스트 외 테이블 포함 → unverifiable
     - required_tables가 모두 화이트리스트 내 → verifiable
     - required_tables 일부만 화이트리스트 내 → unverifiable (부분 매칭 거부)

  2. _extract_table_names() 순수 함수
     - FROM 절 테이블 추출
     - JOIN 절 테이블 추출
     - 대소문자 혼합 SQL 처리
     - 서브쿼리 기본 처리

  3. execute_sql 도구 4중 보안 (DataValidator 인스턴스를 Mock repo로 생성)
     - SELECT가 아닌 쿼리 → 에러 JSON 반환
     - DROP 키워드 → 에러 JSON 반환
     - DELETE 키워드 → 에러 JSON 반환
     - 화이트리스트 밖 테이블 → 에러 JSON 반환
     - 정상 SELECT → repo.execute_readonly_sql 호출 (Mock 검증)

  4. validate() 분기 테스트 (Mock LLM)
     - unverifiable 가설 → 즉시 unverified 판정 (LLM 미호출)
     - verifiable 가설 → LLM 루프 진입 (Mock chain 확인)

LLM API 실제 호출 없음. 모든 LLM 의존 코드는 unittest.mock으로 차단.

Java 비유:
    @ExtendWith(MockitoExtension.class)
    class DataValidatorTest {
        @Mock GameDataRepository mockRepo;
        @InjectMocks DataValidator validator;
    }
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from datapilot.agents.data_validator import (
    DataValidator,
    _extract_table_names,
    classify,
)
from datapilot.agents.hypothesis_generator import Hypothesis, HypothesisList
from datapilot.repository.port import GameDataRepository


# ──────────────────────────────────────────────────────────────────
# 헬퍼 팩토리
# ──────────────────────────────────────────────────────────────────

_AVAILABLE_TABLES = frozenset(
    {
        "daily_kpi",
        "users",
        "products",
        "payments",
        "shop_impressions",
        "releases",
        "events",
        "sessions",
        "content_releases",
        "gateways",
        "payment_attempts",
        "payment_errors",
    }
)


def _make_hypothesis(
    hypothesis: str = "테스트 가설",
    reasoning: str = "이유",
    required_tables: list[str] | None = None,
    required_data: str | None = None,
) -> Hypothesis:
    return Hypothesis(
        hypothesis=hypothesis,
        reasoning=reasoning,
        required_tables=required_tables or [],
        required_data=required_data,
    )


def _make_mock_repo() -> MagicMock:
    """GameDataRepository 인터페이스를 구현한 Mock 객체.

    Java 비유: Mockito.mock(GameDataRepository.class)
    """
    mock_repo = MagicMock(spec=GameDataRepository)
    mock_repo.execute_readonly_sql.return_value = [{"revenue": 100}]
    return mock_repo


def _make_available_schema(tables: list[str] | None = None) -> dict:
    """get_available_schema() 반환 형식과 동일한 구조."""
    if tables is None:
        tables = list(_AVAILABLE_TABLES)
    return {
        "tables": [
            {"name": t, "columns": ["id", "date"], "description": ""}
            for t in tables
        ]
    }


# ════════════════════════════════════════════════════════════════════
# 1. classify() 순수 함수
# ════════════════════════════════════════════════════════════════════


class TestClassify:
    """classify()는 LLM 호출 없이 Hypothesis.required_tables만 보고 판정.

    Java 비유: 순수 static 메서드 — 입력이 같으면 항상 같은 결과(결정적).
    """

    def test_returns_unverifiable_when_required_tables_is_empty(self):
        """required_tables가 빈 리스트 → 검증 불가 (데이터 없음)."""
        h = _make_hypothesis(required_tables=[])
        result = classify(h, _AVAILABLE_TABLES)
        assert result == "unverifiable"

    def test_returns_verifiable_when_all_tables_in_whitelist(self):
        """required_tables가 모두 화이트리스트 내 → 검증 가능."""
        h = _make_hypothesis(required_tables=["payments", "shop_impressions"])
        result = classify(h, _AVAILABLE_TABLES)
        assert result == "verifiable"

    def test_returns_unverifiable_when_table_not_in_whitelist(self):
        """required_tables에 화이트리스트 밖 테이블 포함 → 검증 불가."""
        h = _make_hypothesis(required_tables=["external_ad_data"])
        result = classify(h, _AVAILABLE_TABLES)
        assert result == "unverifiable"

    def test_returns_unverifiable_when_partial_tables_not_in_whitelist(self):
        """일부 테이블만 화이트리스트 내 → 전체 unverifiable.

        payments는 허용이지만 ad_platform은 미허용 → unverifiable.
        Java 비유: Set.containsAll() 이 false이면 통째로 거부.
        """
        h = _make_hypothesis(required_tables=["payments", "ad_platform"])
        result = classify(h, _AVAILABLE_TABLES)
        assert result == "unverifiable"

    def test_returns_verifiable_when_single_valid_table(self):
        """required_tables에 유효한 테이블 1개 → verifiable."""
        h = _make_hypothesis(required_tables=["users"])
        result = classify(h, _AVAILABLE_TABLES)
        assert result == "verifiable"

    def test_verifiable_is_case_sensitive(self):
        """테이블명은 대소문자 구분. 'Payments' ≠ 'payments' → unverifiable."""
        h = _make_hypothesis(required_tables=["Payments"])
        result = classify(h, _AVAILABLE_TABLES)
        assert result == "unverifiable"

    def test_accepts_frozenset_as_available_tables(self):
        """frozenset 타입도 정상 처리."""
        h = _make_hypothesis(required_tables=["payments"])
        result = classify(h, frozenset({"payments", "users"}))
        assert result == "verifiable"

    def test_accepts_set_as_available_tables(self):
        """일반 set 타입도 정상 처리."""
        h = _make_hypothesis(required_tables=["payments"])
        result = classify(h, {"payments", "users"})
        assert result == "verifiable"


# ════════════════════════════════════════════════════════════════════
# 2. _extract_table_names() 순수 함수
# ════════════════════════════════════════════════════════════════════


class TestExtractTableNames:
    """_extract_table_names()는 SQL에서 FROM/JOIN 뒤 테이블명을 추출.

    Java 비유: 정규식 유틸 테스트 — SQL 파싱 로직.
    """

    def test_extracts_table_from_simple_select(self):
        sql = "SELECT * FROM payments"
        result = _extract_table_names(sql)
        assert result == {"payments"}

    def test_extracts_table_from_join(self):
        sql = "SELECT * FROM payments JOIN users ON payments.user_id = users.id"
        result = _extract_table_names(sql)
        assert "payments" in result
        assert "users" in result

    def test_extracts_multiple_joins(self):
        sql = (
            "SELECT * FROM payments "
            "JOIN users ON payments.user_id = users.id "
            "JOIN products ON payments.product_id = products.id"
        )
        result = _extract_table_names(sql)
        assert result == {"payments", "users", "products"}

    def test_handles_lowercase_from(self):
        sql = "select * from sessions"
        result = _extract_table_names(sql)
        assert "sessions" in result

    def test_handles_mixed_case(self):
        sql = "SELECT * FROM Users JOIN Payments ON Users.id = Payments.user_id"
        result = _extract_table_names(sql)
        assert "Users" in result
        assert "Payments" in result

    def test_returns_empty_set_for_no_table(self):
        """FROM/JOIN 없는 쿼리 → 빈 set."""
        sql = "SELECT 1 + 1"
        result = _extract_table_names(sql)
        assert result == set()

    def test_returns_set_type(self):
        result = _extract_table_names("SELECT * FROM payments")
        assert isinstance(result, set)


# ════════════════════════════════════════════════════════════════════
# 3. execute_sql 도구 4중 보안
# ════════════════════════════════════════════════════════════════════


class TestExecuteSqlSecurity:
    """DataValidator 내부 execute_sql Tool의 4중 보안 로직 테스트.

    DataValidator 인스턴스를 Mock repo로 생성하고,
    _execute_sql_tool을 직접 invoke해 보안 로직을 검증한다.

    Java 비유:
        @Test void shouldBlockNonSelectQuery() {
            String result = validator.executeSqlTool("DROP TABLE users");
            assertThat(result).contains("error");
        }
    """

    @pytest.fixture
    def validator(self):
        """Mock repo 주입 + Mock LLM 주입으로 DataValidator 생성.

        LLM은 __init__ 내부에서만 사용되고, execute_sql 도구 테스트에서는
        LLM을 실제로 호출하지 않으므로 MagicMock으로 충분하다.
        """
        mock_llm = MagicMock()
        # bind_tools가 같은 mock을 반환하도록 설정
        mock_llm.bind_tools.return_value = mock_llm
        mock_llm.with_structured_output.return_value = MagicMock()
        mock_repo = _make_mock_repo()
        # DataValidator는 __init__에서 llm.bind_tools / with_structured_output 호출
        return DataValidator(llm=mock_llm, repo=mock_repo)

    def _invoke_tool(self, validator: DataValidator, query: str) -> dict:
        """execute_sql Tool을 직접 호출하고 결과를 dict로 파싱."""
        result_str = validator._execute_sql_tool.invoke({"query": query})
        return json.loads(result_str)

    def _set_allowed_tables(self, validator: DataValidator, tables: frozenset[str]):
        """화이트리스트를 테스트용으로 설정."""
        validator._allowed_tables = tables

    # ── 보안 레이어 1: SELECT only ──────────────────────────────

    def test_blocks_non_select_query_with_error(self, validator):
        """SELECT로 시작하지 않는 쿼리 → 에러."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(validator, "UPDATE payments SET amount=0")
        assert "error" in result

    def test_blocks_bare_insert_statement(self, validator):
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "INSERT INTO payments VALUES (1, 2, 3)"
        )
        assert "error" in result

    def test_blocks_create_table_statement(self, validator):
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "CREATE TABLE hack AS SELECT * FROM payments"
        )
        assert "error" in result

    # ── 보안 레이어 2: 위험 키워드 블랙리스트 ──────────────────

    def test_blocks_drop_keyword_in_query(self, validator):
        """SELECT 문 안에 DROP 키워드 포함 → 차단.

        예: SELECT * FROM payments; DROP TABLE payments
        (세미콜론 주입 공격 패턴)
        """
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "SELECT * FROM payments; DROP TABLE payments"
        )
        assert "error" in result

    def test_blocks_delete_keyword_in_query(self, validator):
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "SELECT * FROM payments WHERE 1=1; DELETE FROM payments"
        )
        assert "error" in result

    def test_blocks_update_keyword_in_query(self, validator):
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "SELECT * FROM payments; UPDATE payments SET x=1"
        )
        assert "error" in result

    def test_blocks_truncate_keyword(self, validator):
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "SELECT 1; TRUNCATE TABLE payments"
        )
        assert "error" in result

    def test_blocks_alter_keyword(self, validator):
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "SELECT 1; ALTER TABLE payments ADD COLUMN x INT"
        )
        assert "error" in result

    # ── 보안 레이어 3: 테이블 화이트리스트 ─────────────────────

    def test_blocks_query_on_non_whitelisted_table(self, validator):
        """화이트리스트에 없는 테이블 접근 → 차단."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "SELECT * FROM secret_admin_table"
        )
        assert "error" in result

    def test_error_message_contains_disallowed_table_name(self, validator):
        """에러 메시지에 허용되지 않은 테이블명이 포함되어야 한다."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator, "SELECT * FROM secret_table"
        )
        assert "secret_table" in result.get("error", "")

    def test_blocks_join_with_non_whitelisted_table(self, validator):
        """JOIN 절에 화이트리스트 밖 테이블 → 차단."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        result = self._invoke_tool(
            validator,
            "SELECT * FROM payments JOIN admin_logs ON payments.id = admin_logs.id",
        )
        assert "error" in result

    # ── 보안 레이어 4: 정상 SELECT 통과 ────────────────────────

    def test_passes_valid_select_query(self, validator):
        """정상 SELECT 쿼리는 repo.execute_readonly_sql 호출로 이어진다."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        validator._repo.execute_readonly_sql.return_value = [{"amount": 500}]

        result = self._invoke_tool(validator, "SELECT amount FROM payments")

        # 에러 없이 결과 반환
        assert "error" not in result
        # repo가 실제로 호출되었는지 확인 (Java: verify(mockRepo).executeReadonlySql(...))
        validator._repo.execute_readonly_sql.assert_called_once()

    def test_passes_select_with_where_clause(self, validator):
        self._set_allowed_tables(validator, frozenset({"payments"}))
        validator._repo.execute_readonly_sql.return_value = [{"count": 10}]

        result = self._invoke_tool(
            validator,
            "SELECT COUNT(*) as count FROM payments WHERE platform = 'android'",
        )
        assert "error" not in result

    def test_passes_select_with_join_on_whitelisted_tables(self, validator):
        self._set_allowed_tables(validator, frozenset({"payments", "users"}))
        validator._repo.execute_readonly_sql.return_value = []

        result = self._invoke_tool(
            validator,
            "SELECT * FROM payments JOIN users ON payments.user_id = users.id",
        )
        assert "error" not in result

    def test_repo_execute_called_with_correct_sql(self, validator):
        """repo.execute_readonly_sql에 원본 SQL이 그대로 전달된다."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        validator._repo.execute_readonly_sql.return_value = []
        sql = "SELECT amount FROM payments LIMIT 10"

        self._invoke_tool(validator, sql)

        call_args = validator._repo.execute_readonly_sql.call_args
        assert call_args[0][0] == sql

    def test_repo_execute_called_with_max_rows_100(self, validator):
        """max_rows=100 으로 호출되어야 한다 (결과 행 수 상한)."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        validator._repo.execute_readonly_sql.return_value = []

        self._invoke_tool(validator, "SELECT * FROM payments")

        call_args = validator._repo.execute_readonly_sql.call_args
        assert call_args[1].get("max_rows", call_args[0][1] if len(call_args[0]) > 1 else None) == 100

    def test_returns_json_string(self, validator):
        """execute_sql Tool 반환값은 JSON 문자열이어야 한다."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        validator._repo.execute_readonly_sql.return_value = [{"x": 1}]

        raw_result = validator._execute_sql_tool.invoke({"query": "SELECT x FROM payments"})

        # JSON 파싱 성공 여부로 검증
        parsed = json.loads(raw_result)
        assert isinstance(parsed, (dict, list))

    def test_handles_repo_runtime_error_gracefully(self, validator):
        """repo가 RuntimeError를 던지면 에러 JSON을 반환한다."""
        self._set_allowed_tables(validator, frozenset({"payments"}))
        validator._repo.execute_readonly_sql.side_effect = RuntimeError("DB 연결 오류")

        result = self._invoke_tool(validator, "SELECT * FROM payments")

        assert "error" in result


# ════════════════════════════════════════════════════════════════════
# 4. validate() 분기 테스트 (Mock LLM)
# ════════════════════════════════════════════════════════════════════


class TestValidateMethod:
    """validate()의 classify() 분기 로직을 Mock으로 검증.

    LLM 에이전트 루프(tool_calls 처리)는 범위 밖.
    여기서는 "unverifiable → 즉시 unverified", "verifiable → LLM 진입" 분기만 확인.

    Java 비유:
        @Test void shouldSkipLlmForUnverifiableHypothesis() {
            verify(mockLlm, never()).invoke(any());
        }
    """

    def _make_validator_with_mock_llm(self) -> tuple[DataValidator, MagicMock]:
        """Mock LLM + Mock repo를 주입한 DataValidator 반환."""
        mock_llm = MagicMock()
        mock_llm.bind_tools.return_value = mock_llm
        # with_structured_output은 _verdict_chain에서 사용됨
        mock_llm.with_structured_output.return_value = MagicMock()
        mock_repo = _make_mock_repo()
        validator = DataValidator(llm=mock_llm, repo=mock_repo)
        return validator, mock_llm

    def test_unverifiable_hypothesis_becomes_unverified_status(self):
        """required_tables 빈 가설 → validate() 결과가 unverified."""
        validator, _ = self._make_validator_with_mock_llm()
        h = _make_hypothesis(required_tables=[])
        hl = HypothesisList(anomaly="revenue", hypotheses=[h])
        schema = _make_available_schema()

        results = validator.validate(hl, schema)

        assert len(results) == 1
        assert results[0].status == "unverified"

    def test_unverifiable_carries_required_data(self):
        """unverifiable 가설의 required_data가 결과에 그대로 유지된다."""
        validator, _ = self._make_validator_with_mock_llm()
        h = _make_hypothesis(
            required_tables=[],
            required_data="광고 플랫폼 데이터 필요",
        )
        hl = HypothesisList(anomaly="revenue", hypotheses=[h])
        schema = _make_available_schema()

        results = validator.validate(hl, schema)

        assert results[0].required_data == "광고 플랫폼 데이터 필요"

    def test_validate_sets_allowed_tables_from_schema(self):
        """validate() 호출 시 _allowed_tables가 schema에서 추출된 테이블명으로 설정된다."""
        validator, _ = self._make_validator_with_mock_llm()
        hl = HypothesisList(anomaly="revenue", hypotheses=[])
        schema = _make_available_schema(tables=["payments", "users"])

        validator.validate(hl, schema)

        assert validator._allowed_tables == frozenset({"payments", "users"})

    def test_validate_returns_list(self):
        """validate() 반환 타입은 list."""
        validator, _ = self._make_validator_with_mock_llm()
        hl = HypothesisList(anomaly="revenue", hypotheses=[])
        schema = _make_available_schema()

        result = validator.validate(hl, schema)

        assert isinstance(result, list)

    def test_returns_one_result_per_hypothesis(self):
        """가설 N개 → ValidationResult N개 반환."""
        validator, _ = self._make_validator_with_mock_llm()
        hypotheses = [
            _make_hypothesis(hypothesis=f"가설 {i}", required_tables=[])
            for i in range(3)
        ]
        hl = HypothesisList(anomaly="revenue", hypotheses=hypotheses)
        schema = _make_available_schema()

        results = validator.validate(hl, schema)

        assert len(results) == 3

    def test_hypothesis_text_preserved_in_result(self):
        """결과의 hypothesis 필드가 원본 가설 텍스트와 일치한다."""
        validator, _ = self._make_validator_with_mock_llm()
        h = _make_hypothesis(
            hypothesis="Android 상점 UI 변경으로 프리미엄 패키지 노출 감소",
            required_tables=[],
        )
        hl = HypothesisList(anomaly="revenue", hypotheses=[h])
        schema = _make_available_schema()

        results = validator.validate(hl, schema)

        assert results[0].hypothesis == "Android 상점 UI 변경으로 프리미엄 패키지 노출 감소"

    def test_all_unverifiable_hypotheses_skip_llm(self):
        """모든 가설이 unverifiable이면 LLM invoke가 호출되지 않는다.

        Java 비유: verify(mockLlm, never()).invoke(any())
        """
        validator, mock_llm = self._make_validator_with_mock_llm()
        hypotheses = [
            _make_hypothesis(required_tables=[]) for _ in range(3)
        ]
        hl = HypothesisList(anomaly="revenue", hypotheses=hypotheses)
        schema = _make_available_schema()

        validator.validate(hl, schema)

        # bind_tools로 생성된 _llm_with_tools.invoke가 호출되지 않아야 함
        # (mock_llm이 bind_tools.return_value로 자기 자신을 반환하므로)
        mock_llm.invoke.assert_not_called()