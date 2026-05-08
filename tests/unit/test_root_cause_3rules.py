"""⑤ Root Cause Reasoner 환각 방어 3룰 자동 회귀 단위 테스트.

CLAUDE.md "에이전트 구현 원칙 — Root Cause Reasoner 전용 규칙" 의 3룰을
``RootCauseReport`` 출력 구조 레벨에서 자동 검증.

3룰:
- 룰 1 (원인 불명 선언): supported 가설이 0 개면 chain 비우고 summary 에
  "원인 불명" 명시. 억지로 인과 만들지 않는다.
- 룰 2 (unverified 본문 미혼입): unverified 가설은 chain 의 step / evidence
  본문에 섞지 않는다. additional_investigation 에 별도 기재.
- 룰 3 (근거 SQL 인용): chain 의 모든 step 에 evidence 채워져 있다.

본 테스트는 ``RootCauseReport`` 인스턴스를 fixture 로 직접 만들어 검증 함수가
룰 위반을 정확히 잡아내는지 회귀로 막는다. LLM 이 실제로 환각을 만드는지는
단위 테스트 영역 밖 (manual eval 영역).
"""

from __future__ import annotations

import pytest

from datapilot.agents.root_cause_reasoner import (
    CausalStep,
    RootCause,
    RootCauseReport,
    UnverifiedHypothesis,
)


# ════════════════════════════════════════════════════════════════════
# 룰 검증 헬퍼 (테스트 전용)
# ════════════════════════════════════════════════════════════════════

#: 룰 1 합격 키워드 — summary 가 이 중 하나를 포함하면 "원인 불명 선언" 으로 본다.
_UNKNOWN_CAUSE_KEYWORDS = ("원인 불명", "확인된 가설이 없", "근거 없")


def violates_rule_1_unknown_cause(report: RootCauseReport) -> bool:
    """룰 1 위반: chain 비었는데 summary 가 인과를 시사."""
    if report.root_cause.chain:
        return False  # chain 있으면 룰 1 적용 영역 X
    summary = report.root_cause.summary
    return not any(kw in summary for kw in _UNKNOWN_CAUSE_KEYWORDS)


def violates_rule_2_unverified_in_chain(
    report: RootCauseReport,
    unverified_texts: list[str],
) -> bool:
    """룰 2 위반: chain 의 step / evidence 텍스트에 unverified 가설 문구 등장."""
    chain_text = " ".join(
        s.step + " " + s.evidence for s in report.root_cause.chain
    )
    return any(t in chain_text for t in unverified_texts if t)


def violates_rule_3_missing_evidence(report: RootCauseReport) -> bool:
    """룰 3 위반: chain 안의 어떤 step 의 evidence 가 비어 있음."""
    return any(not s.evidence.strip() for s in report.root_cause.chain)


# ════════════════════════════════════════════════════════════════════
# 1. 정상 출력 (모든 룰 충족)
# ════════════════════════════════════════════════════════════════════


def _good_report() -> RootCauseReport:
    """3룰 모두 충족하는 정상 RCR 출력 fixture."""
    return RootCauseReport(
        anomaly="payment_success_rate",
        root_cause=RootCause(
            chain=[
                CausalStep(
                    step="브라질 PG (PagSeguro) 의 status 가 degraded 로 전환",
                    evidence="gateways.status='degraded' (D-0 시점)",
                ),
                CausalStep(
                    step="해당 PG 결제 시도의 실패율 56% 로 급등",
                    evidence="payment_attempts: 16 건 중 9 건 failed",
                ),
                CausalStep(
                    step="전체 결제 성공률 약 -5pp 하락",
                    evidence="daily_kpi.payment_success_rate 0.978 → 0.924",
                ),
            ],
            summary="브라질 PG 장애로 결제 성공률 -5pp 하락",
        ),
        additional_investigation=[
            UnverifiedHypothesis(
                hypothesis="결제 모듈 자체 버그 가능성",
                required_data="앱 클라이언트 결제 SDK 로그",
            ),
        ],
    )


class TestGoodReportPassesAllRules:
    def test_rule_1_passes(self):
        assert not violates_rule_1_unknown_cause(_good_report())

    def test_rule_2_passes(self):
        assert not violates_rule_2_unverified_in_chain(
            _good_report(),
            unverified_texts=["결제 모듈 자체 버그 가능성"],
        )

    def test_rule_3_passes(self):
        assert not violates_rule_3_missing_evidence(_good_report())


# ════════════════════════════════════════════════════════════════════
# 2. 룰 1 — 원인 불명 선언
# ════════════════════════════════════════════════════════════════════


class TestRule1UnknownCause:
    def test_unknown_cause_passes(self):
        """chain 빈 + summary 명시적 '원인 불명' → 통과."""
        report = RootCauseReport(
            anomaly="revenue",
            root_cause=RootCause(
                chain=[],
                summary="원인 불명 (supported 가설 0 건)",
            ),
        )
        assert not violates_rule_1_unknown_cause(report)

    def test_empty_chain_with_causal_summary_caught(self):
        """chain 빈데 summary 가 인과를 시사 → 룰 1 위반 잡힘."""
        report = RootCauseReport(
            anomaly="revenue",
            root_cause=RootCause(
                chain=[],
                summary="Android UI 변경으로 매출 -11% 하락",
            ),
        )
        assert violates_rule_1_unknown_cause(report)

    def test_chain_present_rule_1_inapplicable(self):
        """chain 채워져 있으면 룰 1 적용 영역 X (위반 아님)."""
        report = _good_report()
        assert not violates_rule_1_unknown_cause(report)


# ════════════════════════════════════════════════════════════════════
# 3. 룰 2 — unverified 본문 미혼입
# ════════════════════════════════════════════════════════════════════


class TestRule2UnverifiedNotInChain:
    def test_unverified_in_step_text_caught(self):
        report = RootCauseReport(
            anomaly="d7_retention",
            root_cause=RootCause(
                chain=[
                    CausalStep(
                        step="미검증된 시즌 이벤트 종료 영향으로 D7 리텐션 ↓",
                        evidence="(증거 인용 없음)",
                    ),
                ],
                summary="시즌 이벤트 종료 추정",
            ),
        )
        assert violates_rule_2_unverified_in_chain(
            report,
            unverified_texts=["미검증된 시즌 이벤트 종료"],
        )

    def test_unverified_in_evidence_text_caught(self):
        report = RootCauseReport(
            anomaly="d7_retention",
            root_cause=RootCause(
                chain=[
                    CausalStep(
                        step="D7 리텐션 -4pp 하락",
                        evidence="추정: 컨텐츠 공백 (검증 불가, SQL 미실행)",
                    ),
                ],
                summary="컨텐츠 공백 추정",
            ),
        )
        assert violates_rule_2_unverified_in_chain(
            report,
            unverified_texts=["검증 불가"],
        )

    def test_unverified_only_in_additional_investigation_passes(self):
        """unverified 가 chain 본문 X + additional_investigation 에 격리 → 통과."""
        report = _good_report()  # additional_investigation 에 미검증 가설
        assert not violates_rule_2_unverified_in_chain(
            report,
            unverified_texts=["결제 모듈 자체 버그 가능성"],
        )


# ════════════════════════════════════════════════════════════════════
# 4. 룰 3 — 근거 SQL 인용
# ════════════════════════════════════════════════════════════════════


class TestRule3EvidenceRequired:
    def test_empty_evidence_caught(self):
        report = RootCauseReport(
            anomaly="revenue",
            root_cause=RootCause(
                chain=[
                    CausalStep(
                        step="Android premium 노출 감소",
                        evidence="",  # 빈 문자열 — 룰 3 위반
                    ),
                ],
                summary="Android UI 변경",
            ),
        )
        assert violates_rule_3_missing_evidence(report)

    def test_whitespace_only_evidence_caught(self):
        report = RootCauseReport(
            anomaly="revenue",
            root_cause=RootCause(
                chain=[
                    CausalStep(
                        step="Android premium 노출 감소",
                        evidence="   \n  ",  # 공백만
                    ),
                ],
                summary="Android UI 변경",
            ),
        )
        assert violates_rule_3_missing_evidence(report)

    def test_some_steps_missing_evidence_caught(self):
        """일부 step 만 evidence 누락해도 위반."""
        report = RootCauseReport(
            anomaly="revenue",
            root_cause=RootCause(
                chain=[
                    CausalStep(
                        step="step 1",
                        evidence="releases 테이블 D-3 Android 빌드 등록",
                    ),
                    CausalStep(
                        step="step 2",
                        evidence="",  # 누락
                    ),
                ],
                summary="ok",
            ),
        )
        assert violates_rule_3_missing_evidence(report)

    def test_all_evidence_filled_passes(self):
        assert not violates_rule_3_missing_evidence(_good_report())


# ════════════════════════════════════════════════════════════════════
# 5. RootCauseReport Pydantic 스키마 무결성
# ════════════════════════════════════════════════════════════════════


class TestSchemaIntegrity:
    """RootCauseReport / RootCause / CausalStep 의 필드 정합성 회귀 차단."""

    def test_root_cause_chain_defaults_to_empty(self):
        rc = RootCause(summary="ok")
        assert rc.chain == []

    def test_root_cause_report_additional_investigation_defaults_to_empty(self):
        report = RootCauseReport(
            anomaly="x",
            root_cause=RootCause(summary="원인 불명"),
        )
        assert report.additional_investigation == []

    def test_causal_step_requires_step_and_evidence(self):
        with pytest.raises(Exception):  # ValidationError
            CausalStep(step="step")  # evidence 누락

    def test_unverified_hypothesis_required_fields(self):
        with pytest.raises(Exception):  # ValidationError
            UnverifiedHypothesis(hypothesis="h")  # required_data 누락
