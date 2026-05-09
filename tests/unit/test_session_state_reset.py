"""산업 selectbox 변경 시 session_state reset 단위 테스트.

검증:
- ``RESET_ON_DOMAIN_CHANGE`` 가 분석 결과 키를 모두 포함 (회귀 차단)
- ``_on_domain_change`` 호출 시 RESET 키만 비우고 다른 키는 유지
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


# app 모듈 import 실패 가능성 (streamlit 환경 부재) 대응
try:
    import app as app_module

    APP_IMPORTABLE = True
except Exception:
    APP_IMPORTABLE = False


pytestmark = pytest.mark.skipif(
    not APP_IMPORTABLE,
    reason="app.py import 불가 — streamlit 컨텍스트 환경 필요",
)


# ════════════════════════════════════════════════════════════════════
# 1. RESET_ON_DOMAIN_CHANGE 정합성
# ════════════════════════════════════════════════════════════════════


class TestResetSetIntegrity:
    def test_is_frozenset(self):
        assert isinstance(app_module.RESET_ON_DOMAIN_CHANGE, frozenset)

    def test_contains_analysis_result_keys(self):
        """회귀 차단: 분석 결과 / 선택 상태 key 가 RESET 에 포함돼야 한다.

        새 분석 결과 key 를 session_state 에 추가했는데 RESET 갱신을 잊으면
        도메인 전환 시 이전 도메인 결과가 잔존한다.
        """
        required = {"report", "selected_anomaly_idx"}
        missing = required - app_module.RESET_ON_DOMAIN_CHANGE
        assert not missing, (
            f"RESET_ON_DOMAIN_CHANGE 에 분석 결과 key 누락: {missing}"
        )

    def test_contains_viewer_keys(self):
        """mock-data-viewer 의 도메인 의존 키 (테이블 / 페이지) 가 RESET 에 포함."""
        required = {"viewer_selected_table", "viewer_selected_table_page"}
        missing = required - app_module.RESET_ON_DOMAIN_CHANGE
        assert not missing, (
            f"RESET_ON_DOMAIN_CHANGE 에 viewer 키 누락: {missing}"
        )

    def test_does_not_contain_viewer_open(self):
        """viewer_open 은 UX 일관 위해 RESET 미포함 — 도메인 전환 시 펼침 보존."""
        assert "viewer_open" not in app_module.RESET_ON_DOMAIN_CHANGE, (
            "viewer_open 이 RESET 에 들어가면 도메인 전환 시 viewer 가 매번 닫힘 "
            "(UX 일관 정책 위반)"
        )


# ════════════════════════════════════════════════════════════════════
# 2. _on_domain_change 동작
# ════════════════════════════════════════════════════════════════════


class TestOnDomainChangeCallback:
    def test_clears_only_reset_keys(self):
        """RESET 키들은 비우고 다른 key (domain, page 등) 는 유지."""
        fake_state = {
            # RESET 대상
            "report": "previous_analysis",
            "selected_anomaly_idx": 3,
            # 유지 대상
            "domain": "ecommerce",
            "page": "report",
            "period": ("d1", "d2"),
        }
        with patch.object(app_module.st, "session_state", fake_state):
            app_module._on_domain_change()

        # RESET 대상은 비워짐
        for k in app_module.RESET_ON_DOMAIN_CHANGE:
            assert k not in fake_state, f"{k} 가 비워지지 않음"
        # 유지 대상은 그대로
        assert fake_state["domain"] == "ecommerce"
        assert fake_state["page"] == "report"
        assert fake_state["period"] == ("d1", "d2")

    def test_handles_missing_keys_gracefully(self):
        """RESET 대상 key 가 session_state 에 없어도 에러 없이 통과."""
        fake_state = {"domain": "game"}
        with patch.object(app_module.st, "session_state", fake_state):
            app_module._on_domain_change()  # 예외 없이 통과

        assert fake_state == {"domain": "game"}
