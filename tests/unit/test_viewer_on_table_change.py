"""viewer._on_table_change 콜백 단위 테스트.

st.radio on_change 콜백 — 테이블 변경 시 페이지를 1 로 리셋.
회귀 차단: 다른 테이블 클릭 시 이전 페이지 잔존 X.
"""

from __future__ import annotations

import streamlit as st

from datapilot.viewer import _on_table_change


def test_on_table_change_resets_page_from_99(monkeypatch):
    fake_state: dict[str, int] = {"viewer_selected_table_page": 99}
    monkeypatch.setattr(st, "session_state", fake_state)
    _on_table_change()
    assert fake_state["viewer_selected_table_page"] == 1


def test_on_table_change_sets_page_when_missing(monkeypatch):
    """이전에 페이지 키가 없어도 콜백이 1 로 초기화."""
    fake_state: dict[str, int] = {}
    monkeypatch.setattr(st, "session_state", fake_state)
    _on_table_change()
    assert fake_state["viewer_selected_table_page"] == 1


def test_on_table_change_preserves_other_keys(monkeypatch):
    """페이지 키만 손대고 다른 viewer 키는 유지."""
    fake_state: dict[str, object] = {
        "viewer_selected_table_page": 7,
        "viewer_open": True,
        "viewer_selected_table": "orders",
        "report": {"some": "data"},
    }
    monkeypatch.setattr(st, "session_state", fake_state)
    _on_table_change()
    assert fake_state["viewer_selected_table_page"] == 1
    assert fake_state["viewer_open"] is True
    assert fake_state["viewer_selected_table"] == "orders"
    assert fake_state["report"] == {"some": "data"}
