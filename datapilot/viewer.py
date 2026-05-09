"""Mock 데이터 viewer (mock-data-viewer 기능).

비개발자 데모 평가자 (인사담당관 / AX 실무자) 가 코드 없이 mock DB 의
테이블 / 스키마 / raw row 를 한 화면에서 확인하게 하는 가시화 레이어.

진입: ``app.py`` 의 page_start 에서 ``render_mock_data_viewer(domain, repo)`` 호출.

session_state 키:
    viewer_open: bool
        "가상 데이터 보기" 버튼 토글. UX 일관 위해 ``RESET_ON_DOMAIN_CHANGE``
        에는 미포함 — 도메인 전환 시에도 펼침/접힘 상태 보존.
    viewer_selected_table: str | None
        F2 라디오 선택 테이블. RESET 포함 — 도메인 간 의미 잃음.
    viewer_selected_table_page: int
        F3 페이지네이션 현재 페이지. RESET 포함 + 테이블 변경 시 1 자동 리셋.

설계 문서:
    - PRD: ``docs/features/mock-data-viewer/prd.md``
    - tech-spec: ``docs/features/mock-data-viewer/tech-spec.md``
"""

from __future__ import annotations

from math import ceil
from typing import Any

import streamlit as st

from datapilot.domain import DOMAINS
from datapilot.repository.duckdb_adapter import DuckDBAdapter

#: F3 한 페이지 row 수 — 비개발자 인지 부담 최소화 (PRD §7).
PAGE_SIZE = 50


def render_mock_data_viewer(domain: str, repo: DuckDBAdapter) -> None:
    """page_start 진입점. ``viewer_open`` 가 True 일 때만 expander 펼침.

    Args:
        domain: 현재 선택된 도메인 식별자 (``DOMAINS`` 키).
        repo: ``@st.cache_resource`` 로 캐시된 DuckDBAdapter 인스턴스.
    """
    if not st.session_state.get("viewer_open", False):
        return

    cfg = DOMAINS[domain]
    industry = cfg.ui_labels.industry_name

    with st.expander(f"📊 {industry} 가상 데이터 보기", expanded=True):
        st.caption(
            "데모에 사용된 mock 데이터입니다. AI 분석은 이 테이블들의 raw 데이터에서 "
            "추론합니다. 테이블을 선택하면 컬럼 의미와 실제 row 를 볼 수 있어요."
        )

        selected = _render_f2_table_list(domain, repo)
        if selected is not None:
            page = st.session_state.get("viewer_selected_table_page", 1)
            _render_f3_table_detail(repo, domain, selected, page)


def _render_f2_table_list(domain: str, repo: DuckDBAdapter) -> str | None:
    """F2 — 도메인의 모든 화이트리스트 테이블을 라디오로 표시. 선택값 반환."""
    cfg = DOMAINS[domain]
    schema = repo.get_available_schema(cfg.ui_labels.entity_default_id)
    tables = _filter_to_allowed_tables(schema, cfg.allowed_tables)
    if not tables:
        st.warning(
            f"{cfg.ui_labels.industry_name} mock DB 에 노출 가능한 테이블이 없어요."
        )
        return None

    table_names = sorted(t["name"] for t in tables)
    descriptions = cfg.viewer_table_descriptions

    def _label(name: str) -> str:
        desc = descriptions.get(name, "")
        return f"{name} — {desc}" if desc else name

    return st.radio(
        "테이블 선택",
        table_names,
        format_func=_label,
        key="viewer_selected_table",
        on_change=_on_table_change,
    )


def _render_f3_table_detail(
    repo: DuckDBAdapter,
    domain: str,
    table: str,
    page: int,
) -> None:
    """F3 — 컬럼 툴팁 + 50 row × 페이지네이션 + 총 row 수 노출."""
    cfg = DOMAINS[domain]
    total = repo.get_table_row_count(table)
    if total == 0:
        st.info(f"`{table}` 테이블에 데이터가 없어요.")
        return

    page_count = max(1, ceil(total / PAGE_SIZE))
    page = max(1, min(page, page_count))
    offset = (page - 1) * PAGE_SIZE

    rows = repo.get_table_rows(
        table, limit=PAGE_SIZE, offset=offset, order_desc=True,
    )

    column_descs = cfg.column_descriptions.get(table, {})
    column_config: dict[str, Any] = {}
    if rows:
        for col in rows[0].keys():
            desc = column_descs.get(col, "")
            if desc:
                column_config[col] = st.column_config.Column(help=desc)

    st.dataframe(rows, column_config=column_config, hide_index=True)
    st.number_input(
        f"페이지 (총 {page_count} 페이지 / {total:,} 행)",
        min_value=1,
        max_value=page_count,
        value=page,
        step=1,
        key="viewer_selected_table_page",
    )


def _filter_to_allowed_tables(
    schema: dict[str, Any] | None,
    allowed: frozenset[str],
) -> list[dict[str, Any]]:
    """이중 안전 필터 — ``get_available_schema`` 결과에서 화이트리스트 외 제거.

    mock DB 시드가 미래에 변경되어 화이트리스트 외 테이블이 들어와도
    UI 노출 0 보장.
    """
    if not schema:
        return []
    tables = schema.get("tables", [])
    return [t for t in tables if t.get("name") in allowed]


def _on_table_change() -> None:
    """``st.radio`` on_change 콜백 — 테이블 변경 시 페이지를 1 로 리셋."""
    st.session_state["viewer_selected_table_page"] = 1
