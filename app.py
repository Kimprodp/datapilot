"""DataPilot — Streamlit UI 진입점.

3화면 선형 플로우: 시작 -> 분석 진행 -> 리포트.
screen-spec.md + wireframe.html 기반 구현.
"""

from __future__ import annotations

import time
from datetime import date, timedelta

import streamlit as st

from datapilot.pipeline import (
    AnomalyAnalysis,
    PipelineOrchestrator,
    PipelineReport,
    PipelineStep,
    UnanalyzedAnomaly,
)
from datapilot.repository.duckdb_adapter import DuckDBAdapter

# ------------------------------------------------------------------
# 페이지 설정
# ------------------------------------------------------------------

st.set_page_config(page_title="DataPilot", page_icon="📊", layout="centered")

# ------------------------------------------------------------------
# 상수
# ------------------------------------------------------------------

_APP_SUBTITLE = "운영 지표에서 병목을 찾고, 원인을 분석하고, 액션을 제안합니다."

_PERIOD_OPTIONS = {
    "최근 7일": 7,
    "최근 14일": 14,
    "최근 30일": 30,
}

_SEVERITY_COLORS = {
    "HIGH": ("#fce4e4", "#c0392b"),
    "MEDIUM": ("#fef3cd", "#856404"),
    "LOW": ("#e2e3e5", "#383d41"),
}

_STATUS_BADGE = {
    "supported": ("확인됨", "#d4edda", "#155724"),
    "rejected": ("기각됨", "#f8d7da", "#721c24"),
    "unverified": ("미검증", "#e2e3e5", "#383d41"),
}

_PRIORITY_BADGE = {
    "urgent": ("긴급", "#e74c3c", "#fff"),
    "short_term": ("단기", "#fef3cd", "#856404"),
    "mid_term": ("중기", "#e2e3e5", "#555"),
}

_AGENT_NAMES = {
    "bottleneck": "병목 감지",
    "segmentation": "세그먼트 분석",
    "hypothesis": "가설 생성",
    "validation": "데이터 검증",
    "root_cause": "원인 추론",
    "action": "액션 추천",
}

_AGENT_ORDER = ["bottleneck", "segmentation", "hypothesis", "validation", "root_cause", "action"]

_SUPPORTED_DISPLAY = "매출 · DAU · 결제 성공률 · D7 리텐션"

_VALIDATION_SORT = {"supported": 0, "rejected": 1, "unverified": 2}
_PRIORITY_SORT = {"urgent": 0, "short_term": 1, "mid_term": 2}


# ------------------------------------------------------------------
# 유틸
# ------------------------------------------------------------------


def _badge_html(text: str, bg: str, fg: str) -> str:
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;'
        f'border-radius:4px;font-size:12px;font-weight:600;">{text}</span>'
    )


def _severity_badge(severity: str) -> str:
    bg, fg = _SEVERITY_COLORS.get(severity, ("#e2e3e5", "#383d41"))
    return _badge_html(severity, bg, fg)


def _app_header() -> None:
    st.title("DataPilot")
    st.caption(_APP_SUBTITLE)
    st.divider()


def _format_elapsed(seconds: float) -> str:
    return f"경과 시간: {int(seconds // 60)}분 {int(seconds % 60)}초"


# ------------------------------------------------------------------
# 이상 요약 카드 (① 공통 — analyzed/unanalyzed 모두 사용)
# ------------------------------------------------------------------


def _render_anomaly_summary(anomaly_item) -> None:
    """이상 지표 요약 카드. screen-spec 3.4 / 3.10.1."""
    with st.container(border=True):
        st.markdown(
            "**이상 지표 요약** "
            "<span style='color:#999;font-size:12px;float:right;'>① 병목 감지</span>",
            unsafe_allow_html=True,
        )
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**지표**: {anomaly_item.metric_label}")
            st.markdown(
                f"**심각도**: {_severity_badge(anomaly_item.severity)}",
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(f"**변화율**: :red[{anomaly_item.change_display}]")
            st.caption(anomaly_item.comparison_detail)
        st.markdown("**판정 근거**")
        st.markdown(
            f"<div style='font-size:13px;color:#555;line-height:1.6;'>"
            f"{anomaly_item.reasoning}</div>",
            unsafe_allow_html=True,
        )


# ------------------------------------------------------------------
# 화면 1: 시작
# ------------------------------------------------------------------


def page_start() -> None:
    _app_header()

    game = st.selectbox("게임 선택", ["Pizza Ready"], index=0)
    period_label = st.radio(
        "분석 기간",
        list(_PERIOD_OPTIONS.keys()),
        index=2,
        horizontal=True,
    )

    if st.button("분석 시작", type="primary", use_container_width=True):
        days = _PERIOD_OPTIONS[period_label]
        today = date(2026, 3, 31)  # Mock 데이터 기준일
        period = (today - timedelta(days=days - 1), today)

        st.session_state.game_id = "pizza_ready"
        st.session_state.game_name = game
        st.session_state.period = period
        st.session_state.period_label = period_label
        st.session_state.page = "running"
        st.rerun()


# ------------------------------------------------------------------
# 화면 2: 분석 진행
# ------------------------------------------------------------------


def _render_step(container, num: int, agent: str, status: str, summary: str) -> None:
    """화면2 진행 스텝 1행. 와이어프레임 일치: 번호 원 + 이름 + 상태 텍스트."""
    name = _AGENT_NAMES.get(agent, agent)

    # 상태별 스타일
    styles = {
        "done":   {"bg": "#d4edda", "fg": "#28a745", "icon": str(num), "status_fg": "#28a745", "row_bg": "#fff"},
        "active": {"bg": "#e74c3c", "fg": "#fff",    "icon": str(num), "status_fg": "#e74c3c", "row_bg": "#fef9f9"},
        "error":  {"bg": "#e74c3c", "fg": "#fff",    "icon": "✕",      "status_fg": "#e74c3c", "row_bg": "#fef2f2"},
        "wait":   {"bg": "#eee",    "fg": "#999",    "icon": str(num), "status_fg": "#999",    "row_bg": "#fff"},
    }
    s = styles.get(status, styles["wait"])

    # 상태 텍스트
    status_labels = {
        "done": f"완료 — {summary}",
        "active": "분석 중...",
        "error": f"실패 — {summary}",
        "wait": "대기",
    }
    status_text = status_labels.get(status, "대기")

    # 이름 스타일
    name_style = "font-weight:600;color:#e74c3c;" if status == "error" else "font-weight:500;"
    status_weight = "font-weight:600;" if status in ("active", "error") else ""

    row_bg = s["row_bg"]
    icon_bg = s["bg"]
    icon_fg = s["fg"]
    icon_text = s["icon"]
    st_fg = s["status_fg"]

    container.markdown(
        f"<div style='display:flex;align-items:center;gap:12px;padding:14px 16px;"
        f"border-bottom:1px solid #f0f0f0;background:{row_bg};'>"
        f"<div style='width:32px;height:32px;border-radius:50%;background:{icon_bg};color:{icon_fg};"
        f"display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;"
        f"flex-shrink:0;'>{icon_text}</div>"
        f"<div style='flex:1;font-size:14px;{name_style}'>{name}</div>"
        f"<div style='font-size:12px;color:{st_fg};{status_weight}'>{status_text}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )


def page_running() -> None:
    _app_header()
    st.subheader(f"{st.session_state.game_name} ({st.session_state.period_label}) 분석 중...")

    step_containers = {}
    for idx, agent_name in enumerate(_AGENT_ORDER, 1):
        step_containers[agent_name] = st.empty()
        _render_step(step_containers[agent_name], idx, agent_name, "wait", "대기")

    elapsed_placeholder = st.empty()
    error_placeholder = st.empty()
    start_time = time.time()

    last_status: dict[str, tuple[str, str]] = {}

    def on_step(step: PipelineStep) -> None:
        last_status[step.agent] = (step.status, step.summary)
        for step_idx, step_agent in enumerate(_AGENT_ORDER, 1):
            if step_agent in last_status:
                s, summ = last_status[step_agent]
                _render_step(step_containers[step_agent], step_idx, step_agent, s, summ)
        secs = time.time() - start_time
        elapsed_placeholder.caption(_format_elapsed(secs))

    try:
        with DuckDBAdapter() as repo:
            orchestrator = PipelineOrchestrator(repo)
            report = orchestrator.run(
                st.session_state.game_id,
                st.session_state.period,
                on_step=on_step,
            )

        st.session_state.report = report
        st.session_state.page = "report"
        time.sleep(0.5)
        st.rerun()

    except Exception as exc:
        secs = time.time() - start_time
        elapsed_placeholder.caption(_format_elapsed(secs))
        with error_placeholder.container():
            st.error(f"분석 중 오류가 발생했습니다: {exc}")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("다시 시도", type="primary", use_container_width=True):
                    st.rerun()
            with col2:
                if st.button("처음으로", use_container_width=True):
                    st.session_state.page = "start"
                    st.rerun()


# ------------------------------------------------------------------
# 화면 3: 리포트
# ------------------------------------------------------------------


def _build_tab_data(report: PipelineReport) -> tuple[list[str], dict]:
    """anomaly_order 기준으로 탭 이름 + 데이터 매핑을 생성한다."""
    analyzed_map = {a.anomaly.metric: a for a in report.analyzed}
    unanalyzed_map = {u.anomaly.metric: u for u in report.unanalyzed}

    tab_names: list[str] = []
    tab_data: dict = {}
    for metric in report.anomaly_order:
        if metric in analyzed_map:
            tab_names.append(analyzed_map[metric].anomaly.metric_label)
            tab_data[metric] = analyzed_map[metric]
        elif metric in unanalyzed_map:
            tab_names.append(unanalyzed_map[metric].anomaly.metric_label)
            tab_data[metric] = unanalyzed_map[metric]
    return tab_names, tab_data


def page_report() -> None:
    report: PipelineReport = st.session_state.report
    _app_header()
    st.subheader(
        f"분석 완료 — {st.session_state.game_name} ({st.session_state.period_label})"
    )

    if not report.analyzed and not report.unanalyzed:
        st.info("모든 지표가 정상입니다. 이상 지표가 감지되지 않았습니다.")
        if st.button("새 분석 시작", type="primary"):
            st.session_state.page = "start"
            st.rerun()
        return

    tab_names, tab_data = _build_tab_data(report)
    tabs = st.tabs(tab_names)
    for tab, metric in zip(tabs, report.anomaly_order):
        with tab:
            data = tab_data.get(metric)
            if isinstance(data, AnomalyAnalysis):
                _render_analyzed(data)
            elif isinstance(data, UnanalyzedAnomaly):
                _render_unanalyzed(data)

    st.divider()
    if st.button("새 분석 시작", type="primary", use_container_width=True):
        st.session_state.page = "start"
        st.rerun()


# ------------------------------------------------------------------
# 리포트: segmentable 상세 (5카드)
# ------------------------------------------------------------------


def _render_segment_card(analysis: AnomalyAnalysis) -> None:
    """카드 2: 세그먼트 분해 (② 세그먼트 분석)."""
    seg = analysis.segmentation
    with st.container(border=True):
        st.markdown(
            "**세그먼트 분해** "
            "<span style='color:#999;font-size:12px;float:right;'>② 세그먼트 분석</span>",
            unsafe_allow_html=True,
        )
        st.markdown(f":red[{seg.summary}]")

        for dim, values in seg.breakdown.items():
            st.caption(f"{dim}별 변화율")
            for seg_name, change_val in values.items():
                color = "red" if change_val < 0 else "green"
                pct = f"{change_val:+.1%}" if abs(change_val) < 1 else f"{change_val:+.0%}"
                bar_width = min(abs(change_val) * 500, 100)
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:8px;margin:2px 0;'>"
                    f"<span style='width:80px;text-align:right;font-size:12px;color:#666;'>{seg_name}</span>"
                    f"<div style='flex:1;height:16px;background:#eee;border-radius:3px;'>"
                    f"<div style='width:{bar_width}%;height:100%;background:{color};border-radius:3px;opacity:0.5;'></div></div>"
                    f"<span style='width:50px;font-size:12px;font-weight:600;color:{color};'>{pct}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )


def _render_hypothesis_card(analysis: AnomalyAnalysis) -> None:
    """카드 3: 가설과 검증 (③+④)."""
    with st.container(border=True):
        st.markdown(
            "**가설과 검증** "
            "<span style='color:#999;font-size:12px;float:right;'>③ 가설 생성 + ④ 데이터 검증</span>",
            unsafe_allow_html=True,
        )
        sorted_results = sorted(
            analysis.validation_results,
            key=lambda vr: _VALIDATION_SORT.get(vr.status, 3),
        )
        for vr in sorted_results:
            vr_label, bg, fg = _STATUS_BADGE.get(vr.status, ("?", "#eee", "#333"))
            badge = _badge_html(vr_label, bg, fg)
            ev_color = "#555" if vr.status == "supported" else "#888"
            ev_text = vr.evidence or vr.required_data or ""
            st.markdown(
                f"<div style='display:flex;align-items:flex-start;gap:8px;padding:8px 0;"
                f"border-bottom:1px solid #f0f0f0;'>"
                f"<div style='flex-shrink:0;'>{badge}</div>"
                f"<div>"
                f"<div style='font-size:13px;font-weight:500;'>{vr.hypothesis}</div>"
                f"<div style='font-size:11px;color:{ev_color};margin-top:3px;'>{ev_text}</div>"
                f"</div></div>",
                unsafe_allow_html=True,
            )


def _render_root_cause_card(analysis: AnomalyAnalysis) -> None:
    """카드 4: 근본 원인 (⑤)."""
    rc = analysis.root_cause
    with st.container(border=True):
        st.markdown(
            "**근본 원인** "
            "<span style='color:#999;font-size:12px;float:right;'>⑤ 원인 추론</span>",
            unsafe_allow_html=True,
        )

        if rc.root_cause.chain:
            chain_len = len(rc.root_cause.chain)
            for step_idx, step in enumerate(rc.root_cause.chain, 1):
                st.markdown(
                    f"<div style='display:flex;align-items:flex-start;gap:10px;padding:6px 0;'>"
                    f"<div style='width:22px;height:22px;border-radius:50%;background:#e74c3c;color:#fff;"
                    f"font-size:11px;font-weight:700;display:flex;align-items:center;"
                    f"justify-content:center;flex-shrink:0;'>{step_idx}</div>"
                    f"<div><div style='font-size:13px;font-weight:500;'>{step.step}</div>"
                    f"<div style='font-size:11px;color:#888;margin-top:2px;'>근거: {step.evidence}</div>"
                    f"</div></div>",
                    unsafe_allow_html=True,
                )
                if step_idx < chain_len:
                    st.markdown(
                        "<div style='padding-left:8px;color:#ccc;'>↓</div>",
                        unsafe_allow_html=True,
                    )
            st.markdown(
                f"<div style='margin-top:8px;padding:10px;background:#fef7f7;"
                f"border-left:3px solid #e74c3c;border-radius:4px;"
                f"font-size:13px;color:#c0392b;font-weight:500;'>"
                f"<strong>결론:</strong> {rc.root_cause.summary}</div>",
                unsafe_allow_html=True,
            )
        else:
            st.warning(f"원인 불명 — {rc.root_cause.summary}")

        if rc.additional_investigation:
            items_html = "".join(
                f"<div style='font-size:12px;color:#888;'>- {inv.hypothesis} ({inv.required_data})</div>"
                for inv in rc.additional_investigation
            )
            st.markdown(
                f"<div style='margin-top:10px;padding:10px;background:#f8f9fa;"
                f"border:1px dashed #ccc;border-radius:6px;'>"
                f"<div style='font-size:12px;font-weight:600;color:#666;margin-bottom:4px;'>"
                f"추가 검토 필요</div>{items_html}</div>",
                unsafe_allow_html=True,
            )


def _render_action_card(analysis: AnomalyAnalysis) -> None:
    """카드 5: 추천 액션 (⑥)."""
    with st.container(border=True):
        st.markdown(
            "**추천 액션** "
            "<span style='color:#999;font-size:12px;float:right;'>⑥ 액션 추천</span>",
            unsafe_allow_html=True,
        )
        sorted_actions = sorted(
            analysis.action_plan.actions,
            key=lambda a: _PRIORITY_SORT.get(a.priority, 3),
        )
        for action in sorted_actions:
            action_label, bg, fg = _PRIORITY_BADGE.get(action.priority, ("?", "#eee", "#333"))
            badge = _badge_html(action_label, bg, fg)
            st.markdown(
                f"<div style='display:flex;align-items:flex-start;gap:8px;padding:8px 0;"
                f"border-bottom:1px solid #f0f0f0;'>"
                f"<div style='flex-shrink:0;'>{badge}</div>"
                f"<div>"
                f"<div style='font-size:13px;font-weight:600;'>{action.title}</div>"
                f"<div style='font-size:11px;color:#888;margin-top:2px;'>효과: {action.effect}</div>"
                f"<div style='font-size:11px;color:#888;'>리소스: {action.effort}</div>"
                f"</div></div>",
                unsafe_allow_html=True,
            )
        if analysis.action_plan.note:
            st.info(analysis.action_plan.note)


def _render_analyzed(analysis: AnomalyAnalysis) -> None:
    """segmentable 이상 지표 5카드 상세."""
    _render_anomaly_summary(analysis.anomaly)
    _render_segment_card(analysis)
    _render_hypothesis_card(analysis)
    _render_root_cause_card(analysis)
    _render_action_card(analysis)


# ------------------------------------------------------------------
# 리포트: non-segmentable 축약 (screen-spec 3.10)
# ------------------------------------------------------------------


def _render_unanalyzed(ua: UnanalyzedAnomaly) -> None:
    _render_anomaly_summary(ua.anomaly)

    with st.container(border=True):
        st.markdown(
            "<div style='text-align:center;padding:16px;'>"
            "<div style='font-size:32px;opacity:0.3;margin-bottom:8px;'>📊</div>"
            "<div style='font-size:14px;font-weight:600;color:#999;margin-bottom:6px;'>"
            "세부 분석 미지원</div>"
            "<div style='font-size:12px;color:#aaa;line-height:1.6;'>"
            "이 지표는 세부 분석이 아직 지원되지 않아 이상 지표 요약만 제공됩니다.</div>"
            f"<div style='margin-top:12px;padding:8px 16px;background:#f8f9fa;"
            f"border-radius:6px;display:inline-block;'>"
            f"<div style='font-size:11px;color:#888;'>현재 세부 분석 가능 지표</div>"
            f"<div style='font-size:12px;color:#555;margin-top:2px;font-weight:500;'>"
            f"{_SUPPORTED_DISPLAY}</div></div></div>",
            unsafe_allow_html=True,
        )


# ------------------------------------------------------------------
# 라우터
# ------------------------------------------------------------------


def main() -> None:
    if "page" not in st.session_state:
        st.session_state.page = "start"

    page = st.session_state.page
    if page == "start":
        page_start()
    elif page == "running":
        page_running()
    elif page == "report":
        page_report()


if __name__ == "__main__":
    main()