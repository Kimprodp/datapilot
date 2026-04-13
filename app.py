"""DataPilot — Streamlit UI 진입점.

3화면 선형 플로우: 시작 -> 분석 진행 -> 리포트.
screen-spec.md + wireframe.html 기반 구현.
"""

from __future__ import annotations

import re
import time
from datetime import date, timedelta

import streamlit as st

from datapilot.demo import run_demo
from datapilot.pipeline import (
    AnomalyAnalysis,
    PipelineOrchestrator,
    PipelineReport,
    PipelineStep,
    UnanalyzedAnomaly,
)
from datapilot.repository.duckdb_adapter import DuckDBAdapter

# True면 API 호출 없이 데모 데이터로 동작
DEMO_MODE = True

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


def _card_header(title: str, agent_label: str) -> str:
    """와이어프레임 detail-header: 회색 배경 + 하단 구분선."""
    return (
        f"<div style='display:flex;align-items:center;justify-content:space-between;"
        f"padding:12px 16px;background:#f8f8f8;"
        f"margin:-1rem -1rem 12px;border-bottom:1px solid #e0e0e0;"
        f"border-radius:6px 6px 0 0;'>"
        f"<span style='font-size:14px;font-weight:600;'>{title}</span>"
        f"<span style='font-size:11px;color:#999;'>{agent_label}</span>"
        f"</div>"
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


def _extract_detail(comparison_detail: str) -> str:
    """'98.1% -> 92.4% (직전 26일 평균 ...)' → '직전 26일 평균 ...'

    comparison_detail에서 괄호 안 내용만 추출한다.
    괄호가 없으면 원본을 그대로 반환.
    """
    match = re.search(r"\((.+)\)\s*$", comparison_detail)
    return match.group(1) if match else comparison_detail


def _render_anomaly_summary(anomaly_item) -> None:
    """이상 지표 요약 카드. 와이어프레임: 세로 row 레이아웃 (label + value)."""
    a = anomaly_item
    with st.container(border=True):
        st.markdown(_card_header("이상 지표 요약", "① 병목 감지"), unsafe_allow_html=True)
        # 세로 row 형태 (와이어프레임 kpi-row 패턴)
        change_text = a.change_display.replace("->", "→").replace("- >", "→")
        detail_text = _extract_detail(a.comparison_detail).replace("->", "→").replace("- >", "→")
        rows = [
            ("지표", f"<strong>{_korean_label(a.metric_label)}</strong>"),
            ("변화율", f"<span style='color:#e74c3c;'>{change_text}</span>"
                       f"<br><span style='font-size:13px;color:#888;'>{detail_text}</span>"),
            ("심각도", f"<strong>{a.severity}</strong>"),
            ("판정 근거", f"<span style='font-weight:400;'>{a.reasoning}</span>"),
        ]
        for label, value in rows:
            st.markdown(
                f"<div style='padding:10px 0;border-bottom:1px solid #f0f0f0;'>"
                f"<div style='font-size:12px;color:#999;margin-bottom:3px;'>{label}</div>"
                f"<div style='font-size:13px;font-weight:600;line-height:1.5;'>{value}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )


# ------------------------------------------------------------------
# 화면 1: 시작
# ------------------------------------------------------------------


def page_start() -> None:
    _app_header()

    # 게임 선택
    st.markdown(
        "<div style='font-size:14px;font-weight:600;margin-bottom:2px;'>게임 선택</div>",
        unsafe_allow_html=True,
    )
    game = st.selectbox("게임 선택", ["Pizza Ready"], index=0, label_visibility="collapsed")

    # 화면1 공통 CSS
    st.markdown("""<style>
        /* 분석 기간 pill 버튼 */
        button[data-testid*='pill'] {
            border-radius: 6px !important;
            min-height: 48px !important;
            padding: 0 28px !important;
            font-size: 14px !important;
            margin-left: 4px !important;
            margin-right: 4px !important;
        }
        /* 분석 기간 라벨 */
        div[data-testid='stButtonGroup'] label[data-testid='stWidgetLabel'] p {
            font-size: 14px !important;
            font-weight: 600 !important;
        }
        /* 게임 선택 드롭다운 */
        [data-baseweb='select'] > div {
            min-height: 48px !important;
            display: flex !important;
            align-items: center !important;
        }
        /* 분석 시작 버튼 */
        button[data-testid='stBaseButton-primary'] {
            min-height: 52px !important;
            font-size: 15px !important;
        }
    </style>""", unsafe_allow_html=True)

    # 분석 기간
    period_label = st.pills(
        "분석 기간",
        list(_PERIOD_OPTIONS.keys()),
        default="최근 30일",
    )
    if period_label is None:
        period_label = "최근 30일"

    # 분석 기간 ↔ 분석 시작 사이 여백
    st.markdown("<div style='margin-top:12px;'></div>", unsafe_allow_html=True)

    def _on_start_click() -> None:
        """on_click 콜백: 렌더링 전에 상태 전환 → page_start 재렌더 방지."""
        days = _PERIOD_OPTIONS[period_label]
        today = date(2026, 3, 31)  # Mock 데이터 기준일
        p = (today - timedelta(days=days - 1), today)
        st.session_state.game_id = "pizza_ready"
        st.session_state.game_name = game
        st.session_state.period = p
        st.session_state.period_label = period_label
        st.session_state.page = "running"

    st.button(
        "분석 시작", type="primary", use_container_width=True,
        on_click=_on_start_click,
    )


# ------------------------------------------------------------------
# 화면 2: 분석 진행
# ------------------------------------------------------------------


# 화면 2 상수 ─────────────────────────────────────────────

_STEP_AGENTS = ["segmentation", "hypothesis", "validation", "root_cause", "action"]
_STEP_LABELS = {
    "segmentation": "세그먼트",
    "hypothesis": "가설",
    "validation": "가설 검증",
    "root_cause": "원인",
    "action": "액션",
}
_METRIC_DISPLAY: dict[str, str] = {
    "revenue": "인앱결제 매출",
    "dau": "DAU",
    "payment_success_rate": "결제 성공률",
    "d7_retention": "D7 리텐션",
}


def _step_box_html(text: str, status: str) -> str:
    """스텝 네모 1개 HTML. 와이어프레임 2-B-2 스타일."""
    styles = {
        "done":   {"bg": "#d4edda", "fg": "#155724", "fw": "400"},
        "active": {"bg": "#e74c3c", "fg": "#fff",    "fw": "600"},
        "error":  {"bg": "#e74c3c", "fg": "#fff",    "fw": "600"},
        "wait":   {"bg": "#eee",    "fg": "#999",    "fw": "400"},
    }
    s = styles.get(status, styles["wait"])
    return (
        f"<div style='flex:1;text-align:center;padding:6px 0;"
        f"background:{s['bg']};border-radius:4px;"
        f"font-size:11px;color:{s['fg']};font-weight:{s['fw']};'>{text}</div>"
    )


def _detection_banner_html(status: str, summary: str) -> str:
    """이상 지표 탐지 배너. 와이어프레임 2-B-1 / 2-B-2."""
    if status == "active":
        return (
            "<div style='display:flex;align-items:center;gap:10px;padding:16px;"
            "background:#fef9f9;border:1.5px solid #e0e0e0;border-radius:8px;"
            "margin-bottom:12px;'>"
            "<div style='width:28px;height:28px;border-radius:50%;background:#e74c3c;"
            "display:flex;align-items:center;justify-content:center;'>"
            "<div style='width:10px;height:10px;border-radius:50%;background:#fff;'></div></div>"
            "<div style='flex:1;'>"
            "<div style='font-size:14px;font-weight:600;color:#333;'>이상 지표 탐지 중...</div>"
            "<div style='font-size:12px;color:#888;margin-top:2px;'>"
            "KPI 시계열을 분석하고 있습니다</div></div></div>"
        )
    if status == "done":
        return (
            "<div style='display:flex;align-items:center;gap:10px;padding:12px 16px;"
            "background:#f0faf0;border:1.5px solid #d4edda;border-radius:8px;"
            "margin-bottom:20px;'>"
            "<div style='width:28px;height:28px;border-radius:50%;background:#d4edda;"
            "color:#28a745;display:flex;align-items:center;justify-content:center;"
            "font-size:14px;font-weight:700;'>✓</div>"
            "<div style='flex:1;'>"
            "<div style='font-size:14px;font-weight:600;color:#333;'>이상 지표 탐지 완료</div>"
            f"<div style='font-size:12px;color:#666;margin-top:2px;'>{summary}</div>"
            "</div></div>"
        )
    return ""


def page_running() -> None:
    _app_header()
    st.subheader(
        f"{st.session_state.game_name} ({st.session_state.period_label}) 분석 중..."
    )

    detection_ph = st.empty()
    cards_ph = st.empty()
    elapsed_ph = st.empty()
    error_ph = st.empty()
    # page_start의 stale 요소(분석 시작 버튼 등)가 DOM에 남지 않도록 빈 슬롯 추가
    for _ in range(3):
        st.empty()
    start_time = time.time()

    # ── 상태 추적 ──
    detection: dict[str, str] = {"status": "active", "summary": ""}
    # metric → {agent: (status, summary)}
    cards: dict[str, dict[str, tuple[str, str]]] = {}
    card_order: list[str] = []
    card_errors: dict[str, str] = {}
    hyp_counts: dict[str, int] = {}

    # ── 렌더링 함수 ──

    def render_detection() -> None:
        detection_ph.markdown(
            _detection_banner_html(detection["status"], detection["summary"]),
            unsafe_allow_html=True,
        )

    def _step_text(agent: str, status: str, summary: str, metric: str) -> str:
        """스텝 네모 안에 들어갈 텍스트. screen-spec 2.3 규칙."""
        if agent == "hypothesis" and status == "done":
            match = re.search(r"(\d+)", summary)
            if match:
                hyp_counts[metric] = int(match.group(1))
            return f"가설 {hyp_counts.get(metric, '')}개"
        if agent == "validation" and status == "done":
            n = hyp_counts.get(metric, "?")
            return f"가설 검증 {n}/{n}"
        if status == "error":
            label = _STEP_LABELS.get(agent, agent)
            return f"{label} 실패"
        return _STEP_LABELS.get(agent, agent)

    def render_cards() -> None:
        if not cards:
            cards_ph.empty()
            return
        html = (
            "<div style='font-size:13px;font-weight:600;color:#888;"
            "margin-top:8px;margin-bottom:12px;'>이상 지표별 상세 분석</div>"
        )
        for metric in card_order:
            steps = cards[metric]
            label = _METRIC_DISPLAY.get(metric, metric)

            all_done = all(s[0] == "done" for s in steps.values())
            has_error = metric in card_errors

            # 카드 스타일 (실패만 빨간 테두리)
            border = "#e74c3c" if has_error else "#e0e0e0"
            bg = "#fef7f7" if has_error else "#fff"

            # 헤더 우측 상태
            if has_error:
                st_html = ("<span style='font-size:11px;color:#e74c3c;"
                           "font-weight:600;'>✕ 실패</span>")
            elif all_done:
                st_html = ("<span style='font-size:11px;color:#28a745;"
                           "font-weight:600;'>✓ 완료</span>")
            else:
                st_html = "<span style='font-size:11px;color:#888;'>분석 중</span>"

            html += (
                f"<div style='border:1.5px solid {border};border-radius:8px;"
                f"margin-bottom:16px;padding:14px 16px;background:{bg};'>"
                f"<div style='display:flex;align-items:center;"
                f"justify-content:space-between;margin-bottom:10px;'>"
                f"<span style='font-size:14px;font-weight:600;color:#333;'>"
                f"{label}</span>{st_html}</div>"
                f"<div style='display:flex;gap:6px;'>"
            )
            for agent in _STEP_AGENTS:
                s_status, s_summary = steps[agent]
                text = _step_text(agent, s_status, s_summary, metric)
                html += _step_box_html(text, s_status)
            html += "</div>"

            if has_error:
                html += (
                    f"<div style='font-size:12px;color:#e74c3c;margin-top:10px;'>"
                    f"{card_errors[metric]}</div>"
                )
            html += "</div>"

        cards_ph.markdown(html, unsafe_allow_html=True)

    # ── 콜백 ──

    def on_step(step: PipelineStep) -> None:
        if step.agent == "bottleneck":
            detection["status"] = step.status
            detection["summary"] = step.summary
            render_detection()
        else:
            m = step.metric
            if m not in cards:
                cards[m] = {a: ("wait", "") for a in _STEP_AGENTS}
                card_order.append(m)
            cards[m][step.agent] = (step.status, step.summary)
            if step.status == "error":
                card_errors[m] = step.summary
            render_cards()
        elapsed_ph.markdown(
            f"<div style='text-align:right;font-size:12px;"
            f"color:#aaa;'>{_format_elapsed(time.time() - start_time)}</div>",
            unsafe_allow_html=True,
        )

    # ── 초기 렌더링 + 파이프라인 실행 ──
    render_detection()

    try:
        if DEMO_MODE:
            report = run_demo(
                st.session_state.period,
                on_step=on_step,
            )
        else:
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
        elapsed_ph.markdown(
            f"<div style='text-align:right;font-size:12px;"
            f"color:#aaa;'>{_format_elapsed(time.time() - start_time)}</div>",
            unsafe_allow_html=True,
        )
        with error_ph.container():
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


def _build_card_data(report: PipelineReport) -> list[tuple[str, AnomalyAnalysis | UnanalyzedAnomaly]]:
    """anomaly_order 기준으로 (metric, data) 리스트를 생성한다."""
    analyzed_map = {a.anomaly.metric: a for a in report.analyzed}
    unanalyzed_map = {u.anomaly.metric: u for u in report.unanalyzed}

    items: list[tuple[str, AnomalyAnalysis | UnanalyzedAnomaly]] = []
    for metric in report.anomaly_order:
        if metric in analyzed_map:
            items.append((metric, analyzed_map[metric]))
        elif metric in unanalyzed_map:
            items.append((metric, unanalyzed_map[metric]))
    return items


def _korean_label(metric_label: str) -> str:
    """'결제 성공률 (payment_success_rate)' → '결제 성공률'."""
    return metric_label.split(" (")[0] if " (" in metric_label else metric_label


def _render_anomaly_cards(items: list[tuple[str, AnomalyAnalysis | UnanalyzedAnomaly]], selected_idx: int) -> None:
    """상단 이상 요약 카드 행 (와이어프레임: 가로 카드 + 클릭 선택).

    카드 HTML 위에 투명 버튼을 겹쳐서 카드 자체를 클릭 가능하게 만든다.
    """
    # 투명 버튼을 카드 위로 겹치는 CSS
    st.markdown(
        "<style>"
        "div[data-testid='column'] .card-overlay {"
        "  position:relative; margin-top:-90px; height:86px; z-index:1;"
        "}"
        "div[data-testid='column'] .card-overlay .stButton > button {"
        "  opacity:0 !important; width:100% !important; height:86px !important;"
        "  cursor:pointer !important; border:none !important;"
        "}"
        "</style>",
        unsafe_allow_html=True,
    )

    cols = st.columns(len(items))
    for idx, (col, (metric, data)) in enumerate(zip(cols, items)):
        with col:
            anomaly = data.anomaly
            is_selected = idx == selected_idx
            is_nonseg = isinstance(data, UnanalyzedAnomaly)

            # 선택 상태별 스타일
            if is_selected and is_nonseg:
                border, bg = "#999", "#f8f8f8"
            elif is_selected:
                border, bg = "#e74c3c", "#fef7f7"
            else:
                border, bg = "#e0e0e0", "#fafafa"

            sev_bg, sev_fg = _SEVERITY_COLORS.get(anomaly.severity, ("#e2e3e5", "#383d41"))
            korean_name = _korean_label(anomaly.metric_label)
            change_text = anomaly.change_display.replace("->", "→").replace("- >", "→")

            st.markdown(
                f"<div style='border:1.5px solid {border};background:{bg};padding:12px;"
                f"border-radius:8px;text-align:left;min-height:80px;cursor:pointer;'>"
                f"<div style='font-size:12px;font-weight:600;color:#555;'>{korean_name}</div>"
                f"<div style='font-size:16px;font-weight:700;color:#e74c3c;margin:4px 0;'>"
                f"{change_text}</div>"
                f"<span style='background:{sev_bg};color:{sev_fg};padding:2px 6px;"
                f"border-radius:4px;font-size:10px;font-weight:600;'>{anomaly.severity}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

            # 투명 버튼을 카드 위에 겹침 (클릭 핸들러)
            st.markdown('<div class="card-overlay">', unsafe_allow_html=True)
            if st.button(" ", key=f"card_{idx}", use_container_width=True):
                st.session_state.selected_anomaly_idx = idx
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)


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

    items = _build_card_data(report)

    # 선택 상태 초기화
    if "selected_anomaly_idx" not in st.session_state:
        st.session_state.selected_anomaly_idx = 0
    selected_idx = st.session_state.selected_anomaly_idx

    # 상단 카드 행
    _render_anomaly_cards(items, selected_idx)

    st.divider()

    # 선택된 카드의 상세 렌더링
    _, selected_data = items[selected_idx]
    if isinstance(selected_data, AnomalyAnalysis):
        _render_analyzed(selected_data)
    elif isinstance(selected_data, UnanalyzedAnomaly):
        _render_unanalyzed(selected_data)

    st.divider()
    # 새 분석 시작 버튼 (세로 높이 확대 — start-btn CSS 재사용)
    st.markdown(
        "<style>"
        ".start-btn .stButton > button[kind='primary'] {"
        "  padding-top:16px !important; padding-bottom:16px !important;"
        "  font-size:16px !important;"
        "}"
        "</style>",
        unsafe_allow_html=True,
    )
    st.markdown('<div class="start-btn">', unsafe_allow_html=True)
    if st.button("새 분석 시작", type="primary", use_container_width=True):
        st.session_state.page = "start"
        st.session_state.pop("selected_anomaly_idx", None)
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)


# ------------------------------------------------------------------
# 리포트: segmentable 상세 (5카드)
# ------------------------------------------------------------------


def _render_segment_card(analysis: AnomalyAnalysis) -> None:
    """카드 2: 세그먼트 분해 (② 세그먼트 분석)."""
    seg = analysis.segmentation
    with st.container(border=True):
        st.markdown(_card_header("세그먼트 분해", "② 세그먼트 분석"), unsafe_allow_html=True)
        st.markdown(f":red[{seg.summary}]")

        for dim, values in seg.breakdown.items():
            # 그룹 제목: 해당 차트 바로 위에 붙도록 margin-top 추가
            st.markdown(
                f"<div style='font-size:12px;color:#888;margin-top:16px;margin-bottom:6px;'>"
                f"{dim}별 변화율</div>",
                unsafe_allow_html=True,
            )
            for seg_name, change_val in values.items():
                color = "red" if change_val < 0 else "green"
                pct = f"{change_val:+.1%}" if abs(change_val) < 1 else f"{change_val:+.0%}"
                bar_width = min(abs(change_val) * 500, 100)
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:12px;margin:4px 0;'>"
                    f"<span style='width:80px;text-align:right;font-size:13px;color:#555;'>{seg_name}</span>"
                    f"<div style='flex:1;height:20px;background:#eee;border-radius:4px;'>"
                    f"<div style='width:{bar_width}%;height:100%;background:{color};border-radius:4px;opacity:0.5;'></div></div>"
                    f"<span style='width:55px;font-size:13px;font-weight:600;color:{color};'>{pct}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )


def _render_hypothesis_card(analysis: AnomalyAnalysis) -> None:
    """카드 3: 가설과 검증 (③+④)."""
    with st.container(border=True):
        st.markdown(_card_header("가설과 검증", "③ 가설 생성 + ④ 데이터 검증"), unsafe_allow_html=True)
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
                f"<div style='display:flex;align-items:flex-start;gap:10px;padding:10px 0;"
                f"border-bottom:1px solid #f0f0f0;'>"
                f"<div style='flex-shrink:0;margin-top:2px;'>{badge}</div>"
                f"<div>"
                f"<div style='font-size:14px;font-weight:500;'>{vr.hypothesis}</div>"
                f"<div style='font-size:13px;color:{ev_color};margin-top:4px;line-height:1.5;'>{ev_text}</div>"
                f"</div></div>",
                unsafe_allow_html=True,
            )


def _render_root_cause_card(analysis: AnomalyAnalysis) -> None:
    """카드 4: 근본 원인 (⑤)."""
    rc = analysis.root_cause
    with st.container(border=True):
        st.markdown(_card_header("근본 원인", "⑤ 원인 추론"), unsafe_allow_html=True)

        if rc.root_cause.chain:
            chain_len = len(rc.root_cause.chain)
            for step_idx, step in enumerate(rc.root_cause.chain, 1):
                st.markdown(
                    f"<div style='display:flex;align-items:flex-start;gap:12px;padding:8px 0;'>"
                    f"<div style='width:24px;height:24px;border-radius:50%;background:#e74c3c;color:#fff;"
                    f"font-size:12px;font-weight:700;display:flex;align-items:center;"
                    f"justify-content:center;flex-shrink:0;'>{step_idx}</div>"
                    f"<div><div style='font-size:14px;font-weight:500;'>{step.step}</div>"
                    f"<div style='font-size:13px;color:#888;margin-top:3px;line-height:1.5;'>근거: {step.evidence}</div>"
                    f"</div></div>",
                    unsafe_allow_html=True,
                )
                if step_idx < chain_len:
                    st.markdown(
                        "<div style='padding-left:10px;color:#ccc;font-size:16px;'>↓</div>",
                        unsafe_allow_html=True,
                    )
            st.markdown(
                f"<div style='margin-top:10px;padding:12px;background:#fef7f7;"
                f"border-left:3px solid #e74c3c;border-radius:6px;"
                f"font-size:14px;color:#c0392b;font-weight:500;'>"
                f"<strong>결론:</strong> {rc.root_cause.summary}</div>",
                unsafe_allow_html=True,
            )
        else:
            st.warning(f"원인 불명 — {rc.root_cause.summary}")

        if rc.additional_investigation:
            items_html = "".join(
                f"<div style='font-size:13px;color:#666;padding:3px 0;'>- {inv.hypothesis}</div>"
                for inv in rc.additional_investigation
            )
            st.markdown(
                f"<div style='margin-top:12px;margin-bottom:8px;padding:12px;background:#f8f9fa;"
                f"border:1px dashed #ccc;border-radius:6px;'>"
                f"<div style='font-size:13px;font-weight:600;color:#555;margin-bottom:6px;'>"
                f"추가 검토 필요</div>{items_html}</div>",
                unsafe_allow_html=True,
            )


def _render_action_card(analysis: AnomalyAnalysis) -> None:
    """카드 5: 추천 액션 (⑥)."""
    with st.container(border=True):
        st.markdown(_card_header("추천 액션", "⑥ 액션 추천"), unsafe_allow_html=True)
        sorted_actions = sorted(
            analysis.action_plan.actions,
            key=lambda a: _PRIORITY_SORT.get(a.priority, 3),
        )
        for action in sorted_actions:
            action_label, bg, fg = _PRIORITY_BADGE.get(action.priority, ("?", "#eee", "#333"))
            badge = _badge_html(action_label, bg, fg)
            st.markdown(
                f"<div style='display:flex;align-items:flex-start;gap:10px;padding:10px 0;"
                f"border-bottom:1px solid #f0f0f0;'>"
                f"<div style='flex-shrink:0;margin-top:2px;'>{badge}</div>"
                f"<div>"
                f"<div style='font-size:14px;font-weight:600;'>{action.title}</div>"
                f"<div style='font-size:13px;color:#888;margin-top:3px;'>효과: {action.effect}</div>"
                f"<div style='font-size:13px;color:#888;'>리소스: {action.effort}</div>"
                f"</div></div>",
                unsafe_allow_html=True,
            )
        if analysis.action_plan.note:
            st.markdown(
                "<div style='font-size:14px;font-weight:600;color:#555;"
                "margin-top:12px;margin-bottom:4px;'>참고</div>",
                unsafe_allow_html=True,
            )
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
