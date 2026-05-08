"""게임 도메인 정의 (피자 레디 mock).

기존 게임 분석 흐름을 그대로 보존한 도메인 설정. 12 개 mock 테이블 화이트리스트와
4 개 segmentable KPI 는 ``analysis-performance`` 묶음의 정비 결과를 따른다.
"""

from __future__ import annotations

from datapilot.domain.base import DomainConfig, DomainKeywords, UILabels

GAME = DomainConfig(
    name="game",
    db_path="data/mock/game.db",
    allowed_tables=frozenset({
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
    }),
    supported_segment_metrics=frozenset({
        "revenue",
        "dau",
        "payment_success_rate",
        "d7_retention",
    }),
    ui_labels=UILabels(
        industry_name="게임",
        entity_default_id="pizza_ready",
        kpi_korean={
            "dau": "일간 활성 유저",
            "mau": "월간 활성 유저",
            "revenue": "매출",
            "arppu": "결제 유저 평균 매출",
            "d1_retention": "D1 리텐션",
            "d7_retention": "D7 리텐션",
            "sessions": "세션 수",
            "avg_session_sec": "평균 세션 길이",
            "payment_success_rate": "결제 성공률",
            "new_installs": "신규 설치",
        },
        scenario_descriptions=(
            "결제 성공률 급락 (PG 장애 — 브라질)",
            "Android 상점 UI 패치 영향",
        ),
    ),
    agent_keywords=DomainKeywords(
        persona="게임 PM",
        role_descriptor="게임 운영 데이터 분석가",
        primary_kpis=("DAU", "ARPPU", "리텐션", "매출", "결제 성공률"),
    ),
)