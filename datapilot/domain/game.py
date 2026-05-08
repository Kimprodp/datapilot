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
    table_descriptions={
        "daily_kpi": "일별 KPI 집계 (Bottleneck Detector 입력)",
        "users": "유저 마스터 (세그먼트 차원: platform, country, user_type, device_model)",
        "products": "상품 마스터 (price_tier, category). 상품별 가격대 확인",
        "payments": "결제 이벤트 (user_id, product_id, amount, status, timestamp). 유저별 결제 내역 추적",
        "shop_impressions": "상점 진열 노출 로그 (product_id, slot_order, platform, date). 앱 업데이트 전후 상품 진열 순서 변화 분석에 활용",
        "releases": "앱 빌드 배포 이력 (platform, version, released_at, build_notes). 특정 날짜 배포와 이상 시점 연관 분석",
        "events": "인게임 이벤트 로그 (user_id, event_type: login/stage_clear/purchase/event_participate, event_time)",
        "sessions": "세션 로그 (user_id, session_start, session_end). 리텐션·활성화 분석",
        "content_releases": "컨텐츠/이벤트 스케줄 (content_type, name, start_date, end_date). 이벤트 종료 시점과 지표 변화 연관 분석",
        "gateways": "PG사 정보 (name, region, status: active/degraded/down). PG사 장애 상태 확인",
        "payment_attempts": "결제 시도 로그 (user_id, gateway, status: success/failed, attempt_time). 게이트웨이별 성공률 분석",
        "payment_errors": "결제 에러 상세 (error_code, error_message, gateway, first_seen). PG사별 에러 패턴 추적",
    },
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