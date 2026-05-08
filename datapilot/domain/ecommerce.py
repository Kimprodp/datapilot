"""이커머스 도메인 정의 (시연용 mock).

시나리오 2 개:
- B (재고 부족): 카테고리 매출 ↓ → 인기 상품 품절. ``category_daily_revenue`` × ``products`` 1 회 GROUP BY 로 입증.
- C (프로모션 종료): GMV ↓ → 할인 이벤트 종료. 시계열 + ``promotions`` 비교 1 회.

스키마 상세는 ``docs/features/domain-extension/tech-spec.md`` §4 참조.
"""

from __future__ import annotations

from datapilot.domain.base import DomainConfig, DomainKeywords, UILabels

ECOMMERCE = DomainConfig(
    name="ecommerce",
    db_path="data/mock/ecommerce.db",
    allowed_tables=frozenset({
        "daily_kpi",
        "customers",
        "orders",
        "products",
        "promotions",
        "category_daily_revenue",
    }),
    supported_segment_metrics=frozenset({
        "gmv",
        "payment_success_rate",
        "conversion",
    }),
    ui_labels=UILabels(
        industry_name="이커머스",
        entity_default_id="ecommerce_demo",
        kpi_korean={
            "gmv": "총거래액",
            "orders": "주문 건수",
            "conversion": "전환율",
            "visitors": "방문자 수",
            "payment_success_rate": "결제 성공률",
            "avg_order_value": "평균 객단가",
            "cart_abandonment_rate": "장바구니 이탈률",
        },
        scenario_descriptions=(
            "카테고리 매출 급락 (인기 상품 재고 부족)",
            "GMV 하락 (프로모션 종료 후 자연 감소)",
        ),
    ),
    agent_keywords=DomainKeywords(
        persona="이커머스 운영자",
        role_descriptor="이커머스 GMV 분석가",
        primary_kpis=("GMV", "전환율", "장바구니 이탈률", "결제 성공률"),
    ),
)