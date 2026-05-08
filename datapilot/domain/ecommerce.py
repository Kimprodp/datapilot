"""이커머스 도메인 정의 (시연용 mock).

시나리오 1 개:
- 재고 부족: 인기 상품 (p_kitchen_01) D-7 품절 → kitchen 카테고리 주문수 50% 감소
  → 전체 주문 수 / GMV 감소. ``inventory_changes`` × ``orders`` × ``category_daily_revenue``
  로 시점별 검증.

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
        "category_daily_revenue",
        "inventory_changes",
    }),
    # segment 분해 가능 KPI = orders 테이블 + customers JOIN 으로 계산되는 것들.
    # conversion / payment_success_rate / visitors 는 daily_kpi 만 있고 raw 테이블
    # 에서 segment 별 분해 불가능 — segmentable 에서 제외 (Bottleneck Detector 가
    # 탐지하면 "세부 분석 미지원 지표" 카드로 표시).
    supported_segment_metrics=frozenset({
        "gmv",
        "orders",
    }),
    table_descriptions={
        "daily_kpi": "일별 KPI 집계 (gmv/orders/conversion/visitors/payment_success_rate). ① Bottleneck Detector 입력",
        "customers": "고객 마스터 (세그먼트 차원: country, customer_type[new/returning/vip], device). ② SegmentationAnalyzer 의 GROUP BY 대상",
        "orders": "주문 이벤트 (customer_id, category, product_id, amount, paid_at). 시점별 매출/주문수/카테고리 분해",
        "products": "상품 마스터 (category, inventory_status[in_stock/out_of_stock/discontinued], name). 현재 재고 상태 확인",
        "category_daily_revenue": "카테고리별 일별 매출 집계 (date, category, gmv, orders). 카테고리 매출 변동 / 인기 카테고리 영향 추적",
        "inventory_changes": "재고 상태 시점별 변경 이력 (product_id, changed_at, status, note). 재고 부족 / 품절 영향 분석에 활용",
    },
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
        ),
    ),
    agent_keywords=DomainKeywords(
        persona="이커머스 운영자",
        role_descriptor="이커머스 GMV 분석가",
        primary_kpis=("GMV", "전환율", "장바구니 이탈률", "결제 성공률"),
    ),
)
