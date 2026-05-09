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
    viewer_table_descriptions={
        "daily_kpi": "일별 핵심 지표 모음 — 하루치 거래액 / 주문 수 / 전환율 / 방문자 수 등",
        "customers": "고객 정보 — 국가 / 고객 유형 (신규 / 재구매 / VIP) / 기기 정보",
        "orders": "주문 기록 — 누가 / 언제 / 어떤 카테고리에서 / 얼마짜리를 샀는지 한 건씩",
        "products": "상품 정보 — 카테고리 / 재고 상태 (정상 / 품절 / 단종) / 상품명",
        "category_daily_revenue": "카테고리별 일별 매출 집계 — 카테고리당 하루 거래액 / 주문 수",
        "inventory_changes": "재고 변동 이력 — 어느 상품이 언제 품절 / 재입고 되었는지 시점별 기록",
    },
    column_descriptions={
        "category_daily_revenue": {
            "date": "날짜",
            "category": "상품 카테고리",
            "gmv": "카테고리 거래액 (원) - 해당 일에 해당 카테고리에서 들어온 주문 금액 합계",
            "orders": "카테고리 주문 건수 - 해당 일 그 카테고리에서 들어온 주문 총 개수",
        },
        "customers": {
            "customer_id": "고객 식별자",
            "country": "국가",
            "customer_type": "고객 유형 (new = 신규 / returning = 재구매 / vip)",
            "device": "사용 기기",
        },
        "daily_kpi": {
            "date": "날짜",
            "gmv": "총 거래액 (원) - 해당 일에 들어온 모든 주문 금액 합계",
            "orders": "주문 건수 - 해당 일에 들어온 주문의 총 개수",
            "conversion": "전환율 (방문 대비 주문 비율, 0~1)",
            "visitors": "방문자 수 - 해당 일에 사이트를 방문한 고유 사용자 수",
            "payment_success_rate": "결제 성공률 (0~1) - 해당 일 결제 시도 대비 성공 비율",
        },
        "inventory_changes": {
            "change_id": "변경 이력 식별자",
            "product_id": "상품 식별자",
            "changed_at": "변경 시각",
            "status": "변경 후 상태 (in_stock = 정상 / out_of_stock = 품절 / discontinued = 단종)",
            "note": "부가 메모",
        },
        "orders": {
            "order_id": "주문 식별자",
            "customer_id": "주문한 고객",
            "category": "상품 카테고리",
            "product_id": "주문한 상품",
            "amount": "결제 금액 (원)",
            "paid_at": "결제 시각",
        },
        "products": {
            "product_id": "상품 식별자",
            "category": "상품 카테고리",
            "inventory_status": "현재 재고 상태 (in_stock / out_of_stock / discontinued)",
            "name": "상품 이름",
        },
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
