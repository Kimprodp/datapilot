"""도메인 정의의 공용 dataclass.

각 도메인 (게임 / 이커머스 / ...) 은 `DomainConfig` 인스턴스로 표현된다.
도메인별 정의 파일 (`game.py`, `ecommerce.py`) 이 이 dataclass 를 채우고,
`__init__.py` 의 `DOMAINS` dict 가 통합한다.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DomainKeywords:
    """에이전트 프롬프트에 주입되는 도메인 키워드 (③⑤⑥ user template 용)."""

    persona: str
    """예: "게임 PM" / "이커머스 운영자"."""

    role_descriptor: str
    """예: "게임 운영 데이터 분석가" / "이커머스 GMV 분석가"."""

    primary_kpis: tuple[str, ...]
    """예: ("DAU", "ARPPU", "리텐션", "매출") / ("GMV", "전환율", "장바구니이탈률")."""


@dataclass(frozen=True)
class UILabels:
    """Streamlit UI 에 노출되는 도메인별 라벨/문구."""

    industry_name: str
    """산업 selectbox 표시명. 예: "게임" / "이커머스"."""

    entity_default_id: str
    """엔티티 디폴트 식별자. 예: "pizza_ready" / "ecommerce_demo"."""

    kpi_korean: dict[str, str]
    """KPI 코드 → 한글명 매핑. 카드 라벨에 사용."""

    scenario_descriptions: tuple[str, ...]
    """시연용 시나리오 한 줄 설명 모음."""


@dataclass(frozen=True, kw_only=True)
class DomainConfig:
    """한 도메인의 전체 정의.

    도메인 추가 시 ``datapilot/domain/<name>.py`` 에 이 dataclass 인스턴스를
    만들고 ``__init__.py`` 의 ``DOMAINS`` dict 에 등록한다.

    ``kw_only=True`` 로 모든 필드가 키워드 인자 — 호출 가독성 보존.
    """

    name: str
    """식별자 (소문자, 영문). 예: "game" / "ecommerce"."""

    db_path: str
    """DuckDB 파일의 상대 경로 (프로젝트 루트 기준)."""

    allowed_tables: frozenset[str]
    """4 중 방어의 테이블 화이트리스트.

    ④ DataValidator 가 실행 시점의 ``filtered_schema`` 와 교집합으로 사용한다 —
    이중 안전장치.
    """

    supported_segment_metrics: frozenset[str]
    """② SegmentationAnalyzer 가 분해 가능한 KPI 코드 집합."""

    table_descriptions: dict[str, str]
    """테이블 코드명 → 한글 설명 (분석 활용도 시사).

    ③ HypothesisGenerator 가 가용 스키마를 받을 때 이 설명이 함께 전달되어
    가설 발산 방향성을 잡아준다. 예: ``"inventory_changes": "재고 상태 시점별
    변경 이력. 재고 부족 / 품절 영향 분석에 활용"``.
    """

    viewer_table_descriptions: dict[str, str] = field(default_factory=dict)
    """테이블 코드명 → **비개발자 톤** 한 줄 설명 (mock-data-viewer 전용 SoT).

    ``table_descriptions`` 가 ③ HypothesisGenerator 의 분석 활용도 시사 톤이라면,
    이 필드는 데모 평가자(인사담당관 / AX 실무자 — 비개발자)에게 노출되는
    F2 라디오 라벨용. 영문 에이전트명 / 내부 용어 노출 X.
    예: ``"orders": "주문 한 건씩 기록한 표 — 누가 / 언제 / 어떤 카테고리에서 / 얼마짜리를 샀는지"``.

    빈 dict 디폴트 — 도메인별 데이터 채움 전 단계에서도 import 안 깨짐.
    누락 / 잉여 검증은 ``test_domains_config.py`` 의 키 양방향 비교 단위 테스트.
    """

    column_descriptions: dict[str, dict[str, str]] = field(default_factory=dict)
    """테이블 → 컬럼 → **비개발자 톤** 한 줄 설명 (mock-data-viewer 전용 SoT).

    F3 ``st.dataframe`` 의 ``column_config.Column(help=...)`` 컬럼 헤더 툴팁용.
    예: ``{"orders": {"customer_id": "고객 식별자", "amount": "결제 금액 (원)"}}``.

    빈 dict 디폴트 — 누락 시 ``dict.get(table, {}).get(col, "")`` 로 graceful fallback.
    실 mock DB 컬럼과의 양방향 키 검증은 ``test_domains_config.py`` 의 단위 테스트.
    """

    ui_labels: UILabels

    agent_keywords: DomainKeywords