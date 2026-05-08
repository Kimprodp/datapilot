"""도메인 정의 패키지.

각 도메인 (게임 / 이커머스 / ...) 의 ``DomainConfig`` 를 ``DOMAINS`` dict 로 통합한다.
도메인 추가 시 ``<name>.py`` 파일 1 개 + 본 파일에 한 줄 등록만 하면 된다.

사용처는 단 3 곳:
- ``datapilot.repository.make_repository(domain)``
- ``datapilot.agents.AgentBundle.create(domain, repo=...)``
- ``app.py`` 의 산업 selectbox

다른 모듈이 ``DOMAINS`` 를 직접 참조하면 강결합이 되니 주의.
"""

from __future__ import annotations

from datapilot.domain.base import DomainConfig, DomainKeywords, UILabels
from datapilot.domain.ecommerce import ECOMMERCE
from datapilot.domain.game import GAME

DOMAINS: dict[str, DomainConfig] = {
    "game": GAME,
    "ecommerce": ECOMMERCE,
}

__all__ = [
    "DOMAINS",
    "DomainConfig",
    "DomainKeywords",
    "UILabels",
    "GAME",
    "ECOMMERCE",
]