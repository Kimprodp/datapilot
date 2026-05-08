"""6 에이전트 묶음 (AgentBundle).

도메인별 키워드 + 화이트리스트를 주입한 6 에이전트 인스턴스를 한 번에 생성한다.
``PipelineOrchestrator(repo, agents=AgentBundle.create(domain, repo=repo))`` 패턴.

도메인별 차이가 흡수되는 위치:
- ③⑤⑥: ``domain_keywords`` 가 user template placeholder 로 주입됨
- ④: ``domain_keywords`` 는 균일 시그니처용 무시 인자, ``allowed_tables`` 는
  ``DomainConfig.allowed_tables`` 로 주입되어 schema 와 교집합 (이중 안전장치)
- ① ②: ``domain_keywords`` 받지만 미사용 (도메인 무관)
"""

from __future__ import annotations

from dataclasses import dataclass

from datapilot.agents.action_recommender import ActionRecommender
from datapilot.agents.bottleneck_detector import BottleneckDetector
from datapilot.agents.data_validator import DataValidator
from datapilot.agents.hypothesis_generator import HypothesisGenerator
from datapilot.agents.root_cause_reasoner import RootCauseReasoner
from datapilot.agents.segmentation_analyzer import SegmentationAnalyzer
from datapilot.domain import DOMAINS
from datapilot.repository.port import DataRepository


@dataclass(frozen=True)
class AgentBundle:
    """6 에이전트 인스턴스 묶음."""

    bottleneck: BottleneckDetector
    segmenter: SegmentationAnalyzer
    hypothesis: HypothesisGenerator
    validator: DataValidator
    reasoner: RootCauseReasoner
    recommender: ActionRecommender

    @classmethod
    def create(cls, domain: str, *, repo: DataRepository) -> "AgentBundle":
        """도메인 키워드 + 화이트리스트를 주입한 6 에이전트 묶음 생성.

        Args:
            domain: ``DOMAINS`` dict 의 키 (``"game"`` / ``"ecommerce"``).
            repo: ④ DataValidator 가 생성자로 받는 ``DataRepository``.
                Tool Use 클로저가 인스턴스 생성 시점에 repo 를 캡처해야 하므로
                AgentBundle.create 시점에 함께 주입.

        Raises:
            ValueError: 알 수 없는 ``domain``.
        """
        if domain not in DOMAINS:
            raise ValueError(
                f"unsupported domain: {domain!r}. "
                f"supported: {sorted(DOMAINS.keys())}"
            )
        cfg = DOMAINS[domain]
        kw = cfg.agent_keywords
        return cls(
            bottleneck=BottleneckDetector(domain_keywords=kw),
            segmenter=SegmentationAnalyzer(domain_keywords=kw),
            hypothesis=HypothesisGenerator(domain_keywords=kw),
            validator=DataValidator(
                repo=repo,
                domain_keywords=kw,
                allowed_tables=cfg.allowed_tables,
            ),
            reasoner=RootCauseReasoner(domain_keywords=kw),
            recommender=ActionRecommender(domain_keywords=kw),
        )
