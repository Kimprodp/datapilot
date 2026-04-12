"""에이전트 계층.

게임 KPI 진단 파이프라인을 구성하는 6종 에이전트를 제공한다.

파이프라인 순서:
    1 BottleneckDetector    -- KPI 이상 탐지
    2 SegmentationAnalyzer  -- 세그먼트 분해
    3 HypothesisGenerator   -- 원인 가설 발산
    4 DataValidator          -- 가설 데이터 검증 (Tool Use)
    5 RootCauseReasoner     -- 인과 체인 추론
    6 ActionRecommender     -- 액션 제안
"""

from datapilot.agents.action_recommender import ActionPlan, ActionRecommender
from datapilot.agents.bottleneck_detector import AnomalyReport, BottleneckDetector
from datapilot.agents.data_validator import DataValidator, ValidationResult
from datapilot.agents.hypothesis_generator import HypothesisGenerator, HypothesisList
from datapilot.agents.root_cause_reasoner import RootCauseReasoner, RootCauseReport
from datapilot.agents.segmentation_analyzer import (
    SegmentationAnalyzer,
    SegmentationReport,
)

__all__ = [
    # 1 Bottleneck Detector
    "BottleneckDetector",
    "AnomalyReport",
    # 2 Segmentation Analyzer
    "SegmentationAnalyzer",
    "SegmentationReport",
    # 3 Hypothesis Generator
    "HypothesisGenerator",
    "HypothesisList",
    # 4 Data Validator
    "DataValidator",
    "ValidationResult",
    # 5 Root Cause Reasoner
    "RootCauseReasoner",
    "RootCauseReport",
    # 6 Action Recommender
    "ActionRecommender",
    "ActionPlan",
]