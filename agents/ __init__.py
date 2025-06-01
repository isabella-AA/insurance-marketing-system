# agents/__init__.py
"""
智能体模块包

包含所有智能体的实现类：
- BaseAgent: 智能体基类
- HotspotAgent: 热点抓取智能体
- RiskAnalyzerAgent: 风险分析智能体
- MaterialCollectorAgent: 素材收集智能体
- ProductMatcherAgent: 产品匹配智能体
- ContentCreatorAgent: 内容创作智能体
- EditorAgent: 内容编辑智能体
"""

from agents.base_agent import BaseAgent
from agents.hotspot_agent import HotspotAgent
from agents.risk_analyzer_agent import RiskAnalyzerAgent
from agents.material_collector_agent import MaterialCollectorAgent
from agents.product_matcher_agent import ProductMatcherAgent
from agents.content_creator_agent import ContentCreatorAgent
from agents.editor_agent import EditorAgent

__all__ = [
    'BaseAgent',
    'HotspotAgent', 
    'RiskAnalyzerAgent',
    'MaterialCollectorAgent',
    'ProductMatcherAgent',
    'ContentCreatorAgent',
    'EditorAgent'
]

__version__ = "1.0.0"
__author__ = "Insurance Marketing AI Team"
__description__ = "基于多智能体的热点自适应保险营销内容生成系统"