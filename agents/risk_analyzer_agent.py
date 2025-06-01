import os
import json
import re
from typing import Dict, List, Any, Optional
from .base_agent import BaseAgent
from dotenv import load_dotenv

load_dotenv('config/.env')

class RiskAnalyzerAgent(BaseAgent):
    """
    风险分析智能体
    负责识别热点事件中的人群类型和风险类型
    """
    
    def __init__(self):
        super().__init__("RiskAnalyzer")
        
        # 配置参数
        self.index_name = os.getenv("HOT_EVENT_INDEX", "hoteventdb")
        self.batch_size = int(os.getenv("RISK_ANALYZER_BATCH_SIZE", 5))
        
        # 风险类型映射（用于结果标准化）
        self.risk_types = {
            "健康风险": ["健康", "医疗", "疾病", "身体", "健康风险", "医疗风险"],
            "财产损失": ["财产", "金钱", "经济", "财务", "损失", "财产风险"],
            "出行安全": ["交通", "出行", "驾驶", "行车", "路况", "交通风险"],
            "法律责任": ["法律", "责任", "诉讼", "赔偿", "法律风险"],
            "意外伤害": ["意外", "伤害", "事故", "受伤", "意外风险"],
            "火灾风险": ["火灾", "燃烧", "爆炸", "火灾风险"],
            "网络安全": ["网络", "信息", "数据", "隐私", "网络风险"],
            "自然灾害": ["地震", "洪水", "台风", "自然", "灾害风险"]
        }
        
        # 人群类型映射
        self.crowd_types = {
            "老年人": ["老人", "老年", "长者", "老年人"],
            "儿童": ["儿童", "小孩", "孩子", "幼儿", "学生"],
            "中年人": ["中年", "成年", "职场", "白领"],
            "司机": ["司机", "驾驶员", "车主", "开车"],
            "孕妇": ["孕妇", "孕期", "怀孕"],
            "游客": ["游客", "旅客", "旅游"],
            "病患": ["病人", "患者", "病患"],
            "家属": ["家属", "家庭成员", "亲属"]
        }
        
        self.logger.info(f"✅ 风险分析器初始化完成，索引: {self.index_name}")
    
    def run_once(self) -> str:
        """
        执行一次风险分析任务
        
        Returns:
            处理结果描述
        """
        # 获取待分析事件
        events = self._fetch_unanalyzed_events()
        
        if not events:
            self.logger.info("⚠️ 暂无待分析事件")
            return "无待处理事件"
        
        # 批量处理事件
        success_count = 0
        total_count = len(events)
        
        for event in events:
            try:
                if self._analyze_single_event(event):
                    success_count += 1
                    
            except Exception as e:
                self.logger.error(f"❌ 分析事件失败: {event.get('title', 'Unknown')}, {e}")
        
        result = f"处理完成: {success_count}/{total_count} 成功"
        self.logger.info(f"📊 {result}")
        return result
    
    def _fetch_unanalyzed_events(self) -> List[Dict[str, Any]]:
        """
        获取未分析的事件
        
        Returns:
            事件列表
        """
        try:
            query = {
                "bool": {
                    "must_not": [
                        {"term": {"risk_analyzed": True}}
                    ]
                }
            }
            
            events = self.es.search(
                index=self.index_name,
                query=query,
                size=self.batch_size
            )
            
            self.logger.debug(f"🔍 获取到 {len(events)} 个待分析事件")
            return events
            
        except Exception as e:
            self.logger.error(f"❌ 获取待分析事件失败: {e}")
            return []
    
    def _analyze_single_event(self, event: Dict[str, Any]) -> bool:
        """
        分析单个事件的风险
        
        Args:
            event: 事件数据
            
        Returns:
            是否分析成功
        """
        title = event.get("title", "")
        content = event.get("content", "")
        event_id = event.get("_id")
        
        if not title:
            self.logger.warning(f"⚠️ 事件标题为空: {event_id}")
            return False
        
        self.logger.info(f"🔍 正在分析: {title[:50]}...")
        
        # 执行风险分析
        risk_result = self._perform_risk_analysis(title, content)
        
        if risk_result:
            # 更新事件记录
            return self._update_event_risk(event_id, risk_result)
        else:
            self.logger.warning(f"⚠️ 风险分析失败: {title}")
            # 即使分析失败，也标记为已处理，避免重复分析
            return self._mark_as_analyzed(event_id, None)
    
    def _perform_risk_analysis(self, title: str, content: str) -> Optional[Dict[str, str]]:
        """
        执行风险分析
        
        Args:
            title: 事件标题
            content: 事件内容
            
        Returns:
            风险分析结果
        """
        try:
            # 构建系统提示词
            system_prompt = self._build_system_prompt()
            
            # 构建用户输入
            user_input = self._build_user_input(title, content)
            
            # 调用LLM进行分析
            response = self.llm.extract_json(
                user_input=user_input,
                system_prompt=system_prompt,
                expected_keys=["涉及人群", "风险类型"]
            )
            
            if response and "涉及人群" in response and "风险类型" in response:
                # 标准化结果
                normalized_result = self._normalize_risk_result(response)
                
                self.logger.debug(f"✅ 风险分析成功: {normalized_result}")
                return normalized_result
            else:
                # 尝试从原始文本中提取
                return self._extract_from_raw_response(user_input)
                
        except Exception as e:
            self.logger.error(f"❌ 风险分析异常: {e}")
            return None
    
    def _build_system_prompt(self) -> str:
        """构建系统提示词"""
        risk_types_list = ", ".join(self.risk_types.keys())
        crowd_types_list = ", ".join(self.crowd_types.keys())
        
        return f"""你是一个专业的保险风险识别专家，请基于社会热点事件内容，准确识别其中涉及的人群类型和风险类型。

**识别规则：**
1. 人群类型应从以下类别中选择：{crowd_types_list}
2. 风险类型应从以下类别中选择：{risk_types_list}
3. 如果事件涉及多个人群或风险，请选择最主要的一个
4. 必须严格按照JSON格式输出，不要包含其他文本

**输出格式：**
{{"涉及人群": "具体人群类型", "风险类型": "具体风险类型"}}

**注意事项：**
- 如果事件不涉及明显的保险风险，涉及人群填写"一般人群"，风险类型填写"无明显风险"
- 确保输出的JSON格式正确，键名必须完全匹配
- 不要输出解释性文字，只输出JSON"""
    
    def _build_user_input(self, title: str, content: str) -> str:
        """构建用户输入"""
        return f"""事件标题：{title}

事件内容：{content or '无详细内容'}

请分析以上事件的风险要素："""
    
    def _normalize_risk_result(self, result: Dict[str, str]) -> Dict[str, str]:
        """
        标准化风险分析结果
        
        Args:
            result: 原始分析结果
            
        Returns:
            标准化后的结果
        """
        crowd = result.get("涉及人群", "").strip()
        risk = result.get("风险类型", "").strip()
        
        # 标准化人群类型
        normalized_crowd = self._normalize_crowd_type(crowd)
        
        # 标准化风险类型
        normalized_risk = self._normalize_risk_type(risk)
        
        return {
            "涉及人群": normalized_crowd,
            "风险类型": normalized_risk
        }
    
    def _normalize_crowd_type(self, crowd: str) -> str:
        """标准化人群类型"""
        if not crowd:
            return "一般人群"
        
        crowd_lower = crowd.lower()
        
        for standard_type, aliases in self.crowd_types.items():
            for alias in aliases:
                if alias in crowd_lower:
                    return standard_type
        
        return crowd  # 如果没有匹配，返回原值
    
    def _normalize_risk_type(self, risk: str) -> str:
        """标准化风险类型"""
        if not risk:
            return "无明显风险"
        
        risk_lower = risk.lower()
        
        for standard_type, aliases in self.risk_types.items():
            for alias in aliases:
                if alias in risk_lower:
                    return standard_type
        
        return risk  # 如果没有匹配，返回原值
    
    def _extract_from_raw_response(self, user_input: str) -> Optional[Dict[str, str]]:
        """
        从原始回复中提取风险信息（备用方案）
        
        Args:
            user_input: 用户输入
            
        Returns:
            提取的风险信息
        """
        try:
            # 使用更简单的提示词重试
            simple_prompt = """基于事件内容，简单回答：
1. 主要涉及什么人群？
2. 可能存在什么风险？

请用简短词语回答，格式：人群：XXX，风险：XXX"""
            
            response = self.llm.simple_chat(user_input, simple_prompt)
            
            if response:
                # 尝试从回复中提取信息
                crowd_match = re.search(r'人群[：:]\s*([^，,。\n]+)', response)
                risk_match = re.search(r'风险[：:]\s*([^，,。\n]+)', response)
                
                if crowd_match and risk_match:
                    return {
                        "涉及人群": crowd_match.group(1).strip(),
                        "风险类型": risk_match.group(1).strip()
                    }
            
        except Exception as e:
            self.logger.error(f"❌ 备用提取方案失败: {e}")
        
        return None
    
    def _update_event_risk(self, event_id: str, risk_result: Dict[str, str]) -> bool:
        """
        更新事件的风险分析结果
        
        Args:
            event_id: 事件ID
            risk_result: 风险分析结果
            
        Returns:
            更新是否成功
        """
        try:
            update_data = {
                "risk_element": risk_result,
                "risk_analyzed": True
            }
            
            success = self.es.update_by_id(
                index=self.index_name,
                doc_id=event_id,
                doc=update_data
            )
            
            if success:
                self.logger.info(f"✅ 风险分析结果已更新: {event_id}")
                return True
            else:
                self.logger.error(f"❌ 更新风险分析结果失败: {event_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 更新事件风险异常: {event_id}, {e}")
            return False
    
    def _mark_as_analyzed(self, event_id: str, risk_result: Optional[Dict[str, str]]) -> bool:
        """
        标记事件为已分析
        
        Args:
            event_id: 事件ID
            risk_result: 风险分析结果（可为None）
            
        Returns:
            标记是否成功
        """
        try:
            update_data = {
                "risk_analyzed": True
            }
            
            if risk_result:
                update_data["risk_element"] = risk_result
            
            return self.es.update_by_id(
                index=self.index_name,
                doc_id=event_id,
                doc=update_data
            )
            
        except Exception as e:
            self.logger.error(f"❌ 标记事件分析状态异常: {event_id}, {e}")
            return False