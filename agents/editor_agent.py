import os
import json
import re
from typing import List, Dict, Any, Optional
from .base_agent import BaseAgent
from dotenv import load_dotenv

load_dotenv('config/.env')

class EditorAgent(BaseAgent):
    """
    内容编辑智能体
    负责对生成的保险营销文案进行润色、优化和合规性检查
    """
    
    def __init__(self):
        super().__init__("Editor")
        
        # 配置参数
        self.index_name = os.getenv("HOT_EVENT_INDEX", "hoteventdb")
        self.batch_size = int(os.getenv("EDITOR_BATCH_SIZE", 3))
        
        # 合规性检查规则
        self.compliance_rules = {
            "禁用词汇": [
                "100%保障", "绝对安全", "一定能赔", "必定赔付", "保证赔付",
                "最好的保险", "最便宜", "最优惠", "限时抢购", "马上购买",
                "秒杀", "抢购", "爆款", "神器", "万能", "包治百病"
            ],
            "风险提示": [
                "需要添加免责条款提示", "需要说明等待期", "需要明确赔付条件",
                "需要提示如实告知义务"
            ],
            "用词规范": {
                "意外险": "意外伤害保险",
                "医疗险": "医疗保险", 
                "重疾险": "重大疾病保险",
                "寿险": "人寿保险"
            }
        }
        
        # 编辑优化规则
        self.editing_rules = {
            "语言风格": {
                "避免口语化": ["咋办", "咋样", "啥时候", "木有"],
                "使用正式表达": ["怎么办", "怎么样", "什么时候", "没有"],
                "情感词汇": ["温暖", "安心", "踏实", "放心", "贴心", "专业"]
            },
            "结构优化": {
                "段落长度": "每段控制在80-120字",
                "句子长度": "每句控制在20-30字",
                "逻辑连接": ["首先", "其次", "最后", "因此", "所以", "然而"]
            },
            "内容完整性": {
                "必须包含": ["风险提醒", "产品介绍", "行动引导"],
                "可选包含": ["案例说明", "数据支撑", "专家观点"]
            }
        }
        
        # 质量评估标准
        self.quality_criteria = {
            "可读性": {"权重": 0.3, "标准": "语言流畅，易于理解"},
            "专业性": {"权重": 0.2, "标准": "用词准确，内容专业"},
            "情感共鸣": {"权重": 0.2, "标准": "能触动读者情感"},
            "行动引导": {"权重": 0.2, "标准": "有明确的行动指引"},
            "合规性": {"权重": 0.1, "标准": "符合保险监管要求"}
        }
        
        self.logger.info(f"✅ 内容编辑器初始化完成，索引: {self.index_name}")
    
    def run_once(self) -> str:
        """
        执行一次内容编辑任务
        
        Returns:
            处理结果描述
        """
        # 获取待编辑的内容
        events = self._fetch_events_for_editing()
        
        if not events:
            self.logger.info("⚠️ 暂无待编辑的内容")
            return "无待处理内容"
        
        # 处理事件
        success_count = 0
        total_count = len(events)
        
        for event in events:
            try:
                if self._edit_content_for_event(event):
                    success_count += 1
                    
            except Exception as e:
                self.logger.error(f"❌ 内容编辑失败: {event.get('title', 'Unknown')}, {e}")
        
        result = f"内容编辑完成: {success_count}/{total_count} 成功"
        self.logger.info(f"📊 {result}")
        return result
    
    def _fetch_events_for_editing(self) -> List[Dict[str, Any]]:
        """
        获取待编辑的事件
        
        Returns:
            事件列表
        """
        try:
            query = {
                "bool": {
                    "must": [
                        {"term": {"marketing_content_generated": True}},
                        {"exists": {"field": "marketing_content"}}
                    ],
                    "must_not": [
                        {"exists": {"field": "edited_content"}}
                    ]
                }
            }
            
            events = self.es.search(
                index=self.index_name,
                query=query,
                size=self.batch_size
            )
            
            self.logger.debug(f"🔍 获取到 {len(events)} 个待编辑的内容")
            return events
            
        except Exception as e:
            self.logger.error(f"❌ 获取待编辑内容失败: {e}")
            return []
    
    def _edit_content_for_event(self, event: Dict[str, Any]) -> bool:
        """
        为单个事件编辑内容
        
        Args:
            event: 事件数据
            
        Returns:
            是否编辑成功
        """
        title = event.get("title", "")
        marketing_content = event.get("marketing_content", {})
        event_id = event.get("_id")
        
        if not marketing_content:
            self.logger.warning(f"⚠️ 事件缺少营销内容: {event_id}")
            return False
        
        self.logger.info(f"✏️ 正在编辑内容: {title[:50]}...")
        
        # 执行内容编辑
        edited_result = self._perform_content_editing(marketing_content, event)
        
        if edited_result:
            # 更新事件记录
            return self._update_event_edited_content(event_id, edited_result)
        else:
            self.logger.warning(f"⚠️ 内容编辑失败: {title}")
            return False
    
    def _perform_content_editing(self, marketing_content: Dict[str, Any], event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        执行内容编辑
        
        Args:
            marketing_content: 原始营销内容
            event: 事件数据
            
        Returns:
            编辑后的内容
        """
        try:
            # 1. 合规性检查
            compliance_issues = self._check_compliance(marketing_content)
            
            # 2. 质量评估
            quality_score = self._assess_quality(marketing_content)
            
            # 3. 内容优化
            if compliance_issues or quality_score < 0.7:
                optimized_content = self._optimize_content(marketing_content, compliance_issues, event)
            else:
                optimized_content = marketing_content
            
            # 4. 最终润色
            polished_content = self._polish_content(optimized_content, event)
            
            # 5. 生成编辑报告
            edit_report = self._generate_edit_report(
                marketing_content, 
                polished_content, 
                compliance_issues, 
                quality_score
            )
            
            return {
                "edited_content": polished_content,
                "edit_report": edit_report,
                "quality_score": self._assess_quality(polished_content),
                "compliance_passed": len(self._check_compliance(polished_content)) == 0
            }
            
        except Exception as e:
            self.logger.error(f"❌ 内容编辑异常: {e}")
            return None
    
    def _check_compliance(self, content: Dict[str, Any]) -> List[str]:
        """
        检查内容合规性
        
        Args:
            content: 内容数据
            
        Returns:
            合规性问题列表
        """
        issues = []
        text_content = ""
        
        # 提取所有文本内容
        for key, value in content.items():
            if isinstance(value, str):
                text_content += f" {value}"
        
        text_content = text_content.lower()
        
        # 检查禁用词汇
        for forbidden_word in self.compliance_rules["禁用词汇"]:
            if forbidden_word.lower() in text_content:
                issues.append(f"包含禁用词汇: {forbidden_word}")
        
        # 检查是否缺少必要的风险提示
        risk_keywords = ["条款", "等待期", "免责", "如实告知", "保险责任"]
        if not any(keyword in text_content for keyword in risk_keywords):
            issues.append("缺少必要的风险提示和条款说明")
        
        # 检查是否有过度承诺
        promise_patterns = [
            r"保证.*赔付", r"一定.*理赔", r"100%.*保障",
            r"绝对.*安全", r"必然.*获得"
        ]
        for pattern in promise_patterns:
            if re.search(pattern, text_content):
                issues.append(f"存在过度承诺表述: {pattern}")
        
        return issues
    
    def _assess_quality(self, content: Dict[str, Any]) -> float:
        """
        评估内容质量
        
        Args:
            content: 内容数据
            
        Returns:
            质量分数 (0-1)
        """
        total_score = 0
        
        main_content = content.get("正文", "")
        if not main_content:
            return 0
        
        # 可读性评估
        readability_score = self._assess_readability(main_content)
        total_score += readability_score * self.quality_criteria["可读性"]["权重"]
        
        # 专业性评估
        professionalism_score = self._assess_professionalism(content)
        total_score += professionalism_score * self.quality_criteria["专业性"]["权重"]
        
        # 情感共鸣评估
        emotional_score = self._assess_emotional_appeal(main_content)
        total_score += emotional_score * self.quality_criteria["情感共鸣"]["权重"]
        
        # 行动引导评估
        action_score = self._assess_action_guidance(content)
        total_score += action_score * self.quality_criteria["行动引导"]["权重"]
        
        # 合规性评估
        compliance_issues = self._check_compliance(content)
        compliance_score = 1.0 if len(compliance_issues) == 0 else 0.5
        total_score += compliance_score * self.quality_criteria["合规性"]["权重"]
        
        return min(1.0, max(0.0, total_score))
    
    def _assess_readability(self, text: str) -> float:
        """评估可读性"""
        if not text:
            return 0
        
        score = 0.5  # 基础分
        
        # 句子长度适中
        sentences = re.split(r'[。！？]', text)
        avg_sentence_length = sum(len(s) for s in sentences) / len(sentences) if sentences else 0
        if 15 <= avg_sentence_length <= 35:
            score += 0.3
        
        # 段落结构清晰
        paragraphs = text.split('\n\n')
        if 2 <= len(paragraphs) <= 5:
            score += 0.2
        
        return min(1.0, score)
    
    def _assess_professionalism(self, content: Dict[str, Any]) -> float:
        """评估专业性"""
        score = 0.5
        
        text_content = str(content)
        
        # 专业术语使用
        professional_terms = ["保险", "保障", "理赔", "承保", "保费", "受益人"]
        term_count = sum(1 for term in professional_terms if term in text_content)
        score += min(0.3, term_count * 0.05)
        
        # 规范用词
        for standard, formal in self.compliance_rules["用词规范"].items():
            if formal in text_content:
                score += 0.05
        
        return min(1.0, score)
    
    def _assess_emotional_appeal(self, text: str) -> float:
        """评估情感共鸣"""
        score = 0.3
        
        # 情感词汇
        emotional_words = self.editing_rules["语言风格"]["情感词汇"]
        emotion_count = sum(1 for word in emotional_words if word in text)
        score += min(0.4, emotion_count * 0.1)
        
        # 故事性元素
        story_indicators = ["突然", "瞬间", "原来", "后来", "结果", "幸好"]
        story_count = sum(1 for indicator in story_indicators if indicator in text)
        score += min(0.3, story_count * 0.1)
        
        return min(1.0, score)
    
    def _assess_action_guidance(self, content: Dict[str, Any]) -> float:
        """评估行动引导"""
        action_guidance = content.get("行动引导", "")
        if not action_guidance:
            return 0.3
        
        score = 0.5
        
        # 明确的行动词汇
        action_words = ["咨询", "了解", "联系", "获取", "申请", "投保"]
        action_count = sum(1 for word in action_words if word in action_guidance)
        score += min(0.3, action_count * 0.1)
        
        # 联系方式或下一步指引
        guidance_indicators = ["电话", "微信", "客服", "顾问", "详情", "方案"]
        guidance_count = sum(1 for indicator in guidance_indicators if indicator in action_guidance)
        score += min(0.2, guidance_count * 0.1)
        
        return min(1.0, score)
    
    def _optimize_content(self, content: Dict[str, Any], issues: List[str], event: Dict[str, Any]) -> Dict[str, Any]:
        """
        优化内容
        
        Args:
            content: 原始内容
            issues: 合规性问题
            event: 事件数据
            
        Returns:
            优化后的内容
        """
        try:
            # 构建优化系统提示词
            system_prompt = self._build_optimization_prompt(issues)
            
            # 构建优化用户输入
            user_input = self._build_optimization_input(content, issues, event)
            
            # 调用LLM进行优化
            response = self.llm.extract_json(
                user_input=user_input,
                system_prompt=system_prompt,
                expected_keys=["标题", "正文", "核心卖点", "行动引导"]
            )
            
            if response:
                # 保留原始元数据
                optimized_content = response.copy()
                for key, value in content.items():
                    if key not in optimized_content and key not in ["标题", "正文", "核心卖点", "行动引导"]:
                        optimized_content[key] = value
                
                self.logger.debug(f"✅ 内容优化完成")
                return optimized_content
            else:
                self.logger.warning("⚠️ LLM优化失败，返回原始内容")
                return content
                
        except Exception as e:
            self.logger.error(f"❌ 内容优化异常: {e}")
            return content
    
    def _build_optimization_prompt(self, issues: List[str]) -> str:
        """构建优化提示词"""
        
        base_prompt = """你是一位专业的保险内容编辑专家，负责对营销文案进行优化和润色。

**编辑原则：**
1. 合规性优先：严格遵守保险营销规范，避免过度承诺
2. 专业性提升：使用准确的保险术语，体现专业水准
3. 可读性优化：语言流畅自然，逻辑清晰
4. 情感共鸣：保持温度，增强感染力
5. 行动引导：明确的下一步指引

**禁止使用的表达：**
- 100%保障、绝对安全、一定能赔、必定赔付
- 最好的保险、最便宜、最优惠
- 限时抢购、马上购买、秒杀等促销用词

**必须包含的要素：**
- 适当的风险提示
- 规范的保险术语
- 温和的推荐语气
- 明确的行动指引"""

        if issues:
            issue_prompt = f"\n\n**需要特别注意的问题：**\n" + "\n".join(f"- {issue}" for issue in issues)
            base_prompt += issue_prompt

        base_prompt += """\n\n**输出要求：**
请以JSON格式输出优化后的内容：
{
  "标题": "优化后的标题",
  "正文": "优化后的正文内容",
  "核心卖点": "一句话核心价值",
  "行动引导": "下一步行动指引"
}

不要输出其他解释文字，只输出JSON格式内容。"""

        return base_prompt
    
    def _build_optimization_input(self, content: Dict[str, Any], issues: List[str], event: Dict[str, Any]) -> str:
        """构建优化输入"""
        
        input_parts = ["**原始内容：**"]
        
        # 添加原始内容各部分
        for key in ["标题", "正文", "核心卖点", "行动引导"]:
            if key in content:
                input_parts.append(f"{key}：{content[key]}")
        
        # 添加问题描述
        if issues:
            input_parts.append("\n**发现的问题：**")
            input_parts.extend(f"- {issue}" for issue in issues)
        
        # 添加事件背景
        event_title = event.get("title", "")
        if event_title:
            input_parts.append(f"\n**事件背景：**{event_title}")
        
        input_parts.append("\n请对以上内容进行优化，确保合规、专业且具有感染力。")
        
        return "\n".join(input_parts)
    
    def _polish_content(self, content: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """
        最终润色
        
        Args:
            content: 优化后的内容
            event: 事件数据
            
        Returns:
            润色后的内容
        """
        try:
            system_prompt = """你是一位资深的文案润色专家，请对保险营销文案进行最后的润色。

**润色重点：**
1. 语言流畅性：确保句子通顺，表达自然
2. 情感温度：增加人文关怀，体现保险的温暖价值
3. 逻辑清晰：段落之间衔接自然，层次分明
4. 细节完善：标点、用词、语气的精细调整

**保持不变：**
- 核心信息和事实内容
- 合规性要求
- 基本结构框架

请直接输出润色后的完整内容，保持JSON格式。"""

            user_input = f"""请对以下内容进行润色：

{json.dumps(content, ensure_ascii=False, indent=2)}

要求：语言更加流畅自然，情感更加温暖真挚，保持专业性的同时增强亲和力。"""

            response = self.llm.extract_json(
                user_input=user_input,
                system_prompt=system_prompt
            )
            
            if response:
                # 确保所有必要字段都存在
                polished_content = content.copy()
                polished_content.update(response)
                
                self.logger.debug("✅ 内容润色完成")
                return polished_content
            else:
                self.logger.warning("⚠️ 润色失败，返回优化后内容")
                return content
                
        except Exception as e:
            self.logger.error(f"❌ 内容润色异常: {e}")
            return content
    
    def _generate_edit_report(self, 
                            original: Dict[str, Any], 
                            edited: Dict[str, Any], 
                            issues: List[str], 
                            original_quality: float) -> Dict[str, Any]:
        """
        生成编辑报告
        
        Args:
            original: 原始内容
            edited: 编辑后内容
            issues: 发现的问题
            original_quality: 原始质量分数
            
        Returns:
            编辑报告
        """
        final_quality = self._assess_quality(edited)
        
        # 计算改进统计
        original_length = len(original.get("正文", ""))
        edited_length = len(edited.get("正文", ""))
        
        # 分析主要改进点
        improvements = []
        
        if len(issues) > 0:
            improvements.append("修复合规性问题")
        
        if final_quality > original_quality:
            improvements.append("提升内容质量")
        
        if abs(edited_length - original_length) > 50:
            if edited_length > original_length:
                improvements.append("丰富内容表达")
            else:
                improvements.append("精简冗余表述")
        
        # 检查具体改进
        if original.get("标题") != edited.get("标题"):
            improvements.append("优化标题表达")
        
        if original.get("行动引导") != edited.get("行动引导"):
            improvements.append("加强行动引导")
        
        return {
            "编辑时间": None,  # 可以添加时间戳
            "原始质量分数": round(original_quality, 2),
            "最终质量分数": round(final_quality, 2),
            "质量提升": round(final_quality - original_quality, 2),
            "发现问题数量": len(issues),
            "已修复问题": issues,
            "主要改进点": improvements,
            "原始字数": original_length,
            "编辑后字数": edited_length,
            "字数变化": edited_length - original_length,
            "编辑状态": "完成" if final_quality >= 0.7 and len(self._check_compliance(edited)) == 0 else "需进一步优化"
        }
    
    def _update_event_edited_content(self, event_id: str, edited_result: Dict[str, Any]) -> bool:
        """
        更新事件的编辑后内容
        
        Args:
            event_id: 事件ID
            edited_result: 编辑结果
            
        Returns:
            更新是否成功
        """
        try:
            update_data = {
                "edited_content": edited_result["edited_content"],
                "edit_report": edited_result["edit_report"],
                "final_quality_score": edited_result["quality_score"],
                "content_edited": True,
                "compliance_passed": edited_result["compliance_passed"]
            }
            
            success = self.es.update_by_id(
                index=self.index_name,
                doc_id=event_id,
                doc=update_data
            )
            
            if success:
                quality_score = edited_result["quality_score"]
                compliance_status = "合规" if edited_result["compliance_passed"] else "待完善"
                self.logger.info(f"✅ 内容编辑完成: {event_id}, 质量分数: {quality_score:.2f}, 合规性: {compliance_status}")
                return True
            else:
                self.logger.error(f"❌ 编辑内容更新失败: {event_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 更新编辑内容异常: {event_id}, {e}")
            return False
    
    def edit_custom_content(self, content: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        自定义内容编辑
        
        Args:
            content: 待编辑的内容
            
        Returns:
            编辑后的内容
        """
        try:
            # 创建虚拟事件数据
            virtual_event = {"title": content.get("标题", "自定义内容")}
            
            # 执行编辑
            edited_result = self._perform_content_editing(content, virtual_event)
            
            if edited_result:
                self.logger.info("✅ 自定义内容编辑成功")
                return edited_result
            else:
                self.logger.error("❌ 自定义内容编辑失败")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 自定义内容编辑异常: {e}")
            return None
    
    def batch_quality_assessment(self, limit: int = 100) -> Dict[str, Any]:
        """
        批量质量评估
        
        Args:
            limit: 评估数量限制
            
        Returns:
            质量评估报告
        """
        try:
            # 获取已编辑的内容
            query = {"term": {"content_edited": True}}
            events = self.es.search(
                index=self.index_name,
                query=query,
                size=limit
            )
            
            if not events:
                return {"message": "没有找到已编辑的内容"}
            
            quality_scores = []
            compliance_passed_count = 0
            total_improvements = 0
            
            for event in events:
                edited_content = event.get("edited_content", {})
                edit_report = event.get("edit_report", {})
                
                if edited_content:
                    quality_score = self._assess_quality(edited_content)
                    quality_scores.append(quality_score)
                    
                    if event.get("compliance_passed", False):
                        compliance_passed_count += 1
                    
                    improvement = edit_report.get("质量提升", 0)
                    if improvement > 0:
                        total_improvements += improvement
            
            avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
            compliance_rate = compliance_passed_count / len(events) if events else 0
            avg_improvement = total_improvements / len(events) if events else 0
            
            # 质量分布统计
            excellent_count = sum(1 for score in quality_scores if score >= 0.8)
            good_count = sum(1 for score in quality_scores if 0.6 <= score < 0.8)
            fair_count = sum(1 for score in quality_scores if 0.4 <= score < 0.6)
            poor_count = sum(1 for score in quality_scores if score < 0.4)
            
            return {
                "评估内容数量": len(events),
                "平均质量分数": round(avg_quality, 2),
                "合规通过率": round(compliance_rate, 2),
                "平均质量提升": round(avg_improvement, 2),
                "质量分布": {
                    "优秀(≥0.8)": excellent_count,
                    "良好(0.6-0.8)": good_count,
                    "一般(0.4-0.6)": fair_count,
                    "较差(<0.4)": poor_count
                },
                "建议": self._generate_quality_suggestions(avg_quality, compliance_rate)
            }
            
        except Exception as e:
            self.logger.error(f"❌ 批量质量评估失败: {e}")
            return {"error": str(e)}
    
    def _generate_quality_suggestions(self, avg_quality: float, compliance_rate: float) -> List[str]:
        """生成质量改进建议"""
        suggestions = []
        
        if avg_quality < 0.6:
            suggestions.append("整体内容质量偏低，建议优化语言表达和逻辑结构")
        
        if compliance_rate < 0.8:
            suggestions.append("合规性有待提升，需要加强对保险营销规范的培训")
        
        if avg_quality >= 0.8 and compliance_rate >= 0.9:
            suggestions.append("内容质量优秀，保持当前标准")
        
        return suggestions
    
    def get_editing_stats(self) -> Dict[str, Any]:
        """
        获取编辑统计信息
        
        Returns:
            编辑统计
        """
        try:
            # 已编辑内容数量
            edited_query = {"term": {"content_edited": True}}
            edited_count = self.es.count(self.index_name, edited_query)
            
            # 待编辑内容数量
            pending_query = {
                "bool": {
                    "must": [
                        {"term": {"marketing_content_generated": True}},
                        {"exists": {"field": "marketing_content"}}
                    ],
                    "must_not": [
                        {"exists": {"field": "edited_content"}}
                    ]
                }
            }
            pending_count = self.es.count(self.index_name, pending_query)
            
            # 合规通过数量
            compliance_query = {"term": {"compliance_passed": True}}
            compliance_count = self.es.count(self.index_name, compliance_query)
            
            return {
                "已编辑内容": edited_count,
                "待编辑内容": pending_count,
                "合规通过": compliance_count,
                "合规通过率": round(compliance_count / edited_count, 2) if edited_count > 0 else 0,
                "编辑完成率": round(edited_count / (edited_count + pending_count), 2) if (edited_count + pending_count) > 0 else 0,
                "可用规则": {
                    "合规检查": len(self.compliance_rules["禁用词汇"]),
                    "编辑优化": len(self.editing_rules["语言风格"]["避免口语化"]),
                    "质量标准": len(self.quality_criteria)
                }
            }
            
        except Exception as e:
            self.logger.error(f"❌ 获取编辑统计失败: {e}")
            return {}