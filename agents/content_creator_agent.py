import os
import json
from typing import List, Dict, Any, Optional
from .base_agent import BaseAgent
from dotenv import load_dotenv

load_dotenv('config/.env')

class ContentCreatorAgent(BaseAgent):
    """
    内容创作智能体
    负责根据热点事件、风险分析和产品推荐生成保险营销文案
    """
    
    def __init__(self):
        super().__init__("ContentCreator")
        
        # 配置参数
        self.index_name = os.getenv("HOT_EVENT_INDEX", "hoteventdb")
        self.batch_size = int(os.getenv("CONTENT_CREATOR_BATCH_SIZE", 3))
        self.example_index = os.getenv("EXAMPLE_INDEX", "marketing_examples")
        
        # 文案模板配置
        self.content_templates = {
            "三段式": {
                "structure": ["背景引入", "风险分析", "产品推荐"],
                "description": "背景+风险+推荐的经典三段式结构"
            },
            "故事式": {
                "structure": ["故事开头", "转折点", "解决方案"],
                "description": "以故事叙述的方式展开"
            },
            "问答式": {
                "structure": ["提出问题", "分析问题", "给出答案"],
                "description": "通过问答形式引导思考"
            }
        }
        
        # 内容风格配置
        self.content_styles = {
            "亲切温和": "语气温和亲切，像朋友间的关怀提醒",
            "专业权威": "用词专业严谨，体现保险专业性",
            "轻松幽默": "适当使用幽默元素，让内容更有趣",
            "急迫提醒": "突出风险的紧迫性，促使行动"
        }
        
        self.logger.info(f"✅ 内容创作器初始化完成，索引: {self.index_name}")
    
    def run_once(self) -> str:
        """
        执行一次内容创作任务
        
        Returns:
            处理结果描述
        """
        # 获取待创作内容的事件
        events = self._fetch_events_for_content_creation()
        
        if not events:
            self.logger.info("⚠️ 暂无待创作内容的事件")
            return "无待处理事件"
        
        # 处理事件
        success_count = 0
        total_count = len(events)
        
        for event in events:
            try:
                if self._create_content_for_event(event):
                    success_count += 1
                    
            except Exception as e:
                self.logger.error(f"❌ 内容创作失败: {event.get('title', 'Unknown')}, {e}")
        
        result = f"内容创作完成: {success_count}/{total_count} 成功"
        self.logger.info(f"📊 {result}")
        return result
    
    def _fetch_events_for_content_creation(self) -> List[Dict[str, Any]]:
        """
        获取待创作内容的事件
        
        Returns:
            事件列表
        """
        try:
            query = {
                "bool": {
                    "must": [
                        {"term": {"material_collected": True}},
                        {"term": {"product_matched": True}}
                    ],
                    "must_not": [
                        {"exists": {"field": "marketing_content"}}
                    ]
                }
            }
            
            events = self.es.search(
                index=self.index_name,
                query=query,
                size=self.batch_size
            )
            
            self.logger.debug(f"🔍 获取到 {len(events)} 个待创作内容的事件")
            return events
            
        except Exception as e:
            self.logger.error(f"❌ 获取待创作事件失败: {e}")
            return []
    
    def _create_content_for_event(self, event: Dict[str, Any]) -> bool:
        """
        为单个事件创作营销内容
        
        Args:
            event: 事件数据
            
        Returns:
            是否创作成功
        """
        title = event.get("title", "")
        event_id = event.get("_id")
        
        if not title:
            self.logger.warning(f"⚠️ 事件标题为空: {event_id}")
            return False
        
        self.logger.info(f"✍️ 正在创作内容: {title[:50]}...")
        
        # 准备创作素材
        creation_materials = self._prepare_creation_materials(event)
        
        # 执行内容创作
        marketing_content = self._generate_marketing_content(creation_materials)
        
        if marketing_content:
            # 更新事件记录
            return self._update_event_content(event_id, marketing_content)
        else:
            self.logger.warning(f"⚠️ 内容创作失败: {title}")
            return False
    
    def _prepare_creation_materials(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备内容创作素材
        
        Args:
            event: 事件数据
            
        Returns:
            创作素材
        """
        materials = {
            "event_info": {
                "title": event.get("title", ""),
                "content": event.get("content", ""),
                "url": event.get("url", "")
            },
            "risk_analysis": event.get("risk_element", {}),
            "recommended_products": event.get("recommended_products", []),
            "supplementary_materials": event.get("material", {}),
            "creation_context": {}
        }
        
        # 获取创作示例
        materials["examples"] = self._get_creation_examples(event)
        
        # 分析内容特点
        materials["content_analysis"] = self._analyze_content_characteristics(event)
        
        return materials
    
    def _get_creation_examples(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        获取相关的创作示例
        
        Args:
            event: 事件数据
            
        Returns:
            创作示例列表
        """
        try:
            # 基于产品类别获取示例
            recommended_products = event.get("recommended_products", [])
            if not recommended_products:
                return []
            
            # 提取产品类别
            product_categories = list(set([
                product.get("产品类别", "") 
                for product in recommended_products 
                if product.get("产品类别")
            ]))
            
            if not product_categories:
                return []
            
            # 从示例库中搜索相关示例
            example_query = {
                "bool": {
                    "should": [
                        {"terms": {"product_category": product_categories}},
                        {"match": {"content": event.get("title", "")}}
                    ]
                }
            }
            
            examples = self.es.search(
                index=self.example_index,
                query=example_query,
                size=3
            )
            
            self.logger.debug(f"📚 获取到 {len(examples)} 个创作示例")
            return examples
            
        except Exception as e:
            self.logger.warning(f"⚠️ 获取创作示例失败: {e}")
            return []
    
    def _analyze_content_characteristics(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        分析内容特点，确定创作策略
        
        Args:
            event: 事件数据
            
        Returns:
            内容分析结果
        """
        title = event.get("title", "")
        content = event.get("content", "")
        risk_element = event.get("risk_element", {})
        
        analysis = {
            "urgency_level": "中等",
            "emotional_tone": "关怀",
            "target_style": "亲切温和",
            "recommended_template": "三段式"
        }
        
        # 分析紧迫性
        urgent_keywords = ["突发", "紧急", "意外", "事故", "危险", "死亡", "受伤"]
        if any(keyword in title + content for keyword in urgent_keywords):
            analysis["urgency_level"] = "高"
            analysis["target_style"] = "急迫提醒"
        
        # 分析情感色调
        sad_keywords = ["去世", "死亡", "离世", "不幸", "悲剧"]
        if any(keyword in title + content for keyword in sad_keywords):
            analysis["emotional_tone"] = "沉重"
            analysis["target_style"] = "专业权威"
        
        # 分析目标人群
        crowd_type = risk_element.get("涉及人群", "")
        if crowd_type in ["儿童", "老年人"]:
            analysis["emotional_tone"] = "关怀"
            analysis["target_style"] = "亲切温和"
        elif crowd_type in ["司机", "中年人"]:
            analysis["target_style"] = "专业权威"
        
        # 推荐模板
        if analysis["urgency_level"] == "高":
            analysis["recommended_template"] = "问答式"
        elif analysis["emotional_tone"] == "沉重":
            analysis["recommended_template"] = "三段式"
        else:
            analysis["recommended_template"] = "故事式"
        
        return analysis
    
    def _generate_marketing_content(self, materials: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        生成营销内容
        
        Args:
            materials: 创作素材
            
        Returns:
            生成的营销内容
        """
        try:
            # 构建系统提示词
            system_prompt = self._build_creation_system_prompt(materials)
            
            # 构建用户输入
            user_input = self._build_creation_user_input(materials)
            
            # 调用LLM生成内容
            response = self.llm.chat(
                user_input=user_input,
                system_prompt=system_prompt,
                temperature=0.8  # 提高创造性
            )
            
            if response.success and response.content:
                # 解析和结构化内容
                structured_content = self._structure_generated_content(
                    response.content, 
                    materials
                )
                
                self.logger.info(f"✅ 内容生成成功，长度: {len(response.content)} 字符")
                return structured_content
            else:
                self.logger.error(f"❌ LLM内容生成失败: {response.error}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 内容生成异常: {e}")
            return None
    
    def _build_creation_system_prompt(self, materials: Dict[str, Any]) -> str:
        """
        构建内容创作的系统提示词
        
        Args:
            materials: 创作素材
            
        Returns:
            系统提示词
        """
        content_analysis = materials.get("content_analysis", {})
        target_style = content_analysis.get("target_style", "亲切温和")
        recommended_template = content_analysis.get("recommended_template", "三段式")
        
        template_structure = self.content_templates[recommended_template]["structure"]
        style_description = self.content_styles[target_style]
        
        system_prompt = f"""你是一位专业的保险营销内容创作专家，擅长将社会热点事件转化为有温度、有价值的保险营销文案。

**创作要求：**
1. 内容结构：采用{recommended_template}结构，包含{' -> '.join(template_structure)}
2. 语言风格：{style_description}
3. 内容长度：300-500字，分为3-4个自然段
4. 情感共鸣：从事件中提炼出普通人能感同身受的情感点
5. 价值主张：自然融入保险产品的保障价值，避免生硬推销

**内容原则：**
- 真实性：基于真实事件，不夸大不虚构
- 适度性：对敏感事件保持适当的情感尺度
- 实用性：提供有价值的风险提醒和保障建议
- 合规性：符合保险营销规范，不做绝对承诺

**输出格式：**
请按以下JSON格式输出：
{{
  "标题": "吸引眼球的标题",
  "正文": "完整的营销文案正文",
  "核心卖点": "一句话总结产品价值",
  "行动引导": "引导用户下一步行动的文案"
}}

不要输出其他解释性文字，只输出JSON格式的内容。"""
        
        return system_prompt
    
    def _build_creation_user_input(self, materials: Dict[str, Any]) -> str:
        """
        构建内容创作的用户输入
        
        Args:
            materials: 创作素材
            
        Returns:
            用户输入文本
        """
        event_info = materials.get("event_info", {})
        risk_analysis = materials.get("risk_analysis", {})
        products = materials.get("recommended_products", [])
        supplementary = materials.get("supplementary_materials", {})
        examples = materials.get("examples", [])
        
        user_input_parts = []
        
        # 热点事件信息
        user_input_parts.append("**热点事件信息：**")
        user_input_parts.append(f"标题：{event_info.get('title', '')}")
        if event_info.get('content'):
            user_input_parts.append(f"内容：{event_info['content']}")
        
        # 风险分析
        user_input_parts.append("\n**风险分析结果：**")
        user_input_parts.append(f"涉及人群：{risk_analysis.get('涉及人群', '')}")
        user_input_parts.append(f"风险类型：{risk_analysis.get('风险类型', '')}")
        
        # 推荐产品
        if products:
            user_input_parts.append("\n**推荐保险产品：**")
            for i, product in enumerate(products[:2], 1):  # 最多使用前2个产品
                user_input_parts.append(f"{i}. {product.get('产品名称', '')}")
                user_input_parts.append(f"   类别：{product.get('产品类别', '')}")
                user_input_parts.append(f"   保障：{product.get('保障内容', '')}")
                user_input_parts.append(f"   理由：{product.get('推荐理由', '')}")
        
        # 补充素材
        supplementary_texts = supplementary.get("texts", [])
        if supplementary_texts:
            user_input_parts.append("\n**补充素材参考：**")
            for i, text in enumerate(supplementary_texts[:2], 1):
                if isinstance(text, dict):
                    content = text.get('content', str(text))
                else:
                    content = str(text)
                user_input_parts.append(f"{i}. {content[:200]}...")
        
        # 创作示例
        if examples:
            user_input_parts.append("\n**优秀文案示例参考：**")
            for i, example in enumerate(examples[:1], 1):  # 只用1个示例避免过长
                example_content = example.get('content', '')
                if example_content:
                    user_input_parts.append(f"示例{i}：{example_content[:300]}...")
        
        user_input_parts.append("\n请基于以上信息创作一份优质的保险营销文案。")
        
        return "\n".join(user_input_parts)
    
    def _structure_generated_content(self, raw_content: str, materials: Dict[str, Any]) -> Dict[str, Any]:
        """
        结构化生成的内容
        
        Args:
            raw_content: 原始生成内容
            materials: 创作素材
            
        Returns:
            结构化的内容
        """
        try:
            # 尝试解析JSON格式
            content_data = json.loads(raw_content)
            
            # 验证必要字段
            required_fields = ["标题", "正文", "核心卖点", "行动引导"]
            for field in required_fields:
                if field not in content_data:
                    content_data[field] = ""
            
            # 添加元数据
            content_data["创作时间"] = materials.get("creation_context", {}).get("timestamp")
            content_data["源事件标题"] = materials.get("event_info", {}).get("title", "")
            content_data["涉及人群"] = materials.get("risk_analysis", {}).get("涉及人群", "")
            content_data["风险类型"] = materials.get("risk_analysis", {}).get("风险类型", "")
            
            # 统计信息
            content_data["字数统计"] = len(content_data.get("正文", ""))
            
            return content_data
            
        except json.JSONDecodeError:
            # JSON解析失败，尝试提取关键内容
            self.logger.warning("⚠️ JSON解析失败，尝试文本解析")
            return self._extract_content_from_text(raw_content, materials)
    
    def _extract_content_from_text(self, raw_content: str, materials: Dict[str, Any]) -> Dict[str, Any]:
        """
        从普通文本中提取结构化内容
        
        Args:
            raw_content: 原始文本内容
            materials: 创作素材
            
        Returns:
            结构化内容
        """
        # 简单的文本分析和结构化
        lines = [line.strip() for line in raw_content.split('\n') if line.strip()]
        
        # 尝试识别标题（通常是第一行或包含特定标识）
        title = ""
        content_lines = []
        
        for i, line in enumerate(lines):
            if i == 0 and len(line) < 50:  # 第一行且较短，可能是标题
                title = line
            elif any(keyword in line for keyword in ["标题", "题目"]):
                title = line.split("：")[-1] if "：" in line else line
            else:
                content_lines.append(line)
        
        # 如果没有找到标题，使用事件标题
        if not title:
            title = f"关于{materials.get('event_info', {}).get('title', '')[:20]}的保险提醒"
        
        main_content = "\n\n".join(content_lines) if content_lines else raw_content
        
        return {
            "标题": title,
            "正文": main_content,
            "核心卖点": "专业保障，贴心守护",
            "行动引导": "了解更多保障方案，请咨询专业顾问",
            "创作时间": None,
            "源事件标题": materials.get("event_info", {}).get("title", ""),
            "涉及人群": materials.get("risk_analysis", {}).get("涉及人群", ""),
            "风险类型": materials.get("risk_analysis", {}).get("风险类型", ""),
            "字数统计": len(main_content),
            "解析方式": "文本提取"
        }
    
    def _update_event_content(self, event_id: str, content: Dict[str, Any]) -> bool:
        """
        更新事件的营销内容
        
        Args:
            event_id: 事件ID
            content: 营销内容
            
        Returns:
            更新是否成功
        """
        try:
            update_data = {
                "marketing_content": content,
                "marketing_content_generated": True,
                "content_created_at": content.get("创作时间")
            }
            
            success = self.es.update_by_id(
                index=self.index_name,
                doc_id=event_id,
                doc=update_data
            )
            
            if success:
                word_count = content.get("字数统计", 0)
                self.logger.info(f"✅ 营销内容已生成: {event_id}, 字数: {word_count}")
                return True
            else:
                self.logger.error(f"❌ 营销内容更新失败: {event_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 更新营销内容异常: {event_id}, {e}")
            return False
    
    def create_custom_content(self, 
                            event_title: str,
                            crowd_type: str,
                            risk_type: str,
                            products: List[Dict],
                            style: str = "亲切温和",
                            template: str = "三段式") -> Optional[Dict[str, Any]]:
        """
        自定义内容创作
        
        Args:
            event_title: 事件标题
            crowd_type: 人群类型
            risk_type: 风险类型
            products: 产品列表
            style: 内容风格
            template: 内容模板
            
        Returns:
            生成的内容
        """
        try:
            # 构建虚拟素材
            materials = {
                "event_info": {"title": event_title},
                "risk_analysis": {"涉及人群": crowd_type, "风险类型": risk_type},
                "recommended_products": products,
                "content_analysis": {
                    "target_style": style,
                    "recommended_template": template
                },
                "supplementary_materials": {},
                "examples": []
            }
            
            # 生成内容
            content = self._generate_marketing_content(materials)
            
            if content:
                self.logger.info(f"✅ 自定义内容创作成功: {event_title}")
                return content
            else:
                self.logger.error(f"❌ 自定义内容创作失败: {event_title}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 自定义内容创作异常: {e}")
            return None
    
    def get_content_stats(self) -> Dict[str, Any]:
        """
        获取内容创作统计信息
        
        Returns:
            统计信息
        """
        try:
            # 统计已创作内容的事件数量
            created_query = {"term": {"marketing_content_generated": True}}
            created_count = self.es.count(self.index_name, created_query)
            
            # 统计待创作的事件数量
            pending_query = {
                "bool": {
                    "must": [
                        {"term": {"material_collected": True}},
                        {"term": {"product_matched": True}}
                    ],
                    "must_not": [
                        {"exists": {"field": "marketing_content"}}
                    ]
                }
            }
            pending_count = self.es.count(self.index_name, pending_query)
            
            # 统计不同风格的内容数量
            style_stats = {}
            for style in self.content_styles.keys():
                style_query = {"match": {"marketing_content.target_style": style}}
                style_count = self.es.count(self.index_name, style_query)
                style_stats[style] = style_count
            
            return {
                "created_content_count": created_count,
                "pending_content_count": pending_count,
                "available_styles": list(self.content_styles.keys()),
                "available_templates": list(self.content_templates.keys()),
                "style_distribution": style_stats
            }
            
        except Exception as e:
            self.logger.error(f"❌ 获取内容统计失败: {e}")
            return {}