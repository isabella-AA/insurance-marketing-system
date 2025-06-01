import os
from typing import List, Dict, Any, Optional
from .base_agent import BaseAgent
from utils.vector_utils import VectorUtils
from dotenv import load_dotenv

load_dotenv('config/.env')

class ProductMatcherAgent(BaseAgent):
    """
    产品匹配智能体
    负责根据风险分析结果匹配合适的保险产品
    """
    
    def __init__(self):
        super().__init__("ProductMatcher")
        
        # 配置参数
        self.event_index = os.getenv("HOT_EVENT_INDEX", "hoteventdb")
        self.product_index = os.getenv("PRODUCT_INDEX", "insurance-products")
        self.batch_size = int(os.getenv("PRODUCT_MATCHER_BATCH_SIZE", 5))
        self.top_k = int(os.getenv("TOP_K_PRODUCTS", 3))
        self.similarity_threshold = float(os.getenv("PRODUCT_SIMILARITY_THRESHOLD", 0.6))
        
        # 初始化向量工具
        self.vector_utils = VectorUtils()
        
        # 产品向量缓存
        self.product_vectors = None
        self._load_products_with_vectors()
        
        # 风险-产品类别映射
        self.risk_product_mapping = {
            "健康风险": ["重疾险", "医疗险", "健康险"],
            "意外伤害": ["意外险", "综合意外险"],
            "财产损失": ["财产险", "家财险", "车险"],
            "出行安全": ["意外险", "旅游险", "交通意外险"],
            "法律责任": ["责任险", "职业责任险"],
            "火灾风险": ["财产险", "家财险"],
            "网络安全": ["网络安全险", "信息安全险"],
            "自然灾害": ["财产险", "灾害险"]
        }
        
        # 人群-产品匹配规则
        self.crowd_product_rules = {
            "老年人": {"age_max": 70, "preferred_categories": ["医疗险", "重疾险", "意外险"]},
            "儿童": {"age_max": 18, "preferred_categories": ["重疾险", "医疗险", "教育险"]},
            "中年人": {"age_range": [30, 55], "preferred_categories": ["重疾险", "寿险", "医疗险"]},
            "司机": {"occupation": "driver", "preferred_categories": ["意外险", "车险"]},
            "孕妇": {"special_group": "pregnant", "preferred_categories": ["母婴险", "医疗险"]},
            "游客": {"scenario": "travel", "preferred_categories": ["旅游险", "意外险"]},
            "病患": {"health_condition": "sick", "preferred_categories": ["医疗险", "护理险"]}
        }
        
        self.logger.info(f"✅ 产品匹配器初始化完成，产品数量: {len(self.product_vectors) if self.product_vectors else 0}")
    
    def run_once(self) -> str:
        """
        执行一次产品匹配任务
        
        Returns:
            处理结果描述
        """
        # 获取待匹配产品的事件
        events = self._fetch_events_for_matching()
        
        if not events:
            self.logger.info("⚠️ 暂无待匹配产品的事件")
            return "无待处理事件"
        
        # 处理事件
        success_count = 0
        total_count = len(events)
        
        for event in events:
            try:
                if self._match_products_for_event(event):
                    success_count += 1
                    
            except Exception as e:
                self.logger.error(f"❌ 产品匹配失败: {event.get('title', 'Unknown')}, {e}")
        
        result = f"产品匹配完成: {success_count}/{total_count} 成功"
        self.logger.info(f"📊 {result}")
        return result
    
    def _load_products_with_vectors(self):
        """加载所有保险产品及其向量"""
        try:
            # 从ES加载产品数据
            products = self.es.search(
                index=self.product_index,
                query={"match_all": {}},
                size=1000
            )
            
            if not products:
                self.logger.warning("⚠️ 未找到任何保险产品数据")
                self.product_vectors = []
                return
            
            self.logger.info(f"📥 加载了 {len(products)} 个保险产品")
            
            # 构建产品描述文本
            descriptions = []
            for product in products:
                desc_parts = []
                
                # 添加产品名称
                if product.get('product_name'):
                    desc_parts.append(product['product_name'])
                
                # 添加类别
                if product.get('category'):
                    desc_parts.append(product['category'])
                
                # 添加适用人群
                if product.get('age_range'):
                    desc_parts.append(f"适用年龄:{product['age_range']}")
                
                # 添加保障内容
                if product.get('coverage'):
                    desc_parts.append(product['coverage'])
                
                # 添加特色描述
                if product.get('features'):
                    desc_parts.append(product['features'])
                
                description = " ".join(desc_parts)
                descriptions.append(description)
            
            # 批量生成向量
            self.logger.info("🔄 正在生成产品向量...")
            vectors = self.vector_utils.embed_batch(descriptions, show_progress=True)
            
            # 构建产品向量数据
            self.product_vectors = []
            for i, product in enumerate(products):
                self.product_vectors.append({
                    "product": product,
                    "vector": vectors[i],
                    "description": descriptions[i]
                })
            
            self.logger.info(f"✅ 产品向量生成完成: {len(self.product_vectors)} 个")
            
        except Exception as e:
            self.logger.error(f"❌ 加载产品向量失败: {e}")
            self.product_vectors = []
    
    def _fetch_events_for_matching(self) -> List[Dict[str, Any]]:
        """
        获取待匹配产品的事件
        
        Returns:
            事件列表
        """
        try:
            query = {
                "bool": {
                    "must": [
                        {"term": {"risk_analyzed": True}},
                        {"exists": {"field": "risk_element"}}
                    ],
                    "must_not": [
                        {"exists": {"field": "recommended_products"}}
                    ]
                }
            }
            
            events = self.es.search(
                index=self.event_index,
                query=query,
                size=self.batch_size
            )
            
            self.logger.debug(f"🔍 获取到 {len(events)} 个待匹配产品的事件")
            return events
            
        except Exception as e:
            self.logger.error(f"❌ 获取待匹配事件失败: {e}")
            return []
    
    def _match_products_for_event(self, event: Dict[str, Any]) -> bool:
        """
        为单个事件匹配产品
        
        Args:
            event: 事件数据
            
        Returns:
            是否匹配成功
        """
        title = event.get("title", "")
        risk_element = event.get("risk_element", {})
        event_id = event.get("_id")
        
        if not risk_element:
            self.logger.warning(f"⚠️ 事件缺少风险要素: {event_id}")
            return False
        
        crowd_type = risk_element.get("涉及人群", "")
        risk_type = risk_element.get("风险类型", "")
        
        self.logger.info(f"🎯 正在匹配产品: {title[:50]}... (人群:{crowd_type}, 风险:{risk_type})")
        
        # 执行产品匹配
        matched_products = self._perform_product_matching(crowd_type, risk_type, title)
        
        if matched_products:
            # 更新事件记录
            return self._update_event_products(event_id, matched_products)
        else:
            self.logger.warning(f"⚠️ 未找到匹配的产品: {title}")
            # 标记为已处理但无匹配结果
            return self._mark_no_match(event_id)
    
    def _perform_product_matching(self, crowd_type: str, risk_type: str, title: str) -> List[Dict[str, Any]]:
        """
        执行产品匹配逻辑
        
        Args:
            crowd_type: 人群类型
            risk_type: 风险类型
            title: 事件标题
            
        Returns:
            匹配的产品列表
        """
        if not self.product_vectors:
            self.logger.warning("⚠️ 产品向量数据未加载")
            return []
        
        try:
            # 1. 基于规则的初步筛选
            rule_candidates = self._filter_by_rules(crowd_type, risk_type)
            
            # 2. 基于向量相似度的精确匹配
            vector_candidates = self._match_by_vector_similarity(crowd_type, risk_type, title)
            
            # 3. 合并和重排序结果
            final_matches = self._merge_and_rank_candidates(rule_candidates, vector_candidates)
            
            # 4. 生成匹配理由
            enriched_matches = self._enrich_with_reasons(final_matches, crowd_type, risk_type)
            
            return enriched_matches[:self.top_k]
            
        except Exception as e:
            self.logger.error(f"❌ 产品匹配异常: {e}")
            return []
    
    def _filter_by_rules(self, crowd_type: str, risk_type: str) -> List[Dict[str, Any]]:
        """
        基于规则筛选候选产品
        
        Args:
            crowd_type: 人群类型
            risk_type: 风险类型
            
        Returns:
            候选产品列表
        """
        candidates = []
        
        # 获取风险对应的产品类别
        preferred_categories = self.risk_product_mapping.get(risk_type, [])
        
        # 获取人群偏好的产品类别
        crowd_rules = self.crowd_product_rules.get(crowd_type, {})
        crowd_categories = crowd_rules.get("preferred_categories", [])
        
        # 合并类别偏好
        target_categories = list(set(preferred_categories + crowd_categories))
        
        for product_item in self.product_vectors:
            product = product_item["product"]
            product_category = product.get("category", "")
            
            # 检查类别匹配
            category_match = any(cat in product_category for cat in target_categories)
            
            # 检查人群适用性
            crowd_match = self._check_crowd_suitability(product, crowd_type)
            
            if category_match or crowd_match:
                score = 0
                if category_match:
                    score += 0.7
                if crowd_match:
                    score += 0.3
                
                candidates.append({
                    "product": product,
                    "score": score,
                    "match_type": "rule",
                    "vector": product_item["vector"]
                })
        
        # 按评分排序
        candidates.sort(key=lambda x: x["score"], reverse=True)
        
        self.logger.debug(f"📋 规则筛选得到 {len(candidates)} 个候选产品")
        return candidates
    
    def _match_by_vector_similarity(self, crowd_type: str, risk_type: str, title: str) -> List[Dict[str, Any]]:
        """
        基于向量相似度匹配产品
        
        Args:
            crowd_type: 人群类型
            risk_type: 风险类型
            title: 事件标题
            
        Returns:
            匹配的产品列表
        """
        try:
            # 构建查询文本
            query_parts = []
            if crowd_type and crowd_type != "一般人群":
                query_parts.append(crowd_type)
            if risk_type and risk_type != "无明显风险":
                query_parts.append(risk_type)
            if title:
                query_parts.append(title[:50])  # 限制标题长度
            
            query_text = " ".join(query_parts)
            
            if not query_text.strip():
                return []
            
            # 生成查询向量
            query_vector = self.vector_utils.embed(query_text)
            
            # 计算与所有产品的相似度
            similarities = []
            for product_item in self.product_vectors:
                similarity = self.vector_utils.cosine_similarity(
                    query_vector, 
                    product_item["vector"]
                )
                
                if similarity >= self.similarity_threshold:
                    similarities.append({
                        "product": product_item["product"],
                        "score": similarity,
                        "match_type": "vector",
                        "vector": product_item["vector"]
                    })
            
            # 按相似度排序
            similarities.sort(key=lambda x: x["score"], reverse=True)
            
            self.logger.debug(f"🔍 向量匹配得到 {len(similarities)} 个候选产品")
            return similarities
            
        except Exception as e:
            self.logger.error(f"❌ 向量相似度匹配失败: {e}")
            return []
    
    def _check_crowd_suitability(self, product: Dict[str, Any], crowd_type: str) -> bool:
        """
        检查产品是否适合特定人群
        
        Args:
            product: 产品信息
            crowd_type: 人群类型
            
        Returns:
            是否适合
        """
        age_range = product.get("age_range", "")
        features = product.get("features", "")
        coverage = product.get("coverage", "")
        
        # 文本匹配检查
        text_content = f"{age_range} {features} {coverage}".lower()
        crowd_keywords = {
            "老年人": ["老年", "老人", "长者", "50", "60", "70"],
            "儿童": ["儿童", "小孩", "孩子", "学生", "18岁以下"],
            "中年人": ["中年", "成年", "30-60", "职场"],
            "司机": ["司机", "驾驶", "车主", "开车"],
            "孕妇": ["孕妇", "孕期", "母婴", "生育"],
            "游客": ["旅游", "出行", "旅客"],
            "病患": ["医疗", "健康", "疾病", "治疗"]
        }
        
        keywords = crowd_keywords.get(crowd_type, [])
        return any(keyword in text_content for keyword in keywords)
    
    def _merge_and_rank_candidates(self, rule_candidates: List[Dict], vector_candidates: List[Dict]) -> List[Dict]:
        """
        合并和重排序候选产品
        
        Args:
            rule_candidates: 规则筛选的候选产品
            vector_candidates: 向量匹配的候选产品
            
        Returns:
            合并后的候选产品列表
        """
        # 使用产品ID去重
        product_scores = {}
        
        # 处理规则候选产品
        for candidate in rule_candidates:
            product_id = candidate["product"].get("_id") or candidate["product"].get("product_name")
            if product_id not in product_scores:
                product_scores[product_id] = {
                    "product": candidate["product"],
                    "rule_score": candidate["score"],
                    "vector_score": 0,
                    "vector": candidate["vector"]
                }
            else:
                product_scores[product_id]["rule_score"] = max(
                    product_scores[product_id]["rule_score"], 
                    candidate["score"]
                )
        
        # 处理向量候选产品
        for candidate in vector_candidates:
            product_id = candidate["product"].get("_id") or candidate["product"].get("product_name")
            if product_id not in product_scores:
                product_scores[product_id] = {
                    "product": candidate["product"],
                    "rule_score": 0,
                    "vector_score": candidate["score"],
                    "vector": candidate["vector"]
                }
            else:
                product_scores[product_id]["vector_score"] = max(
                    product_scores[product_id]["vector_score"], 
                    candidate["score"]
                )
        
        # 计算综合评分
        final_candidates = []
        for product_id, data in product_scores.items():
            # 综合评分：规则权重0.4，向量权重0.6
            combined_score = data["rule_score"] * 0.4 + data["vector_score"] * 0.6
            
            final_candidates.append({
                "product": data["product"],
                "combined_score": combined_score,
                "rule_score": data["rule_score"],
                "vector_score": data["vector_score"]
            })
        
        # 按综合评分排序
        final_candidates.sort(key=lambda x: x["combined_score"], reverse=True)
        
        self.logger.debug(f"🔀 合并后得到 {len(final_candidates)} 个候选产品")
        return final_candidates
    
    def _enrich_with_reasons(self, candidates: List[Dict], crowd_type: str, risk_type: str) -> List[Dict]:
        """
        为匹配结果添加推荐理由
        
        Args:
            candidates: 候选产品列表
            crowd_type: 人群类型
            risk_type: 风险类型
            
        Returns:
            enriched产品列表
        """
        enriched = []
        
        for candidate in candidates:
            product = candidate["product"]
            rule_score = candidate.get("rule_score", 0)
            vector_score = candidate.get("vector_score", 0)
            
            # 生成推荐理由
            reasons = []
            
            # 基于人群匹配的理由
            if crowd_type and crowd_type != "一般人群":
                if self._check_crowd_suitability(product, crowd_type):
                    reasons.append(f"专为{crowd_type}设计")
            
            # 基于风险匹配的理由
            if risk_type and risk_type != "无明显风险":
                product_category = product.get("category", "")
                preferred_categories = self.risk_product_mapping.get(risk_type, [])
                if any(cat in product_category for cat in preferred_categories):
                    reasons.append(f"针对{risk_type}提供保障")
            
            # 基于评分的理由
            if rule_score > 0.5:
                reasons.append("符合专业推荐规则")
            if vector_score > 0.7:
                reasons.append("与事件高度相关")
            
            # 如果没有具体理由，添加通用理由
            if not reasons:
                reasons.append("综合评估推荐")
            
            enriched.append({
                "产品名称": product.get("product_name", ""),
                "产品类别": product.get("category", ""),
                "保障内容": product.get("coverage", ""),
                "适用人群": product.get("age_range", ""),
                "推荐理由": "；".join(reasons),
                "匹配评分": round(candidate["combined_score"], 3)
            })
        
        return enriched
    
    def _update_event_products(self, event_id: str, products: List[Dict[str, Any]]) -> bool:
        """
        更新事件的推荐产品信息
        
        Args:
            event_id: 事件ID
            products: 推荐产品列表
            
        Returns:
            更新是否成功
        """
        try:
            update_data = {
                "recommended_products": products,
                "product_matched": True
            }
            
            success = self.es.update_by_id(
                index=self.event_index,
                doc_id=event_id,
                doc=update_data
            )
            
            if success:
                self.logger.info(f"✅ 产品推荐已更新: {event_id}, 推荐 {len(products)} 款产品")
                return True
            else:
                self.logger.error(f"❌ 产品推荐更新失败: {event_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 更新产品推荐异常: {event_id}, {e}")
            return False
    
    def _mark_no_match(self, event_id: str) -> bool:
        """
        标记无匹配产品
        
        Args:
            event_id: 事件ID
            
        Returns:
            标记是否成功
        """
        try:
            update_data = {
                "recommended_products": [],
                "product_matched": True,
                "no_product_match": True
            }
            
            return self.es.update_by_id(
                index=self.event_index,
                doc_id=event_id,
                doc=update_data
            )
            
        except Exception as e:
            self.logger.error(f"❌ 标记无匹配产品异常: {event_id}, {e}")
            return False
    
    def refresh_product_vectors(self):
        """刷新产品向量缓存"""
        self.logger.info("🔄 刷新产品向量缓存...")
        self._load_products_with_vectors()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取产品匹配统计信息
        
        Returns:
            统计信息
        """
        try:
            # 统计已匹配的事件数量
            matched_query = {"term": {"product_matched": True}}
            matched_count = self.es.count(self.event_index, matched_query)
            
            # 统计无匹配的事件数量
            no_match_query = {"term": {"no_product_match": True}}
            no_match_count = self.es.count(self.event_index, no_match_query)
            
            # 统计待处理的事件数量
            pending_query = {
                "bool": {
                    "must": [
                        {"term": {"risk_analyzed": True}},
                        {"exists": {"field": "risk_element"}}
                    ],
                    "must_not": [
                        {"exists": {"field": "recommended_products"}}
                    ]
                }
            }
            pending_count = self.es.count(self.event_index, pending_query)
            
            return {
                "total_products": len(self.product_vectors) if self.product_vectors else 0,
                "matched_events": matched_count,
                "no_match_events": no_match_count,
                "pending_events": pending_count,
                "match_success_rate": round(matched_count / (matched_count + no_match_count), 2) if (matched_count + no_match_count) > 0 else 0
            }
            
        except Exception as e:
            self.logger.error(f"❌ 获取统计信息失败: {e}")
            return {}