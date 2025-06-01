import os
import time
import hashlib
import requests
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from datetime import datetime
from .base_agent import BaseAgent
from dotenv import load_dotenv

load_dotenv('config/.env')

class HotspotAgent(BaseAgent):
    """
    热点抓取智能体
    负责从多个平台抓取热点事件并写入ES数据库
    """
    
    def __init__(self):
        super().__init__("HotspotAgent")
        
        # 配置参数
        self.index_name = os.getenv("HOT_EVENT_INDEX", "hoteventdb")
        self.api_base_url = os.getenv("DAILY_HOT_API", "http://59.110.48.108:8888")
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", 15))
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))
        self.top_n = int(os.getenv("TOP_N_HOTSPOTS", 50))
        
        # 平台配置
        self.platforms = {
            "weibo": {
                "endpoint": "/weibo",
                "name": "微博热搜",
                "priority": 1,
                "enabled": True
            },
            "douyin": {
                "endpoint": "/douyin", 
                "name": "抖音热点",
                "priority": 2,
                "enabled": False  # 可配置开启
            },
            "toutiao": {
                "endpoint": "/toutiao",
                "name": "今日头条",
                "priority": 3,
                "enabled": True
            },
            "zhihu": {
                "endpoint": "/zhihu-daily",
                "name": "知乎日报", 
                "priority": 4,
                "enabled": False
            },
            "qq_news": {
                "endpoint": "/qq-news",
                "name": "腾讯新闻",
                "priority": 5,
                "enabled": False
            }
        }
        
        # 内容过滤规则
        self.filter_rules = {
            "min_title_length": 5,
            "max_title_length": 200,
            "blacklist_keywords": [
                "广告", "推广", "营销", "测试", "spam"
            ],
            "required_elements": ["title", "url"]
        }
        
        # 请求配置
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        }
        
        self.logger.info(f"✅ 热点抓取器初始化完成，目标平台: {len([p for p in self.platforms.values() if p['enabled']])} 个")
    
    def run_once(self) -> str:
        """
        执行一次热点抓取任务
        
        Returns:
            处理结果描述
        """
        total_fetched = 0
        total_stored = 0
        platform_results = {}
        
        # 获取启用的平台列表（按优先级排序）
        enabled_platforms = [
            (key, config) for key, config in self.platforms.items() 
            if config["enabled"]
        ]
        enabled_platforms.sort(key=lambda x: x[1]["priority"])
        
        self.logger.info(f"🚀 开始抓取热点，目标平台: {[p[1]['name'] for p in enabled_platforms]}")
        
        # 逐个平台抓取
        for platform_key, platform_config in enabled_platforms:
            try:
                self.logger.info(f"🔍 正在抓取 {platform_config['name']}")
                
                # 抓取平台数据
                platform_data = self._fetch_platform_data(platform_key, platform_config)
                
                if platform_data:
                    # 处理和存储数据
                    stored_count = self._process_and_store_data(platform_data, platform_key)
                    
                    total_fetched += len(platform_data)
                    total_stored += stored_count
                    platform_results[platform_config['name']] = {
                        "抓取数量": len(platform_data),
                        "存储数量": stored_count
                    }
                    
                    self.logger.info(f"✅ {platform_config['name']} 完成: 抓取 {len(platform_data)}, 存储 {stored_count}")
                else:
                    platform_results[platform_config['name']] = {"状态": "抓取失败"}
                
                # 平台间延迟，避免频率限制
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"❌ {platform_config['name']} 抓取异常: {e}")
                platform_results[platform_config['name']] = {"状态": f"异常: {str(e)[:50]}"}
        
        # 清理旧数据
        self._cleanup_old_data()
        
        result = f"热点抓取完成: 总计抓取 {total_fetched}, 新增存储 {total_stored}"
        self.logger.info(f"📊 {result}")
        self.logger.info(f"📋 平台详情: {platform_results}")
        
        return result
    
    def _fetch_platform_data(self, platform_key: str, platform_config: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """
        从指定平台抓取数据
        
        Args:
            platform_key: 平台标识
            platform_config: 平台配置
            
        Returns:
            抓取到的数据列表
        """
        url = f"{self.api_base_url}{platform_config['endpoint']}"
        
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"📤 请求 {platform_config['name']} (尝试 {attempt + 1}/{self.max_retries})")
                
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=self.request_timeout
                )
                response.raise_for_status()
                
                data = response.json()
                
                # 验证响应格式
                if not self._validate_response_format(data, platform_key):
                    self.logger.warning(f"⚠️ {platform_config['name']} 响应格式异常")
                    continue
                
                # 提取数据
                items = data.get("data", [])
                if not items:
                    self.logger.warning(f"⚠️ {platform_config['name']} 返回空数据")
                    return []
                
                # 限制数量
                limited_items = items[:self.top_n]
                
                self.logger.debug(f"📥 {platform_config['name']} 获取 {len(limited_items)} 条数据")
                return limited_items
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"⏰ {platform_config['name']} 请求超时 (尝试 {attempt + 1})")
                
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"🔌 {platform_config['name']} 连接错误 (尝试 {attempt + 1})")
                
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"❌ {platform_config['name']} HTTP错误: {e}")
                break  # HTTP错误通常不需要重试
                
            except Exception as e:
                self.logger.error(f"❌ {platform_config['name']} 未知错误: {e}")
                break
            
            # 重试延迟
            if attempt < self.max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避
        
        return None
    
    def _validate_response_format(self, data: Dict[str, Any], platform_key: str) -> bool:
        """
        验证API响应格式
        
        Args:
            data: 响应数据
            platform_key: 平台标识
            
        Returns:
            格式是否有效
        """
        try:
            # 基本结构检查
            if not isinstance(data, dict):
                return False
            
            if "data" not in data:
                return False
            
            items = data["data"]
            if not isinstance(items, list):
                return False
            
            # 检查数据项格式
            if items:
                sample_item = items[0]
                required_fields = self.filter_rules["required_elements"]
                
                for field in required_fields:
                    if field not in sample_item:
                        self.logger.warning(f"⚠️ 缺少必要字段: {field}")
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 响应格式验证异常: {e}")
            return False
    
    def _process_and_store_data(self, raw_data: List[Dict[str, Any]], platform_key: str) -> int:
        """
        处理并存储数据
        
        Args:
            raw_data: 原始数据
            platform_key: 平台标识
            
        Returns:
            实际存储的数量
        """
        processed_items = []
        
        for item in raw_data:
            try:
                # 数据清洗和标准化
                processed_item = self._process_single_item(item, platform_key)
                
                if processed_item and self._validate_item(processed_item):
                    processed_items.append(processed_item)
                    
            except Exception as e:
                self.logger.debug(f"❌ 处理单项数据失败: {e}")
                continue
        
        # 批量存储
        if processed_items:
            return self._batch_store_items(processed_items)
        else:
            return 0
    
    def _process_single_item(self, item: Dict[str, Any], platform_key: str) -> Optional[Dict[str, Any]]:
        """
        处理单个数据项
        
        Args:
            item: 原始数据项
            platform_key: 平台标识
            
        Returns:
            处理后的数据项
        """
        try:
            # 提取基础字段
            title = str(item.get("title", "")).strip()
            url = str(item.get("url", "")).strip()
            desc = str(item.get("desc", "")).strip()
            
            if not title or not url:
                return None
            
            # 标题长度检查
            if len(title) < self.filter_rules["min_title_length"] or \
               len(title) > self.filter_rules["max_title_length"]:
                return None
            
            # 黑名单关键词检查
            title_lower = title.lower()
            if any(keyword in title_lower for keyword in self.filter_rules["blacklist_keywords"]):
                return None
            
            # URL标准化
            normalized_url = self._normalize_url(url)
            
            # 生成唯一ID
            unique_id = self._generate_unique_id(platform_key, title, normalized_url)
            
            # 构建标准化数据结构
            processed_item = {
                "id": unique_id,
                "title": title,
                "url": normalized_url,
                "content": desc,
                "platform": platform_key,
                "platform_name": self.platforms[platform_key]["name"],
                "crawled_at": datetime.now().isoformat(),
                "rank": item.get("rank", 0),
                "hot_score": item.get("hot", 0),
                "extra_data": {
                    key: value for key, value in item.items() 
                    if key not in ["title", "url", "desc", "rank", "hot"]
                }
            }
            
            return processed_item
            
        except Exception as e:
            self.logger.debug(f"❌ 单项处理异常: {e}")
            return None
    
    def _validate_item(self, item: Dict[str, Any]) -> bool:
        """
        验证处理后的数据项
        
        Args:
            item: 处理后的数据项
            
        Returns:
            是否有效
        """
        try:
            # 必要字段检查
            required_fields = ["id", "title", "url", "platform", "crawled_at"]
            for field in required_fields:
                if field not in item or not item[field]:
                    return False
            
            # URL有效性检查
            if not self._is_valid_url(item["url"]):
                return False
            
            return True
            
        except Exception:
            return False
    
    def _normalize_url(self, url: str) -> str:
        """
        标准化URL
        
        Args:
            url: 原始URL
            
        Returns:
            标准化后的URL
        """
        try:
            # 处理相对URL
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                url = "https://weibo.com" + url  # 默认微博域名
            elif not url.startswith(("http://", "https://")):
                url = "https://" + url
            
            # URL解析和清理
            parsed = urlparse(url)
            
            # 移除常见的跟踪参数
            query_params_to_remove = ["utm_source", "utm_medium", "utm_campaign", "from", "share"]
            
            # 这里可以添加更多的URL清理逻辑
            
            return url
            
        except Exception:
            return url
    
    def _is_valid_url(self, url: str) -> bool:
        """
        检查URL是否有效
        
        Args:
            url: URL字符串
            
        Returns:
            是否有效
        """
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def _generate_unique_id(self, platform: str, title: str, url: str) -> str:
        """
        生成唯一ID
        
        Args:
            platform: 平台标识
            title: 标题
            url: URL
            
        Returns:
            唯一ID
        """
        # 使用平台+标题+URL的组合生成MD5哈希
        content = f"{platform}-{title}-{url}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _batch_store_items(self, items: List[Dict[str, Any]]) -> int:
        """
        批量存储数据项
        
        Args:
            items: 数据项列表
            
        Returns:
            实际存储的数量
        """
        if not items:
            return 0
        
        try:
            # 检查重复项
            new_items = []
            existing_count = 0
            
            for item in items:
                if not self.es.exists(self.index_name, item["id"]):
                    new_items.append(item)
                else:
                    existing_count += 1
            
            if existing_count > 0:
                self.logger.debug(f"📋 跳过 {existing_count} 个已存在的项目")
            
            # 批量插入新项目
            if new_items:
                doc_ids = [item["id"] for item in new_items]
                # 移除id字段，因为ES会自动处理
                docs_for_insert = []
                for item in new_items:
                    doc = item.copy()
                    doc.pop("id", None)
                    docs_for_insert.append(doc)
                
                success_count = self.es.bulk_index(
                    self.index_name, 
                    docs_for_insert, 
                    doc_ids
                )
                
                self.logger.debug(f"💾 批量存储完成: {success_count}/{len(new_items)} 成功")
                return success_count
            else:
                self.logger.debug("📋 没有新数据需要存储")
                return 0
                
        except Exception as e:
            self.logger.error(f"❌ 批量存储失败: {e}")
            return 0
    
    def _cleanup_old_data(self, days_to_keep: int = 7):
        """
        清理过期数据
        
        Args:
            days_to_keep: 保留天数
        """
        try:
            # 计算截止日期
            from datetime import datetime, timedelta
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
            
            # 查询过期数据
            query = {
                "range": {
                    "crawled_at": {
                        "lt": cutoff_date
                    }
                }
            }
            
            # 这里可以添加删除逻辑，但要谨慎
            # 建议先统计数量，确认后再删除
            old_count = self.es.count(self.index_name, query)
            
            if old_count > 0:
                self.logger.info(f"🗑️ 发现 {old_count} 条过期数据 (>{days_to_keep}天)")
                # 实际删除逻辑可以根据需要开启
                # self.es.delete_by_query(self.index_name, query)
            
        except Exception as e:
            self.logger.warning(f"⚠️ 清理过期数据失败: {e}")
    
    def get_platform_status(self) -> Dict[str, Any]:
        """
        获取平台状态信息
        
        Returns:
            平台状态
        """
        status = {}
        
        for platform_key, platform_config in self.platforms.items():
            try:
                if platform_config["enabled"]:
                    # 测试平台连接
                    url = f"{self.api_base_url}{platform_config['endpoint']}"
                    response = requests.get(url, headers=self.headers, timeout=5)
                    
                    if response.status_code == 200:
                        data = response.json()
                        item_count = len(data.get("data", []))
                        status[platform_config["name"]] = {
                            "状态": "正常",
                            "可用数据": item_count,
                            "响应时间": f"{response.elapsed.total_seconds():.2f}s"
                        }
                    else:
                        status[platform_config["name"]] = {
                            "状态": f"异常 (HTTP {response.status_code})"
                        }
                else:
                    status[platform_config["name"]] = {"状态": "已禁用"}
                    
            except Exception as e:
                status[platform_config["name"]] = {"状态": f"连接失败: {str(e)[:50]}"}
        
        return status
    
    def toggle_platform(self, platform_key: str, enabled: bool) -> bool:
        """
        启用/禁用平台
        
        Args:
            platform_key: 平台标识
            enabled: 是否启用
            
        Returns:
            操作是否成功
        """
        if platform_key in self.platforms:
            self.platforms[platform_key]["enabled"] = enabled
            status = "启用" if enabled else "禁用"
            self.logger.info(f"🔧 {status}平台: {self.platforms[platform_key]['name']}")
            return True
        else:
            self.logger.error(f"❌ 未知平台: {platform_key}")
            return False
    
    def fetch_single_platform(self, platform_key: str) -> Dict[str, Any]:
        """
        单独抓取指定平台
        
        Args:
            platform_key: 平台标识
            
        Returns:
            抓取结果
        """
        if platform_key not in self.platforms:
            return {"error": f"未知平台: {platform_key}"}
        
        platform_config = self.platforms[platform_key]
        
        try:
            self.logger.info(f"🎯 单独抓取 {platform_config['name']}")
            
            # 抓取数据
            platform_data = self._fetch_platform_data(platform_key, platform_config)
            
            if platform_data:
                # 处理和存储
                stored_count = self._process_and_store_data(platform_data, platform_key)
                
                result = {
                    "platform": platform_config["name"],
                    "status": "成功",
                    "fetched": len(platform_data),
                    "stored": stored_count,
                    "skipped": len(platform_data) - stored_count
                }
                
                self.logger.info(f"✅ {platform_config['name']} 单独抓取完成: {result}")
                return result
            else:
                return {
                    "platform": platform_config["name"],
                    "status": "失败",
                    "error": "数据抓取失败"
                }
                
        except Exception as e:
            self.logger.error(f"❌ {platform_config['name']} 单独抓取异常: {e}")
            return {
                "platform": platform_config["name"],
                "status": "异常",
                "error": str(e)
            }
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取抓取统计信息
        
        Returns:
            统计信息
        """
        try:
            # 总数统计
            total_query = {"match_all": {}}
            total_count = self.es.count(self.index_name, total_query)
            
            # 按平台统计
            platform_stats = {}
            for platform_key, platform_config in self.platforms.items():
                platform_query = {"term": {"platform": platform_key}}
                platform_count = self.es.count(self.index_name, platform_query)
                platform_stats[platform_config["name"]] = platform_count
            
            # 今日新增统计
            from datetime import datetime, timedelta
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_query = {
                "range": {
                    "crawled_at": {
                        "gte": today_start.isoformat()
                    }
                }
            }
            today_count = self.es.count(self.index_name, today_query)
            
            # 热度TOP10
            top_hot_query = {
                "match_all": {}
            }
            top_hot_events = self.es.search(
                self.index_name, 
                top_hot_query, 
                size=10,
                sort=[{"hot_score": {"order": "desc"}}]
            )
            
            return {
                "总热点数量": total_count,
                "今日新增": today_count,
                "平台分布": platform_stats,
                "平台状态": {
                    name: "启用" if config["enabled"] else "禁用"
                    for name, config in [(config["name"], config) for config in self.platforms.values()]
                },
                "热度TOP10": [
                    {
                        "标题": event.get("title", "")[:50],
                        "平台": event.get("platform_name", ""),
                        "热度": event.get("hot_score", 0)
                    }
                    for event in top_hot_events[:10]
                ],
                "配置信息": {
                    "API地址": self.api_base_url,
                    "抓取数量": self.top_n,
                    "超时时间": self.request_timeout,
                    "重试次数": self.max_retries
                }
            }
            
        except Exception as e:
            self.logger.error(f"❌ 获取统计信息失败: {e}")
            return {"error": str(e)}
    
    def search_hotspots(self, 
                       keyword: str, 
                       platform: Optional[str] = None,
                       limit: int = 20) -> List[Dict[str, Any]]:
        """
        搜索热点事件
        
        Args:
            keyword: 搜索关键词
            platform: 指定平台（可选）
            limit: 结果数量限制
            
        Returns:
            搜索结果
        """
        try:
            # 构建查询
            query_conditions = [
                {"multi_match": {
                    "query": keyword,
                    "fields": ["title^2", "content"],
                    "type": "best_fields"
                }}
            ]
            
            if platform:
                query_conditions.append({"term": {"platform": platform}})
            
            search_query = {
                "bool": {
                    "must": query_conditions
                }
            }
            
            # 执行搜索
            results = self.es.search(
                self.index_name,
                search_query,
                size=limit,
                sort=[{"hot_score": {"order": "desc"}}, {"crawled_at": {"order": "desc"}}]
            )
            
            # 格式化结果
            formatted_results = []
            for result in results:
                formatted_results.append({
                    "标题": result.get("title", ""),
                    "平台": result.get("platform_name", ""),
                    "URL": result.get("url", ""),
                    "内容": result.get("content", "")[:100] + "..." if result.get("content") else "",
                    "热度": result.get("hot_score", 0),
                    "抓取时间": result.get("crawled_at", ""),
                    "排名": result.get("rank", 0)
                })
            
            self.logger.info(f"🔍 搜索完成: 关键词='{keyword}', 找到 {len(formatted_results)} 条结果")
            return formatted_results
            
        except Exception as e:
            self.logger.error(f"❌ 搜索热点失败: {e}")
            return []