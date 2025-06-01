import os
import time
import requests
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from .base_agent import BaseAgent
from utils.vector_utils import VectorUtils
from utils.weibo_client import WeiboClient
from dotenv import load_dotenv

load_dotenv('config/.env')

class MaterialCollectorAgent(BaseAgent):
    """
    素材收集智能体
    负责为风险事件收集相关的补充素材和图片
    """
    
    def __init__(self):
        super().__init__("MaterialCollector")
        
        # 配置参数
        self.index_name = os.getenv("HOT_EVENT_INDEX", "hoteventdb")
        self.batch_size = int(os.getenv("MATERIAL_COLLECTOR_BATCH_SIZE", 3))
        self.max_search_results = int(os.getenv("MAX_SEARCH_RESULTS", 10))
        self.similarity_threshold = float(os.getenv("SIMILARITY_THRESHOLD", 0.8))
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", 15))
        
        # 初始化向量工具和微博客户端
        self.vector_utils = VectorUtils()
        self.weibo_client = WeiboClient()
        
        # 验证微博Cookie
        cookie_info = self.weibo_client.get_cookie_info()
        if cookie_info['has_cookie'] and cookie_info['is_valid']:
            self.logger.info("✅ 微博Cookie验证成功，可以获取详细内容")
        else:
            self.logger.warning("⚠️ 微博Cookie无效或未配置，只能获取基础搜索结果")
        
        # 请求头配置
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # 搜索引擎配置
        self.search_engines = {
            'weibo': self._search_weibo,
            'baidu': self._search_baidu,
            'sogou': self._search_sogou
        }
        
        self.logger.info(f"✅ 素材收集器初始化完成，相似度阈值: {self.similarity_threshold}")
    
    def run_once(self) -> str:
        """
        执行一次素材收集任务
        
        Returns:
            处理结果描述
        """
        # 获取待收集素材的事件
        events = self._fetch_pending_events()
        
        if not events:
            self.logger.info("⚠️ 暂无待收集素材的事件")
            return "无待处理事件"
        
        # 处理事件
        success_count = 0
        total_count = len(events)
        
        for event in events:
            try:
                if self._collect_materials_for_event(event):
                    success_count += 1
                    
            except Exception as e:
                self.logger.error(f"❌ 收集素材失败: {event.get('title', 'Unknown')}, {e}")
        
        result = f"素材收集完成: {success_count}/{total_count} 成功"
        self.logger.info(f"📊 {result}")
        return result
    
    def _fetch_pending_events(self) -> List[Dict[str, Any]]:
        """
        获取待收集素材的事件
        
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
                        {"term": {"material_collected": True}}
                    ]
                }
            }
            
            events = self.es.search(
                index=self.index_name,
                query=query,
                size=self.batch_size
            )
            
            self.logger.debug(f"🔍 获取到 {len(events)} 个待收集素材的事件")
            return events
            
        except Exception as e:
            self.logger.error(f"❌ 获取待收集事件失败: {e}")
    def _search_baidu(self, keyword: str) -> List[Dict[str, str]]:
           pass
    def _collect_materials_for_event(self, event: Dict[str, Any]) -> bool:
        """
        为单个事件收集素材
        
        Args:
            event: 事件数据
            
        Returns:
            是否收集成功
        """
        title = event.get("title", "")
        content = event.get("content", "")
        event_id = event.get("_id")
        
        if not title:
            self.logger.warning(f"⚠️ 事件标题为空: {event_id}")
            return False
        
        self.logger.info(f"🔍 正在收集素材: {title[:50]}...")
        
        # 先标记为正在处理，避免重复处理
        self._mark_processing(event_id)
        
        try:
            # 收集网页素材
            web_materials = self._collect_web_materials(title, content)
            
            # 收集图片素材
            image_materials = self._collect_image_materials(title)
            
            # 整合素材
            all_materials = {
                "texts": web_materials,
                "image_urls": image_materials,
                "collected_at": time.time()
            }
            
            # 更新事件记录
            return self._update_event_materials(event_id, all_materials)
            
        except Exception as e:
            self.logger.error(f"❌ 收集素材异常: {title}, {e}")
            # 标记为失败，但不阻止后续处理
            self._mark_failed(event_id)
            return False
    
    def _collect_web_materials(self, title: str, content: str) -> List[str]:
        """
        收集网页文本素材
        
        Args:
            title: 事件标题
            content: 事件内容
            
        Returns:
            相关文本列表
        """
        all_texts = []
        
        # 使用多个搜索引擎
        for engine_name, search_func in self.search_engines.items():
            try:
                self.logger.debug(f"🔍 使用 {engine_name} 搜索相关内容")
                search_results = search_func(title)
                
                for result in search_results[:3]:  # 每个引擎最多取3个结果
                    text_content = self._extract_text_from_url(result.get('url', ''))
                    if text_content:
                        # 检查相关性
                        if self._is_relevant_content(title, text_content):
                            all_texts.append({
                                'content': text_content,
                                'source': result.get('url', ''),
                                'engine': engine_name
                            })
                
            except Exception as e:
                self.logger.warning(f"⚠️ {engine_name} 搜索失败: {e}")
                continue
        
        # 去重和筛选
        unique_texts = self._deduplicate_texts(all_texts)
        
        self.logger.info(f"📝 收集到 {len(unique_texts)} 条文本素材")
        return unique_texts[:5]  # 最多保留5条
    
    def _collect_image_materials(self, title: str) -> List[str]:
        """
        收集图片素材
        
        Args:
            title: 事件标题
            
        Returns:
            图片URL列表
        """
        image_urls = []
        
        try:
            # 使用微博搜索图片
            weibo_images = self._search_weibo_images(title)
            image_urls.extend(weibo_images)
            
            # 可以添加其他图片源
            # baidu_images = self._search_baidu_images(title)
            # image_urls.extend(baidu_images)
            
        except Exception as e:
            self.logger.warning(f"⚠️ 图片收集失败: {e}")
        
        # 去重和验证
        valid_images = self._validate_image_urls(image_urls)
        
        self.logger.info(f"🖼️ 收集到 {len(valid_images)} 张图片")
        return valid_images[:5]  # 最多保留5张
    
    def _search_weibo(self, keyword: str) -> List[Dict[str, str]]:
        """
        搜索微博内容 (使用新的微博客户端)
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            搜索结果列表
        """
        try:
            # 使用微博客户端搜索
            search_results = self.weibo_client.search_posts(
                keyword=keyword,
                max_results=self.max_search_results
            )
            
            # 转换为标准格式
            results = []
            for result in search_results:
                if result.get('text') and result.get('url'):
                    results.append({
                        'title': result['text'][:100] + "..." if len(result['text']) > 100 else result['text'],
                        'url': result['url'],
                        'source': 'weibo',
                        'full_text': result['text'],
                        'images': result.get('images', []),
                        'user_name': result.get('user_name', ''),
                        'publish_time': result.get('publish_time', ''),
                        'interaction_data': {
                            'attitude_count': result.get('attitude_count', ''),
                            'comment_count': result.get('comment_count', ''),
                            'forward_count': result.get('forward_count', '')
                        }
                    })
            
            self.logger.info(f"🔍 微博搜索完成: {keyword}, 找到 {len(results)} 条结果")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 微博搜索失败: {e}")
            # 降级到原有搜索方法
            return self._search_weibo_fallback(keyword)
    
    def _search_weibo_fallback(self, keyword: str) -> List[Dict[str, str]]:
        """
        微博搜索降级方法（无Cookie时使用）
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            搜索结果列表
        """
        try:
            search_url = f"https://s.weibo.com/weibo"
            params = {
                'q': keyword,
                'sort': 'hot',
                'page': 1
            }
            
            response = requests.get(
                search_url, 
                params=params,
                headers=self.headers, 
                timeout=self.request_timeout
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            results = []
            
            # 解析搜索结果
            cards = soup.select('.card-wrap')
            for card in cards[:self.max_search_results]:
                title_elem = card.select_one('.txt')
                link_elem = card.select_one('a[href*="/status/"]')
                
                if title_elem and link_elem:
                    title = title_elem.get_text(strip=True)
                    url = urljoin('https://weibo.com', link_elem['href'])
                    
                    results.append({
                        'title': title,
                        'url': url,
                        'source': 'weibo'
                    })
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 微博降级搜索失败: {e}")
            return []
        """
        搜索百度内容
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            搜索结果列表
        """
        try:
            search_url = "https://www.baidu.com/s"
            params = {
                'wd': keyword,
                'rn': 10,
                'tn': 'baiduhome_pg'
            }
            
            response = requests.get(
                search_url,
                params=params,
                headers=self.headers,
                timeout=self.request_timeout
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            results = []
            
            # 解析百度搜索结果
            for item in soup.select('.result.c-container')[:self.max_search_results]:
                title_elem = item.select_one('h3 a')
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    url = title_elem.get('href', '')
                    
                    if url:
                        results.append({
                            'title': title,
                            'url': url,
                            'source': 'baidu'
                        })
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 百度搜索失败: {e}")
            return []
    
    def _search_sogou(self, keyword: str) -> List[Dict[str, str]]:
        """
        搜索搜狗内容
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            搜索结果列表
        """
        try:
            search_url = "https://www.sogou.com/web"
            params = {
                'query': keyword,
                'num': 10
            }
            
            response = requests.get(
                search_url,
                params=params,
                headers=self.headers,
                timeout=self.request_timeout
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            results = []
            
            # 解析搜狗搜索结果
            for item in soup.select('.result')[:self.max_search_results]:
                title_elem = item.select_one('h3 a')
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    url = title_elem.get('href', '')
                    
                    if url:
                        results.append({
                            'title': title,
                            'url': url,
                            'source': 'sogou'
                        })
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 搜狗搜索失败: {e}")
            return []
    
    def _search_weibo_images(self, keyword: str) -> List[str]:
        """
        搜索微博图片 (使用新的微博客户端)
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            图片URL列表
        """
        try:
            # 使用微博客户端搜索图片
            image_urls = self.weibo_client.search_images(
                keyword=keyword,
                max_results=10
            )
            
            self.logger.info(f"🖼️ 微博图片搜索完成: {keyword}, 找到 {len(image_urls)} 张图片")
            return image_urls
            
        except Exception as e:
            self.logger.error(f"❌ 微博图片搜索失败: {e}")
            return []
        
    
    def _extract_text_from_url(self, url: str) -> Optional[str]:
        """
        从URL提取文本内容
        
        Args:
            url: 网页URL
            
        Returns:
            提取的文本内容
        """
        try:
            if not url or not self._is_valid_url(url):
                return None
            
            response = requests.get(
                url,
                headers=self.headers,
                timeout=self.request_timeout,
                allow_redirects=True
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 移除脚本和样式
            for script in soup(["script", "style"]):
                script.decompose()
            
            # 提取主要内容
            content_selectors = [
                '.content', '.article', '.post', '.text',
                'article', 'main', '.main-content'
            ]
            
            content = ""
            for selector in content_selectors:
                elements = soup.select(selector)
                if elements:
                    content = elements[0].get_text(separator='\n', strip=True)
                    break
            
            if not content:
                # 备用方案：提取body文本
                body = soup.find('body')
                if body:
                    content = body.get_text(separator='\n', strip=True)
            
            # 清理和截取
            lines = [line.strip() for line in content.split('\n') if line.strip()]
            clean_content = '\n'.join(lines)
            
            # 限制长度
            if len(clean_content) > 2000:
                clean_content = clean_content[:2000] + "..."
            
            return clean_content if len(clean_content) > 50 else None
            
        except Exception as e:
            self.logger.debug(f"❌ 提取网页内容失败: {url}, {e}")
            return None
    
    def _is_relevant_content(self, title: str, content: str) -> bool:
        """
        检查内容是否与标题相关
        
        Args:
            title: 事件标题
            content: 网页内容
            
        Returns:
            是否相关
        """
        try:
            # 使用向量相似度判断
            similarity = self.vector_utils.text_similarity(title, content[:500])  # 只比较前500字符
            
            is_relevant = similarity >= self.similarity_threshold
            self.logger.debug(f"📊 内容相关性: {similarity:.3f}, 阈值: {self.similarity_threshold}, 相关: {is_relevant}")
            
            return is_relevant
            
        except Exception as e:
            self.logger.warning(f"⚠️ 相关性检查失败: {e}")
            # 备用方案：关键词匹配
            return self._keyword_relevance_check(title, content)
    
    def _keyword_relevance_check(self, title: str, content: str) -> bool:
        """
        关键词相关性检查（备用方案）
        
        Args:
            title: 事件标题
            content: 网页内容
            
        Returns:
            是否相关
        """
        # 提取标题中的关键词
        title_words = set(title.replace(' ', ''))
        content_words = set(content.replace(' ', ''))
        
        # 计算交集比例
        intersection = title_words.intersection(content_words)
        if len(title_words) > 0:
            relevance_ratio = len(intersection) / len(title_words)
            return relevance_ratio >= 0.3  # 至少30%的关键词匹配
        
        return False
    
    def _deduplicate_texts(self, texts: List[Dict[str, Any]]) -> List[str]:
        """
        去重文本素材
        
        Args:
            texts: 文本素材列表
            
        Returns:
            去重后的文本列表
        """
        if not texts:
            return []
        
        unique_texts = []
        seen_contents = set()
        
        for text_item in texts:
            content = text_item.get('content', '')
            if not content:
                continue
            
            # 简单的重复检查
            content_hash = hash(content[:200])  # 使用前200字符的哈希
            
            if content_hash not in seen_contents:
                seen_contents.add(content_hash)
                unique_texts.append(content)
        
        return unique_texts
    
    def _validate_image_urls(self, image_urls: List[str]) -> List[str]:
        """
        验证图片URL的有效性
        
        Args:
            image_urls: 图片URL列表
            
        Returns:
            有效的图片URL列表
        """
        valid_urls = []
        
        for url in image_urls:
            try:
                if self._is_valid_image_url(url):
                    valid_urls.append(url)
                    
            except Exception as e:
                self.logger.debug(f"❌ 图片URL验证失败: {url}, {e}")
                continue
        
        return valid_urls
    
    def _is_valid_url(self, url: str) -> bool:
        """检查URL是否有效"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    def _is_valid_image_url(self, url: str) -> bool:
        """检查图片URL是否有效"""
        if not self._is_valid_url(url):
            return False
        
        # 检查文件扩展名
        valid_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.webp')
        url_lower = url.lower()
        
        return any(ext in url_lower for ext in valid_extensions) or 'sinaimg' in url_lower
    
    def _mark_processing(self, event_id: str):
        """标记事件为处理中"""
        try:
            self.es.update_by_id(
                index=self.index_name,
                doc_id=event_id,
                doc={"material_collected": False}
            )
        except Exception as e:
            self.logger.warning(f"⚠️ 标记处理状态失败: {event_id}, {e}")
    
    def _mark_failed(self, event_id: str):
        """标记事件处理失败"""
        try:
            self.es.update_by_id(
                index=self.index_name,
                doc_id=event_id,
                doc={
                    "material_collected": True,
                    "material_collection_failed": True
                }
            )
        except Exception as e:
            self.logger.warning(f"⚠️ 标记失败状态失败: {event_id}, {e}")
    
    def _update_event_materials(self, event_id: str, materials: Dict[str, Any]) -> bool:
        """
        更新事件的素材信息
        
        Args:
            event_id: 事件ID
            materials: 素材数据
            
        Returns:
            更新是否成功
        """
        try:
            update_data = {
                "material": materials,
                "material_collected": True
            }
            
            success = self.es.update_by_id(
                index=self.index_name,
                doc_id=event_id,
                doc=update_data
            )
            
            if success:
                text_count = len(materials.get('texts', []))
                image_count = len(materials.get('image_urls', []))
                self.logger.info(f"✅ 素材更新成功: {event_id}, 文本:{text_count}, 图片:{image_count}")
                return True
            else:
                self.logger.error(f"❌ 素材更新失败: {event_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 更新素材异常: {event_id}, {e}")
            return False