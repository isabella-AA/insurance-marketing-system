import os
import time
import logging
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, quote
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv('config/.env')

class WeiboClient:
    """
    微博客户端工具类
    处理微博Cookie认证和内容抓取
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 从环境变量获取配置
        self.cookie = os.getenv('WEIBO_COOKIE', '')
        self.user_agent = os.getenv('WEIBO_USER_AGENT', 
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        self.api_delay = float(os.getenv('WEIBO_API_DELAY', 2))
        self.max_retries = int(os.getenv('WEIBO_MAX_RETRIES', 3))
        self.request_timeout = int(os.getenv('REQUEST_TIMEOUT', 10))
        
        # 请求会话
        self.session = requests.Session()
        self._setup_session()
        
        # 验证Cookie
        if not self.cookie:
            self.logger.warning("⚠️ 微博Cookie未配置，部分功能可能无法使用")
        else:
            self.logger.info("✅ 微博客户端初始化完成")
    
    def _setup_session(self):
        """设置请求会话"""
        # 设置请求头
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        # 设置Cookie
        if self.cookie:
            self.session.headers['Cookie'] = self.cookie
    
    def verify_cookie(self) -> bool:
        """
        验证Cookie是否有效
        
        Returns:
            Cookie是否有效
        """
        try:
            # 尝试访问微博主页
            response = self.session.get(
                'https://weibo.com',
                timeout=self.request_timeout
            )
            
            # 检查是否需要登录
            if '登录' in response.text or 'login' in response.url.lower():
                self.logger.error("❌ 微博Cookie已失效，需要重新获取")
                return False
            
            self.logger.info("✅ 微博Cookie验证成功")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Cookie验证失败: {e}")
            return False
    
    def search_posts(self, keyword: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        搜索微博帖子
        
        Args:
            keyword: 搜索关键词
            max_results: 最大结果数量
            
        Returns:
            搜索结果列表
        """
        try:
            search_url = "https://s.weibo.com/weibo"
            params = {
                'q': keyword,
                'sort': 'hot',
                'page': 1
            }
            
            self.logger.info(f"🔍 搜索微博: {keyword}")
            
            response = self.session.get(
                search_url,
                params=params,
                timeout=self.request_timeout
            )
            response.raise_for_status()
            
            # 解析搜索结果
            soup = BeautifulSoup(response.text, 'html.parser')
            results = []
            
            # 查找微博卡片
            cards = soup.select('.card-wrap')
            
            for card in cards[:max_results]:
                try:
                    result = self._parse_search_card(card)
                    if result:
                        results.append(result)
                except Exception as e:
                    self.logger.debug(f"解析卡片失败: {e}")
                    continue
            
            self.logger.info(f"✅ 搜索完成，找到 {len(results)} 条结果")
            
            # API调用延迟
            time.sleep(self.api_delay)
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 微博搜索失败: {e}")
            return []
    
    def _parse_search_card(self, card) -> Optional[Dict[str, Any]]:
        """
        解析搜索结果卡片
        
        Args:
            card: BeautifulSoup卡片元素
            
        Returns:
            解析后的结果字典
        """
        try:
            result = {}
            
            # 提取文本内容
            text_elem = card.select_one('.txt')
            if text_elem:
                # 移除多余的HTML标签
                for tag in text_elem(['a', 'span']):
                    tag.decompose()
                result['text'] = text_elem.get_text(strip=True)
            
            # 提取链接
            link_elem = card.select_one('a[href*="/status/"]')
            if link_elem:
                href = link_elem.get('href', '')
                if href.startswith('/'):
                    href = 'https://weibo.com' + href
                result['url'] = href
                
                # 从URL提取微博ID
                if '/status/' in href:
                    status_id = href.split('/status/')[-1].split('?')[0]
                    result['status_id'] = status_id
            
            # 提取用户信息
            user_elem = card.select_one('.name')
            if user_elem:
                result['user_name'] = user_elem.get_text(strip=True)
            
            # 提取时间信息
            time_elem = card.select_one('.from')
            if time_elem:
                result['publish_time'] = time_elem.get_text(strip=True)
            
            # 提取互动数据
            attitude_elem = card.select_one('.card-act .attitude')
            if attitude_elem:
                result['attitude_count'] = attitude_elem.get_text(strip=True)
            
            comment_elem = card.select_one('.card-act .comment')
            if comment_elem:
                result['comment_count'] = comment_elem.get_text(strip=True)
            
            forward_elem = card.select_one('.card-act .forward')
            if forward_elem:
                result['forward_count'] = forward_elem.get_text(strip=True)
            
            # 提取图片
            images = []
            img_elems = card.select('img[src*="sinaimg"]')
            for img in img_elems:
                src = img.get('src', '')
                if src and 'sinaimg' in src:
                    # 转换为高清图片URL
                    if 'thumbnail' in src:
                        src = src.replace('thumbnail', 'large')
                    images.append(src)
            
            result['images'] = images
            
            # 基本验证
            if result.get('text') and result.get('url'):
                return result
            else:
                return None
                
        except Exception as e:
            self.logger.debug(f"解析卡片异常: {e}")
            return None
    
    def get_post_detail(self, status_id: str) -> Optional[Dict[str, Any]]:
        """
        获取微博详情
        
        Args:
            status_id: 微博状态ID
            
        Returns:
            微博详情数据
        """
        try:
            detail_url = f"https://weibo.com/status/{status_id}"
            
            response = self.session.get(
                detail_url,
                timeout=self.request_timeout
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 这里可以添加更详细的解析逻辑
            # 由于微博页面结构复杂，这里提供基础框架
            
            detail = {
                'status_id': status_id,
                'url': detail_url,
                'raw_html': response.text[:1000]  # 保留部分原始HTML用于调试
            }
            
            time.sleep(self.api_delay)
            return detail
            
        except Exception as e:
            self.logger.error(f"❌ 获取微博详情失败: {status_id}, {e}")
            return None
    
    def search_images(self, keyword: str, max_results: int = 10) -> List[str]:
        """
        搜索微博图片
        
        Args:
            keyword: 搜索关键词
            max_results: 最大结果数量
            
        Returns:
            图片URL列表
        """
        try:
            search_url = "https://s.weibo.com/pic"
            params = {'q': keyword}
            
            response = self.session.get(
                search_url,
                params=params,
                timeout=self.request_timeout
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            image_urls = []
            
            # 提取图片URL
            img_elems = soup.select('img[src*="sinaimg"]')
            for img in img_elems[:max_results]:
                src = img.get('src', '')
                if src and 'sinaimg' in src:
                    # 转换为高清图片URL
                    if 'thumbnail' in src:
                        src = src.replace('thumbnail', 'large')
                    image_urls.append(src)
            
            # 去重
            unique_images = list(set(image_urls))
            
            self.logger.info(f"🖼️ 搜索到 {len(unique_images)} 张图片")
            
            time.sleep(self.api_delay)
            return unique_images
            
        except Exception as e:
            self.logger.error(f"❌ 微博图片搜索失败: {e}")
            return []
    
    def get_cookie_info(self) -> Dict[str, Any]:
        """
        获取Cookie信息和状态
        
        Returns:
            Cookie状态信息
        """
        cookie_info = {
            'has_cookie': bool(self.cookie),
            'cookie_length': len(self.cookie) if self.cookie else 0,
            'is_valid': False,
            'user_agent': self.user_agent,
            'api_delay': self.api_delay
        }
        
        if self.cookie:
            cookie_info['is_valid'] = self.verify_cookie()
            
            # 解析Cookie中的用户信息
            if 'SUB=' in self.cookie:
                cookie_info['has_login_token'] = True
            
        return cookie_info
    
    def update_cookie(self, new_cookie: str) -> bool:
        """
        更新Cookie
        
        Args:
            new_cookie: 新的Cookie字符串
            
        Returns:
            更新是否成功
        """
        try:
            self.cookie = new_cookie
            self.session.headers['Cookie'] = new_cookie
            
            # 验证新Cookie
            if self.verify_cookie():
                self.logger.info("✅ Cookie更新成功")
                return True
            else:
                self.logger.error("❌ 新Cookie验证失败")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Cookie更新异常: {e}")
            return False