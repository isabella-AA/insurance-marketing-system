import os
import json
import time
import logging
import requests
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv('config/.env')

@dataclass
class LLMResponse:
    """LLM响应结果封装"""
    success: bool
    content: str
    error: Optional[str] = None
    usage: Optional[Dict[str, int]] = None
    model: Optional[str] = None

class LLMError(Exception):
    """LLM调用异常"""
    def __init__(self, message: str, error_type: str = "unknown"):
        super().__init__(message)
        self.error_type = error_type

class GLMClient:
    """
    智谱GLM大语言模型客户端封装
    支持多种调用方式、重试机制和完整的异常处理
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        初始化GLM客户端
        
        Args:
            api_key: API密钥，如果不提供则从环境变量读取
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = api_key or os.getenv('GLM_API_KEY')
        if not self.api_key:
            raise ValueError("GLM_API_KEY 未设置")
        
        # 配置参数
        self.base_url = os.getenv('GLM_BASE_URL', 'https://open.bigmodel.cn/api/paas/v4/chat/completions')
        self.default_model = os.getenv('GLM_MODEL', 'glm-4-air')
        self.timeout = int(os.getenv('GLM_TIMEOUT', 60))
        self.max_retries = int(os.getenv('GLM_MAX_RETRIES', 3))
        self.retry_delay = float(os.getenv('GLM_RETRY_DELAY', 1.0))
        
        # 请求头
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        self.logger.info(f"✅ GLM客户端初始化成功，模型: {self.default_model}")
    
    def chat(self, 
             user_input: str,
             system_prompt: Optional[str] = None,
             model: Optional[str] = None,
             temperature: float = 0.7,
             max_tokens: Optional[int] = None,
             messages: Optional[List[Dict[str, str]]] = None) -> LLMResponse:
        """
        聊天对话接口
        
        Args:
            user_input: 用户输入
            system_prompt: 系统提示词
            model: 使用的模型，不指定则使用默认模型
            temperature: 温度参数，控制生成的随机性
            max_tokens: 最大生成token数
            messages: 完整的消息列表，如果提供则忽略user_input和system_prompt
            
        Returns:
            LLMResponse对象
        """
        try:
            # 构建消息列表
            if messages is None:
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": user_input})
            
            # 构建请求体
            payload = {
                "model": model or self.default_model,
                "messages": messages,
                "temperature": temperature
            }
            
            if max_tokens:
                payload["max_tokens"] = max_tokens
            
            # 发送请求
            response = self._make_request(payload)
            return self._parse_response(response)
            
        except Exception as e:
            self.logger.error(f"❌ 聊天请求失败: {e}")
            return LLMResponse(
                success=False,
                content="",
                error=str(e)
            )
    
    def simple_chat(self, user_input: str, system_prompt: Optional[str] = None) -> str:
        """
        简化的聊天接口，直接返回文本内容
        
        Args:
            user_input: 用户输入
            system_prompt: 系统提示词
            
        Returns:
            生成的文本内容，失败时返回空字符串
        """
        response = self.chat(user_input=user_input, system_prompt=system_prompt)
        return response.content if response.success else ""
    
    def extract_json(self, 
                     user_input: str, 
                     system_prompt: Optional[str] = None,
                     expected_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        提取JSON格式的响应
        
        Args:
            user_input: 用户输入
            system_prompt: 系统提示词
            expected_keys: 期望的JSON键名列表，用于验证
            
        Returns:
            解析后的JSON对象，失败时返回空字典
        """
        # 如果没有指定系统提示词，添加JSON格式要求
        if not system_prompt:
            system_prompt = "请以JSON格式回复，不要包含其他文本。"
        elif "json" not in system_prompt.lower():
            system_prompt += "\n请以JSON格式回复。"
        
        response = self.chat(user_input=user_input, system_prompt=system_prompt)
        
        if not response.success:
            self.logger.error(f"❌ JSON提取失败: {response.error}")
            return {}
        
        try:
            # 尝试解析JSON
            result = json.loads(response.content)
            
            # 验证期望的键名
            if expected_keys:
                missing_keys = [key for key in expected_keys if key not in result]
                if missing_keys:
                    self.logger.warning(f"⚠️ JSON缺少期望的键: {missing_keys}")
            
            self.logger.debug(f"✅ JSON解析成功: {result}")
            return result
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ JSON解析失败: {e}")
            self.logger.error(f"📋 原始回复: {response.content}")
            return {}
    
    def batch_chat(self, 
                   inputs: List[Dict[str, Any]], 
                   delay: float = 0.5) -> List[LLMResponse]:
        """
        批量聊天请求
        
        Args:
            inputs: 输入列表，每个元素包含chat方法的参数
            delay: 请求间隔时间，避免频率限制
            
        Returns:
            响应结果列表
        """
        results = []
        
        for i, input_params in enumerate(inputs):
            self.logger.info(f"📤 处理批量请求 {i+1}/{len(inputs)}")
            
            response = self.chat(**input_params)
            results.append(response)
            
            # 添加延迟避免频率限制
            if delay > 0 and i < len(inputs) - 1:
                time.sleep(delay)
        
        success_count = sum(1 for r in results if r.success)
        self.logger.info(f"📊 批量处理完成: {success_count}/{len(inputs)} 成功")
        
        return results
    
    def _make_request(self, payload: Dict[str, Any]) -> requests.Response:
        """
        发送HTTP请求，包含重试机制
        
        Args:
            payload: 请求体
            
        Returns:
            HTTP响应对象
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"📤 发送GLM请求 (尝试 {attempt + 1}/{self.max_retries})")
                self.logger.debug(f"📋 请求内容: {json.dumps(payload, ensure_ascii=False, indent=2)}")
                
                response = requests.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload,
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.Timeout as e:
                last_error = LLMError(f"请求超时: {e}", "timeout")
                self.logger.warning(f"⏰ 请求超时，尝试 {attempt + 1}/{self.max_retries}")
                
            except requests.exceptions.ConnectionError as e:
                last_error = LLMError(f"连接错误: {e}", "connection")
                self.logger.warning(f"🔌 连接错误，尝试 {attempt + 1}/{self.max_retries}")
                
            except requests.exceptions.HTTPError as e:
                response_text = getattr(e.response, 'text', 'Unknown error')
                last_error = LLMError(f"HTTP错误: {e}, 响应: {response_text}", "http")
                self.logger.error(f"❌ HTTP错误: {e}")
                # HTTP错误通常不需要重试
                break
                
            except Exception as e:
                last_error = LLMError(f"未知错误: {e}", "unknown")
                self.logger.error(f"❌ 未知错误: {e}")
            
            # 重试延迟
            if attempt < self.max_retries - 1:
                delay = self.retry_delay * (2 ** attempt)  # 指数退避
                self.logger.debug(f"😴 等待 {delay:.1f}s 后重试...")
                time.sleep(delay)
        
        raise last_error
    
    def _parse_response(self, response: requests.Response) -> LLMResponse:
        """
        解析API响应
        
        Args:
            response: HTTP响应对象
            
        Returns:
            LLMResponse对象
        """
        try:
            data = response.json()
            
            if "choices" not in data or not data["choices"]:
                raise LLMError("响应格式错误：缺少choices字段", "format")
            
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            model = data.get("model")
            
            self.logger.debug(f"✅ GLM响应解析成功，内容长度: {len(content)}")
            self.logger.debug(f"📊 Token使用情况: {usage}")
            
            return LLMResponse(
                success=True,
                content=content,
                usage=usage,
                model=model
            )
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ 响应JSON解析失败: {e}")
            return LLMResponse(
                success=False,
                content="",
                error=f"JSON解析失败: {e}"
            )
        except Exception as e:
            self.logger.error(f"❌ 响应解析异常: {e}")
            return LLMResponse(
                success=False,
                content="",
                error=f"响应解析异常: {e}"
            )
    
    def health_check(self) -> bool:
        """
        健康检查，验证API连接是否正常
        
        Returns:
            连接是否正常
        """
        try:
            response = self.simple_chat("Hello", "请简单回复一个字。")
            return bool(response.strip())
        except Exception as e:
            self.logger.error(f"❌ 健康检查失败: {e}")
            return False