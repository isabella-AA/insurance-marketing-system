import os
import time
import logging
import traceback
from abc import ABC, abstractmethod
from typing import Optional
from utils.es_client import ESClient
from utils.llm_client import GLMClient
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('config/.env')

class BaseAgent(ABC):
    """
    所有智能体的抽象基类
    提供通用的ES连接、LLM调用、日志记录和运行控制功能
    """
    
    def __init__(self, agent_name: Optional[str] = None):
        """
        初始化基础智能体
        
        Args:
            agent_name: 智能体名称，用于日志标识
        """
        self.agent_name = agent_name or self.__class__.__name__
        self._setup_logger()
        self._validate_config()
        self._init_clients()
        
        # 运行控制参数
        self.interval = int(os.getenv("AGENT_INTERVAL", 300))  # 默认每5分钟运行一次
        self.max_batch_size = int(os.getenv("MAX_BATCH_SIZE", 10))  # 每次处理的最大记录数
        
    def _setup_logger(self):
        """设置日志系统"""
        self.logger = logging.getLogger(self.agent_name)
        if not self.logger.handlers:  # 避免重复添加handler
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f'%(asctime)s - {self.agent_name} - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _validate_config(self):
        """验证必要的环境变量配置"""
        required_vars = {
            'ES_HOST': '101.201.58.151:9200',  # 默认值
            'GLM_API_KEY': None
        }
        
        for var_name, default_value in required_vars.items():
            value = os.getenv(var_name, default_value)
            if value is None:
                raise ValueError(f"环境变量 {var_name} 未设置")
            setattr(self, var_name.lower(), value)
    
    def _init_clients(self):
        """初始化ES和LLM客户端"""
        try:
            # 初始化ES客户端
            es_host = os.getenv('ES_HOST', 'http://101.201.58.151:9200')
            if not es_host.startswith('http'):
                es_host = f'http://{es_host}'
            self.es = ESClient(es_host)
            
            # 初始化LLM客户端
            glm_api_key = os.getenv('GLM_API_KEY')
            if not glm_api_key:
                raise ValueError("GLM_API_KEY 环境变量未设置")
            self.llm = GLMClient(glm_api_key)
            
            self.logger.info("✅ 客户端初始化成功")
            
        except Exception as e:
            self.logger.error(f"❌ 客户端初始化失败: {e}")
            raise
    
    @abstractmethod
    def run_once(self):
        """
        执行一次处理逻辑（子类必须实现）
        
        Returns:
            处理结果的简要描述或统计信息
        """
        raise NotImplementedError("子类必须实现 run_once 方法")
    
    def run_forever(self):
        """
        持续运行智能体
        包含异常处理和自动重试机制
        """
        self.logger.info(f"🚀 {self.agent_name} 开始运行，间隔 {self.interval} 秒")
        
        while True:
            try:
                start_time = time.time()
                result = self.run_once()
                
                execution_time = time.time() - start_time
                self.logger.info(f"✅ 执行完成，耗时 {execution_time:.2f}s")
                
                if result:
                    self.logger.info(f"📊 处理结果: {result}")
                    
            except KeyboardInterrupt:
                self.logger.info("🛑 收到停止信号，正在退出...")
                break
                
            except Exception as e:
                self.logger.error(f"❌ 运行异常: {e}")
                self.logger.error(f"📋 异常详情:\n{traceback.format_exc()}")
                
                # 可以在这里添加告警机制
                self._handle_error(e)
            
            finally:
                self.logger.debug(f"😴 等待 {self.interval} 秒后继续...")
                time.sleep(self.interval)
    
    def _handle_error(self, error: Exception):
        """
        错误处理钩子方法，子类可以重写以实现自定义错误处理
        
        Args:
            error: 捕获到的异常
        """
        # 默认实现：记录错误日志
        # 子类可以重写此方法来实现邮件告警、钉钉通知等
        pass
    
    def log_info(self, message: str):
        """便捷的信息日志方法"""
        self.logger.info(message)
    
    def log_error(self, message: str):
        """便捷的错误日志方法"""
        self.logger.error(message)
    
    def log_warning(self, message: str):
        """便捷的警告日志方法"""
        self.logger.warning(message)