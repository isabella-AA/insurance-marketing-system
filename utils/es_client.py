import os
import logging
from typing import List, Dict, Any, Optional, Union
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, NotFoundError, RequestError
from dotenv import load_dotenv

load_dotenv('config/.env')

class ESClient:
    """
    Elasticsearch 客户端封装类
    提供常用的ES操作方法，包含完整的异常处理和日志记录
    """
    
    def __init__(self, es_host: Optional[str] = None):
        """
        初始化ES客户端
        
        Args:
            es_host: ES主机地址，如果不提供则从环境变量读取
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self._init_client(es_host)
        self._verify_connection()
    
    def _init_client(self, es_host: Optional[str] = None):
        """初始化ES连接"""
        try:
            # 获取连接参数
            host = es_host or os.getenv("ES_HOST", "http://101.201.58.151:9200")
            if not host.startswith('http'):
                host = f'http://{host}'
            
            # 构建连接配置
            config = {
                "hosts": [host],
                "request_timeout": int(os.getenv("ES_REQUEST_TIMEOUT", 30)),
                "retry_on_timeout": True,
                "max_retries": int(os.getenv("ES_MAX_RETRIES", 3))
            }
            
            # 如果有认证信息则添加
            username = os.getenv("ES_USER")
            password = os.getenv("ES_PASSWORD")
            if username and password:
                config["basic_auth"] = (username, password)
            
            self.client = Elasticsearch(**config)
            self.logger.info(f"✅ ES客户端初始化成功，连接到: {host}")
            
        except Exception as e:
            self.logger.error(f"❌ ES客户端初始化失败: {e}")
            raise
    
    def _verify_connection(self):
        """验证ES连接是否正常"""
        try:
            info = self.client.info()
            self.logger.info(f"✅ ES连接验证成功，版本: {info['version']['number']}")
        except ConnectionError as e:
            self.logger.error(f"❌ ES连接失败: {e}")
            raise
        except Exception as e:
            self.logger.error(f"❌ ES连接验证异常: {e}")
            raise
    
    def search(self, index: str, query: Dict[str, Any], size: int = 10, 
               sort: Optional[List] = None, source: Optional[List] = None) -> List[Dict[str, Any]]:
        """
        搜索文档
        
        Args:
            index: 索引名称
            query: 查询条件
            size: 返回结果数量
            sort: 排序条件
            source: 指定返回字段
            
        Returns:
            搜索结果列表，每个结果包含_id字段
        """
        try:
            search_body = {"query": query, "size": size}
            if sort:
                search_body["sort"] = sort
            if source:
                search_body["_source"] = source
            
            result = self.client.search(index=index, body=search_body)
            
            documents = []
            for hit in result["hits"]["hits"]:
                doc = hit["_source"]
                doc["_id"] = hit["_id"]  # 统一使用_id字段名
                documents.append(doc)
            
            self.logger.debug(f"🔍 搜索完成: {index}, 返回 {len(documents)} 条结果")
            return documents
            
        except NotFoundError:
            self.logger.warning(f"⚠️ 索引不存在: {index}")
            return []
        except Exception as e:
            self.logger.error(f"❌ 搜索失败: {index}, {e}")
            raise
    
    def get_by_id(self, index: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        根据ID获取文档
        
        Args:
            index: 索引名称
            doc_id: 文档ID
            
        Returns:
            文档内容，如果不存在则返回None
        """
        try:
            result = self.client.get(index=index, id=doc_id)
            doc = result["_source"]
            doc["_id"] = result["_id"]
            return doc
        except NotFoundError:
            self.logger.debug(f"📄 文档不存在: {index}/{doc_id}")
            return None
        except Exception as e:
            self.logger.error(f"❌ 获取文档失败: {index}/{doc_id}, {e}")
            raise
    
    def exists(self, index: str, doc_id: str) -> bool:
        """
        检查文档是否存在
        
        Args:
            index: 索引名称
            doc_id: 文档ID
            
        Returns:
            文档是否存在
        """
        try:
            return self.client.exists(index=index, id=doc_id)
        except Exception as e:
            self.logger.error(f"❌ 检查文档存在性失败: {index}/{doc_id}, {e}")
            return False
    
    def index_document(self, index: str, doc: Dict[str, Any], doc_id: Optional[str] = None) -> str:
        """
        索引文档（插入或更新）
        
        Args:
            index: 索引名称
            doc: 文档内容
            doc_id: 文档ID，如果不提供则自动生成
            
        Returns:
            文档ID
        """
        try:
            if doc_id:
                result = self.client.index(index=index, id=doc_id, document=doc)
            else:
                result = self.client.index(index=index, document=doc)
            
            self.logger.debug(f"📝 文档索引成功: {index}/{result['_id']}")
            return result["_id"]
            
        except Exception as e:
            self.logger.error(f"❌ 文档索引失败: {index}, {e}")
            raise
    
    def update_by_id(self, index: str, doc_id: str, doc: Dict[str, Any], 
                     upsert: bool = False) -> bool:
        """
        根据ID更新文档
        
        Args:
            index: 索引名称
            doc_id: 文档ID
            doc: 更新内容
            upsert: 如果文档不存在是否创建
            
        Returns:
            更新是否成功
        """
        try:
            update_body = {"doc": doc}
            if upsert:
                update_body["doc_as_upsert"] = True
            
            self.client.update(index=index, id=doc_id, body=update_body)
            self.logger.debug(f"✏️ 文档更新成功: {index}/{doc_id}")
            return True
            
        except NotFoundError:
            self.logger.warning(f"⚠️ 更新失败，文档不存在: {index}/{doc_id}")
            return False
        except Exception as e:
            self.logger.error(f"❌ 文档更新失败: {index}/{doc_id}, {e}")
            raise
    
    def delete_by_id(self, index: str, doc_id: str) -> bool:
        """
        根据ID删除文档
        
        Args:
            index: 索引名称
            doc_id: 文档ID
            
        Returns:
            删除是否成功
        """
        try:
            self.client.delete(index=index, id=doc_id)
            self.logger.debug(f"🗑️ 文档删除成功: {index}/{doc_id}")
            return True
            
        except NotFoundError:
            self.logger.warning(f"⚠️ 删除失败，文档不存在: {index}/{doc_id}")
            return False
        except Exception as e:
            self.logger.error(f"❌ 文档删除失败: {index}/{doc_id}, {e}")
            raise
    
    def bulk_index(self, index: str, docs: List[Dict[str, Any]], 
                   doc_ids: Optional[List[str]] = None) -> int:
        """
        批量索引文档
        
        Args:
            index: 索引名称
            docs: 文档列表
            doc_ids: 文档ID列表，长度应与docs相同
            
        Returns:
            成功索引的文档数量
        """
        try:
            actions = []
            for i, doc in enumerate(docs):
                action = {
                    "_index": index,
                    "_source": doc
                }
                if doc_ids and i < len(doc_ids):
                    action["_id"] = doc_ids[i]
                actions.append(action)
            
            success_count, failed_items = helpers.bulk(
                self.client, 
                actions, 
                stats_only=True,
                chunk_size=int(os.getenv("ES_BULK_SIZE", 100))
            )
            
            self.logger.info(f"📦 批量索引完成: {index}, 成功 {success_count} 条")
            return success_count
            
        except Exception as e:
            self.logger.error(f"❌ 批量索引失败: {index}, {e}")
            raise
    
    def count(self, index: str, query: Optional[Dict[str, Any]] = None) -> int:
        """
        统计文档数量
        
        Args:
            index: 索引名称
            query: 查询条件，不提供则统计全部
            
        Returns:
            文档数量
        """
        try:
            body = {"query": query} if query else None
            result = self.client.count(index=index, body=body)
            count = result["count"]
            self.logger.debug(f"📊 文档统计: {index}, 共 {count} 条")
            return count
            
        except Exception as e:
            self.logger.error(f"❌ 文档统计失败: {index}, {e}")
            raise
    
    def create_index(self, index: str, mapping: Dict[str, Any]) -> bool:
        """
        创建索引
        
        Args:
            index: 索引名称
            mapping: 索引映射
            
        Returns:
            创建是否成功
        """
        try:
            if self.client.indices.exists(index=index):
                self.logger.info(f"ℹ️ 索引已存在: {index}")
                return True
            
            self.client.indices.create(index=index, body=mapping)
            self.logger.info(f"✅ 索引创建成功: {index}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 索引创建失败: {index}, {e}")
            raise
    
    def delete_index(self, index: str) -> bool:
        """
        删除索引
        
        Args:
            index: 索引名称
            
        Returns:
            删除是否成功
        """
        try:
            if not self.client.indices.exists(index=index):
                self.logger.warning(f"⚠️ 索引不存在: {index}")
                return False
            
            self.client.indices.delete(index=index)
            self.logger.info(f"🗑️ 索引删除成功: {index}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 索引删除失败: {index}, {e}")
            raise