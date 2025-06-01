import os
import logging
import hashlib
from typing import List, Dict, Any, Tuple, Optional, Union
import numpy as np
import torch
from sentence_transformers import SentenceTransformer, util
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv('config/.env')

class VectorUtils:
    """
    文本向量化和相似度计算工具类
    支持多种相似度计算方法、批量处理和缓存优化
    """
    
    def __init__(self, 
                 model_name_or_path: Optional[str] = None,
                 device: Optional[str] = None,
                 cache_size: int = 1000):
        """
        初始化向量工具类
        
        Args:
            model_name_or_path: 模型名称或本地路径
            device: 计算设备 ('cpu', 'cuda', 'auto')
            cache_size: 向量缓存大小
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 配置参数
        self.model_name = model_name_or_path or os.getenv('VECTOR_MODEL', 'BAAI/bge-m3')
        self.device = self._get_device(device)
        self.cache_size = cache_size
        
        # 初始化模型
        self.model = None
        self.dimension = None
        self._load_model()
        
        # 向量缓存
        self._vector_cache = {}
        
    def _get_device(self, device: Optional[str]) -> str:
        """确定计算设备"""
        if device == 'auto' or device is None:
            if torch.cuda.is_available():
                device = 'cuda'
                self.logger.info("🚀 检测到CUDA，使用GPU加速")
            else:
                device = 'cpu'
                self.logger.info("💻 使用CPU计算")
        return device
    
    def _load_model(self):
        """加载预训练模型"""
        try:
            self.logger.info(f"📥 正在加载模型: {self.model_name}")
            
            # 尝试加载模型
            self.model = SentenceTransformer(
                self.model_name,
                device=self.device,
                trust_remote_code=True
            )
            
            # 获取向量维度
            test_text = "test"
            test_embedding = self.model.encode(test_text)
            self.dimension = len(test_embedding)
            
            self.logger.info(f"✅ 模型加载成功: {self.model_name}")
            self.logger.info(f"📐 向量维度: {self.dimension}, 设备: {self.device}")
            
        except Exception as e:
            self.logger.error(f"❌ 模型加载失败: {e}")
            self.logger.error("💡 请检查网络连接或尝试使用本地模型路径")
            raise
    
    def _get_cache_key(self, text: str) -> str:
        """生成缓存键"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def _manage_cache(self):
        """管理缓存大小"""
        if len(self._vector_cache) > self.cache_size:
            # 清理最旧的缓存项
            keys_to_remove = list(self._vector_cache.keys())[:-self.cache_size//2]
            for key in keys_to_remove:
                del self._vector_cache[key]
            self.logger.debug(f"🧹 缓存清理完成，保留 {len(self._vector_cache)} 项")
    
    def embed(self, text: str, use_cache: bool = True) -> np.ndarray:
        """
        将单个文本转换为向量
        
        Args:
            text: 输入文本
            use_cache: 是否使用缓存
            
        Returns:
            文本向量
        """
        if not self.model:
            raise RuntimeError("模型未加载")
        
        if not text or not text.strip():
            self.logger.warning("⚠️ 输入文本为空，返回零向量")
            return np.zeros(self.dimension)
        
        # 检查缓存
        if use_cache:
            cache_key = self._get_cache_key(text)
            if cache_key in self._vector_cache:
                return self._vector_cache[cache_key]
        
        try:
            # 生成向量
            embedding = self.model.encode(text, convert_to_tensor=False)
            embedding = np.array(embedding, dtype=np.float32)
            
            # 存入缓存
            if use_cache:
                self._vector_cache[cache_key] = embedding
                self._manage_cache()
            
            return embedding
            
        except Exception as e:
            self.logger.error(f"❌ 向量生成失败: {e}")
            return np.zeros(self.dimension)
    
    def embed_batch(self, 
                    texts: List[str], 
                    batch_size: int = 32,
                    show_progress: bool = False) -> List[np.ndarray]:
        """
        批量将文本转换为向量
        
        Args:
            texts: 文本列表
            batch_size: 批处理大小
            show_progress: 是否显示进度
            
        Returns:
            向量列表
        """
        if not self.model:
            raise RuntimeError("模型未加载")
        
        if not texts:
            return []
        
        try:
            # 预处理：过滤空文本
            valid_texts = []
            text_indices = []
            
            for i, text in enumerate(texts):
                if text and text.strip():
                    valid_texts.append(text)
                    text_indices.append(i)
            
            if not valid_texts:
                self.logger.warning("⚠️ 所有输入文本为空")
                return [np.zeros(self.dimension) for _ in texts]
            
            # 批量生成向量
            self.logger.info(f"🔄 批量处理 {len(valid_texts)} 个文本")
            
            embeddings = self.model.encode(
                valid_texts,
                batch_size=batch_size,
                show_progress_bar=show_progress,
                convert_to_tensor=False
            )
            
            # 转换为numpy数组
            embeddings = np.array(embeddings, dtype=np.float32)
            
            # 构建完整结果列表
            results = []
            valid_idx = 0
            
            for i in range(len(texts)):
                if i in text_indices:
                    results.append(embeddings[valid_idx])
                    valid_idx += 1
                else:
                    results.append(np.zeros(self.dimension))
            
            self.logger.info(f"✅ 批量处理完成")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 批量向量生成失败: {e}")
            return [np.zeros(self.dimension) for _ in texts]
    
    def cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """
        计算两个向量的余弦相似度
        
        Args:
            vec1: 向量1
            vec2: 向量2
            
        Returns:
            余弦相似度值 (-1 到 1)
        """
        try:
            # 计算余弦相似度
            dot_product = np.dot(vec1, vec2)
            norm1 = np.linalg.norm(vec1)
            norm2 = np.linalg.norm(vec2)
            
            if norm1 == 0 or norm2 == 0:
                return 0.0
            
            similarity = dot_product / (norm1 * norm2)
            return float(np.clip(similarity, -1.0, 1.0))
            
        except Exception as e:
            self.logger.error(f"❌ 相似度计算失败: {e}")
            return 0.0
    
    def text_similarity(self, text1: str, text2: str) -> float:
        """
        计算两个文本的相似度
        
        Args:
            text1: 文本1
            text2: 文本2
            
        Returns:
            相似度值 (0 到 1)
        """
        vec1 = self.embed(text1)
        vec2 = self.embed(text2)
        similarity = self.cosine_similarity(vec1, vec2)
        
        # 转换到0-1范围
        return (similarity + 1) / 2
    
    def find_most_similar(self, 
                         query: str,
                         candidates: List[str],
                         top_k: int = 5,
                         threshold: float = 0.0) -> List[Dict[str, Any]]:
        """
        找到与查询文本最相似的候选文本
        
        Args:
            query: 查询文本
            candidates: 候选文本列表
            top_k: 返回前k个结果
            threshold: 最小相似度阈值
            
        Returns:
            相似度结果列表，按相似度降序排列
        """
        if not query or not candidates:
            return []
        
        try:
            # 生成查询向量
            query_vec = self.embed(query)
            
            # 批量生成候选向量
            candidate_vecs = self.embed_batch(candidates)
            
            # 计算相似度
            similarities = []
            for i, candidate_vec in enumerate(candidate_vecs):
                similarity = self.cosine_similarity(query_vec, candidate_vec)
                
                if similarity >= threshold:
                    similarities.append({
                        'text': candidates[i],
                        'similarity': similarity,
                        'index': i
                    })
            
            # 排序并返回top_k
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            
            result = similarities[:top_k]
            self.logger.debug(f"🔍 相似度搜索完成: 查询='{query[:50]}...', 找到 {len(result)} 个结果")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 相似度搜索失败: {e}")
            return []
    
    def top_k_indices(self, 
                     query_vec: np.ndarray,
                     candidate_vecs: List[np.ndarray],
                     k: int) -> List[int]:
        """
        返回与查询向量最相似的前k个候选向量的索引
        
        Args:
            query_vec: 查询向量
            candidate_vecs: 候选向量列表
            k: 返回数量
            
        Returns:
            相似度最高的前k个索引列表
        """
        try:
            similarities = []
            for i, candidate_vec in enumerate(candidate_vecs):
                similarity = self.cosine_similarity(query_vec, candidate_vec)
                similarities.append((similarity, i))
            
            # 按相似度排序
            similarities.sort(key=lambda x: x[0], reverse=True)
            
            # 返回前k个索引
            return [idx for _, idx in similarities[:k]]
            
        except Exception as e:
            self.logger.error(f"❌ Top-k索引计算失败: {e}")
            return []
    
    def cluster_texts(self, 
                     texts: List[str], 
                     threshold: float = 0.8,
                     min_cluster_size: int = 2) -> List[List[int]]:
        """
        基于相似度对文本进行聚类
        
        Args:
            texts: 文本列表
            threshold: 聚类相似度阈值
            min_cluster_size: 最小聚类大小
            
        Returns:
            聚类结果，每个聚类包含文本索引列表
        """
        if len(texts) < min_cluster_size:
            return []
        
        try:
            # 生成向量
            vectors = self.embed_batch(texts)
            
            # 计算相似度矩阵
            n = len(vectors)
            similarity_matrix = np.zeros((n, n))
            
            for i in range(n):
                for j in range(i+1, n):
                    sim = self.cosine_similarity(vectors[i], vectors[j])
                    similarity_matrix[i][j] = sim
                    similarity_matrix[j][i] = sim
            
            # 简单聚类算法
            clusters = []
            used = set()
            
            for i in range(n):
                if i in used:
                    continue
                
                cluster = [i]
                used.add(i)
                
                for j in range(i+1, n):
                    if j not in used and similarity_matrix[i][j] >= threshold:
                        cluster.append(j)
                        used.add(j)
                
                if len(cluster) >= min_cluster_size:
                    clusters.append(cluster)
            
            self.logger.info(f"📊 文本聚类完成: {len(texts)} 个文本分为 {len(clusters)} 个聚类")
            return clusters
            
        except Exception as e:
            self.logger.error(f"❌ 文本聚类失败: {e}")
            return []
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        获取模型信息
        
        Returns:
            模型信息字典
        """
        return {
            'model_name': self.model_name,
            'dimension': self.dimension,
            'device': self.device,
            'cache_size': len(self._vector_cache),
            'max_cache_size': self.cache_size
        }
    
    def clear_cache(self):
        """清空向量缓存"""
        self._vector_cache.clear()
        self.logger.info("🧹 向量缓存已清空")