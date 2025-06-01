import os
import sys
import time
import signal
import threading
import logging
from typing import Dict, List, Tuple, Optional
from importlib import import_module
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('config/.env')

class AgentRunner:
    """
    智能体运行管理器
    负责协调和管理所有智能体的运行
    """
    
    def __init__(self):
        # 配置日志
        self._setup_logging()
        
        # 智能体配置 - 按照流程顺序排列
        self.agent_configs = [
            {
                "module": "agents.hotspot_agent",
                "class": "HotspotAgent", 
                "name": "热点抓取",
                "priority": 1,
                "dependencies": [],
                "enabled": True
            },
            {
                "module": "agents.risk_analyzer_agent",
                "class": "RiskAnalyzerAgent",
                "name": "风险分析", 
                "priority": 2,
                "dependencies": ["hotspot_agent"],
                "enabled": True
            },
            {
                "module": "agents.material_collector_agent", 
                "class": "MaterialCollectorAgent",
                "name": "素材收集",
                "priority": 3,
                "dependencies": ["risk_analyzer_agent"],
                "enabled": True
            },
            {
                "module": "agents.product_matcher_agent",
                "class": "ProductMatcherAgent", 
                "name": "产品匹配",
                "priority": 4,
                "dependencies": ["risk_analyzer_agent"],
                "enabled": True
            },
            {
                "module": "agents.content_creator_agent",
                "class": "ContentCreatorAgent",
                "name": "内容创作",
                "priority": 5, 
                "dependencies": ["material_collector_agent", "product_matcher_agent"],
                "enabled": True
            },
            {
                "module": "agents.editor_agent",
                "class": "EditorAgent",
                "name": "内容编辑",
                "priority": 6,
                "dependencies": ["content_creator_agent"], 
                "enabled": True
            }
        ]
        
        # 运行配置
        self.mode = os.getenv("RUN_MODE", "run_forever")  # run_once, run_forever, pipeline
        self.global_interval = int(os.getenv("GLOBAL_INTERVAL", 300))  # 全局间隔时间
        self.max_workers = int(os.getenv("MAX_WORKERS", 6))  # 最大并发数
        self.pipeline_delay = int(os.getenv("PIPELINE_DELAY", 60))  # 流水线延迟
        
        # 运行状态
        self.agents = {}  # 存储已实例化的智能体
        self.threads = {}  # 存储线程对象
        self.running = False
        self.stats = {
            "start_time": None,
            "total_cycles": 0,
            "agent_stats": {},
            "errors": []
        }
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("🚀 Agent Runner 初始化完成")
        self.logger.info(f"📋 运行模式: {self.mode}, 启用智能体: {len([a for a in self.agent_configs if a['enabled']])}")
    
    def _setup_logging(self):
        """设置日志系统"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('agent_runner.log', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"🛑 收到停止信号 {signum}, 正在优雅关闭...")
        self.stop()
    
    def _load_agent(self, config: Dict) -> Optional[object]:
        """
        加载单个智能体
        
        Args:
            config: 智能体配置
            
        Returns:
            智能体实例
        """
        try:
            module_name = config["module"]
            class_name = config["class"]
            
            # 动态导入模块
            module = import_module(module_name)
            agent_class = getattr(module, class_name)
            
            # 实例化智能体
            agent = agent_class()
            
            self.logger.info(f"✅ 加载成功: {config['name']} ({class_name})")
            return agent
            
        except ImportError as e:
            self.logger.error(f"❌ 模块导入失败: {config['name']} - {e}")
            return None
            
        except AttributeError as e:
            self.logger.error(f"❌ 类不存在: {config['name']} - {e}")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ 智能体加载异常: {config['name']} - {e}")
            return None
    
    def _load_all_agents(self) -> bool:
        """
        加载所有启用的智能体
        
        Returns:
            是否全部加载成功
        """
        self.logger.info("📥 开始加载智能体...")
        
        success_count = 0
        total_count = 0
        
        for config in self.agent_configs:
            if not config["enabled"]:
                self.logger.info(f"⏸️ 跳过禁用的智能体: {config['name']}")
                continue
                
            total_count += 1
            agent = self._load_agent(config)
            
            if agent:
                self.agents[config["module"]] = {
                    "instance": agent,
                    "config": config,
                    "stats": {
                        "runs": 0,
                        "successes": 0,
                        "errors": 0,
                        "last_run": None,
                        "last_result": None
                    }
                }
                success_count += 1
            else:
                self.logger.error(f"❌ {config['name']} 加载失败")
        
        self.logger.info(f"📊 智能体加载完成: {success_count}/{total_count} 成功")
        return success_count == total_count
    
    def _check_dependencies(self, config: Dict) -> bool:
        """
        检查智能体依赖是否满足
        
        Args:
            config: 智能体配置
            
        Returns:
            依赖是否满足
        """
        for dep in config.get("dependencies", []):
            if dep not in self.agents:
                self.logger.warning(f"⚠️ {config['name']} 缺少依赖: {dep}")
                return False
        return True
    
    def _run_agent_once(self, agent_key: str) -> bool:
        """
        运行单个智能体一次
        
        Args:
            agent_key: 智能体标识
            
        Returns:
            是否成功
        """
        if agent_key not in self.agents:
            return False
        
        agent_data = self.agents[agent_key]
        agent = agent_data["instance"]
        config = agent_data["config"]
        stats = agent_data["stats"]
        
        try:
            self.logger.info(f"▶️ 运行 {config['name']}")
            
            start_time = time.time()
            result = agent.run_once()
            execution_time = time.time() - start_time
            
            # 更新统计
            stats["runs"] += 1
            stats["successes"] += 1
            stats["last_run"] = datetime.now().isoformat()
            stats["last_result"] = result
            stats["execution_time"] = execution_time
            
            self.logger.info(f"✅ {config['name']} 完成: {result} (耗时 {execution_time:.2f}s)")
            return True
            
        except Exception as e:
            stats["runs"] += 1
            stats["errors"] += 1
            stats["last_run"] = datetime.now().isoformat()
            stats["last_result"] = f"错误: {str(e)}"
            
            error_info = f"{config['name']}: {str(e)}"
            self.stats["errors"].append({
                "time": datetime.now().isoformat(),
                "agent": config["name"],
                "error": str(e)
            })
            
            self.logger.error(f"❌ {config['name']} 运行失败: {e}")
            return False
    
    def _run_pipeline_once(self) -> Dict[str, bool]:
        """
        按照依赖顺序运行一次完整流水线
        
        Returns:
            各智能体运行结果
        """
        self.logger.info("🔄 开始流水线执行")
        
        results = {}
        
        # 按优先级排序
        sorted_configs = sorted(
            [config for config in self.agent_configs if config["enabled"]],
            key=lambda x: x["priority"]
        )
        
        for config in sorted_configs:
            agent_key = config["module"]
            
            # 检查依赖
            if not self._check_dependencies(config):
                self.logger.warning(f"⚠️ 跳过 {config['name']}: 依赖不满足")
                results[agent_key] = False
                continue
            
            # 运行智能体
            success = self._run_agent_once(agent_key)
            results[agent_key] = success
            
            # 流水线延迟
            if success and self.pipeline_delay > 0:
                self.logger.debug(f"😴 流水线延迟 {self.pipeline_delay}s")
                time.sleep(self.pipeline_delay)
        
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        self.logger.info(f"📊 流水线执行完成: {success_count}/{total_count} 成功")
        return results
    
    def _run_concurrent_forever(self):
        """并发运行所有智能体"""
        self.logger.info("🚀 启动并发模式")
        
        for agent_key, agent_data in self.agents.items():
            config = agent_data["config"]
            agent = agent_data["instance"]
            
            def agent_worker(key=agent_key, cfg=config, instance=agent):
                self.logger.info(f"🏃 启动线程: {cfg['name']}")
                try:
                    instance.run_forever()
                except Exception as e:
                    self.logger.error(f"❌ {cfg['name']} 线程异常: {e}")
            
            thread = threading.Thread(target=agent_worker, name=config["name"])
            thread.daemon = True
            thread.start()
            
            self.threads[agent_key] = thread
            time.sleep(1)  # 错开启动时间
    
    def run(self):
        """启动智能体运行"""
        try:
            # 加载智能体
            if not self._load_all_agents():
                self.logger.error("❌ 智能体加载不完整，退出")
                return
            
            if not self.agents:
                self.logger.error("❌ 没有可用的智能体")
                return
            
            self.running = True
            self.stats["start_time"] = datetime.now().isoformat()
            
            if self.mode == "run_once":
                self._run_mode_once()
            elif self.mode == "pipeline":
                self._run_mode_pipeline()
            elif self.mode == "run_forever":
                self._run_mode_forever()
            else:
                self.logger.error(f"❌ 未知运行模式: {self.mode}")
                
        except KeyboardInterrupt:
            self.logger.info("🛑 收到中断信号")
        except Exception as e:
            self.logger.error(f"❌ 运行异常: {e}")
        finally:
            self.stop()
    
    def _run_mode_once(self):
        """单次运行模式"""
        self.logger.info("🎯 单次运行模式")
        results = self._run_pipeline_once()
        self._print_summary(results)
    
    def _run_mode_pipeline(self):
        """流水线模式"""
        self.logger.info(f"🔁 流水线模式，间隔 {self.global_interval}s")
        
        while self.running:
            try:
                cycle_start = time.time()
                results = self._run_pipeline_once()
                
                self.stats["total_cycles"] += 1
                cycle_time = time.time() - cycle_start
                
                self.logger.info(f"⏱️ 周期 {self.stats['total_cycles']} 完成，耗时 {cycle_time:.2f}s")
                
                # 等待下一个周期
                if self.running:
                    self.logger.debug(f"😴 等待 {self.global_interval}s 后继续...")
                    time.sleep(self.global_interval)
                    
            except Exception as e:
                self.logger.error(f"❌ 流水线周期异常: {e}")
                time.sleep(60)  # 异常时等待1分钟
    
    def _run_mode_forever(self):
        """持续运行模式（并发）"""
        self.logger.info("♾️ 持续运行模式")
        self._run_concurrent_forever()
        
        # 主线程监控
        try:
            while self.running:
                time.sleep(10)
                # 这里可以添加健康检查逻辑
                alive_threads = sum(1 for t in self.threads.values() if t.is_alive())
                if alive_threads < len(self.threads):
                    self.logger.warning(f"⚠️ 检测到线程异常，存活: {alive_threads}/{len(self.threads)}")
                    
        except KeyboardInterrupt:
            self.logger.info("🛑 主线程收到中断信号")
    
    def stop(self):
        """停止所有智能体"""
        if not self.running:
            return
            
        self.logger.info("🛑 正在停止所有智能体...")
        self.running = False
        
        # 等待线程结束
        for agent_key, thread in self.threads.items():
            if thread.is_alive():
                self.logger.info(f"⏳ 等待 {agent_key} 线程结束...")
                thread.join(timeout=5)
                
                if thread.is_alive():
                    self.logger.warning(f"⚠️ {agent_key} 线程未能正常结束")
        
        self._print_final_summary()
        self.logger.info("✅ 所有智能体已停止")
    
    def _print_summary(self, results: Dict[str, bool]):
        """打印运行摘要"""
        self.logger.info("=" * 50)
        self.logger.info("📊 运行摘要")
        self.logger.info("=" * 50)
        
        for agent_key, success in results.items():
            agent_data = self.agents.get(agent_key, {})
            config = agent_data.get("config", {})
            name = config.get("name", agent_key)
            status = "✅ 成功" if success else "❌ 失败"
            self.logger.info(f"{name}: {status}")
    
    def _print_final_summary(self):
        """打印最终统计摘要"""
        if not self.stats["start_time"]:
            return
            
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.stats["start_time"])
        total_time = end_time - start_time
        
        self.logger.info("=" * 60)
        self.logger.info("📈 最终统计摘要")
        self.logger.info("=" * 60)
        self.logger.info(f"运行时间: {total_time}")
        self.logger.info(f"总周期数: {self.stats['total_cycles']}")
        self.logger.info(f"错误次数: {len(self.stats['errors'])}")
        
        self.logger.info("\n各智能体统计:")
        for agent_key, agent_data in self.agents.items():
            config = agent_data["config"]
            stats = agent_data["stats"]
            
            success_rate = stats["successes"] / stats["runs"] if stats["runs"] > 0 else 0
            
            self.logger.info(f"{config['name']}:")
            self.logger.info(f"  运行次数: {stats['runs']}")
            self.logger.info(f"  成功次数: {stats['successes']}")
            self.logger.info(f"  错误次数: {stats['errors']}")
            self.logger.info(f"  成功率: {success_rate:.2%}")
            self.logger.info(f"  最后运行: {stats.get('last_run', '未运行')}")
            if 'execution_time' in stats:
                self.logger.info(f"  平均耗时: {stats['execution_time']:.2f}s")
        
        if self.stats["errors"]:
            self.logger.info(f"\n最近错误 (显示最后5个):")
            for error in self.stats["errors"][-5:]:
                self.logger.info(f"  {error['time']}: {error['agent']} - {error['error']}")
    
    def get_status(self) -> Dict:
        """获取运行状态"""
        status = {
            "running": self.running,
            "mode": self.mode,
            "start_time": self.stats["start_time"],
            "total_cycles": self.stats["total_cycles"],
            "agents": {}
        }
        
        for agent_key, agent_data in self.agents.items():
            config = agent_data["config"]
            stats = agent_data["stats"]
            thread = self.threads.get(agent_key)
            
            status["agents"][config["name"]] = {
                "enabled": config["enabled"],
                "priority": config["priority"],
                "thread_alive": thread.is_alive() if thread else False,
                "runs": stats["runs"],
                "successes": stats["successes"],
                "errors": stats["errors"],
                "last_run": stats.get("last_run"),
                "last_result": stats.get("last_result")
            }
        
        return status
    
    def enable_agent(self, agent_name: str) -> bool:
        """启用智能体"""
        for config in self.agent_configs:
            if config["name"] == agent_name or config["module"] == agent_name:
                config["enabled"] = True
                self.logger.info(f"✅ 启用智能体: {config['name']}")
                return True
        return False
    
    def disable_agent(self, agent_name: str) -> bool:
        """禁用智能体"""
        for config in self.agent_configs:
            if config["name"] == agent_name or config["module"] == agent_name:
                config["enabled"] = False
                self.logger.info(f"⏸️ 禁用智能体: {config['name']}")
                return True
        return False
    
    def restart_agent(self, agent_name: str) -> bool:
        """重启单个智能体（仅在并发模式下有效）"""
        if self.mode != "run_forever":
            self.logger.warning("⚠️ 只有在持续运行模式下才能重启智能体")
            return False
        
        # 查找智能体
        target_key = None
        for agent_key, agent_data in self.agents.items():
            config = agent_data["config"]
            if config["name"] == agent_name or config["module"] == agent_name:
                target_key = agent_key
                break
        
        if not target_key:
            self.logger.error(f"❌ 未找到智能体: {agent_name}")
            return False
        
        try:
            # 停止现有线程
            if target_key in self.threads:
                thread = self.threads[target_key]
                if thread.is_alive():
                    self.logger.info(f"🛑 停止 {agent_name} 线程...")
                    # 这里需要智能体支持优雅停止
                    thread.join(timeout=10)
            
            # 重新启动
            agent_data = self.agents[target_key]
            config = agent_data["config"]
            agent = agent_data["instance"]
            
            def agent_worker():
                self.logger.info(f"🔄 重启线程: {config['name']}")
                try:
                    agent.run_forever()
                except Exception as e:
                    self.logger.error(f"❌ {config['name']} 重启线程异常: {e}")
            
            thread = threading.Thread(target=agent_worker, name=f"{config['name']}-restart")
            thread.daemon = True
            thread.start()
            
            self.threads[target_key] = thread
            self.logger.info(f"✅ {agent_name} 重启成功")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 重启 {agent_name} 失败: {e}")
            return False


def main():
    """主入口函数"""
    print("=" * 60)
    print("🤖 基于多智能体的热点自适应保险营销内容生成系统")
    print("=" * 60)
    
    try:
        # 创建并启动运行器
        runner = AgentRunner()
        
        # 显示配置信息
        print(f"📋 运行模式: {runner.mode}")
        print(f"⚙️ 全局间隔: {runner.global_interval}s")
        print(f"🧵 最大工作线程: {runner.max_workers}")
        print(f"⏱️ 流水线延迟: {runner.pipeline_delay}s")
        print("=" * 60)
        
        # 启动系统
        runner.run()
        
    except KeyboardInterrupt:
        print("\n🛑 用户中断")
    except Exception as e:
        print(f"\n❌ 系统异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n👋 系统已退出")


if __name__ == "__main__":
    main()