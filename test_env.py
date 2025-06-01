import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('config/.env')

# 检查环境变量
glm_key = os.getenv('GLM_API_KEY')
print(f"GLM_API_KEY: {glm_key}")
print(f"GLM_API_KEY 是否存在: {bool(glm_key)}")

# 列出所有环境变量中包含GLM的
for key, value in os.environ.items():
    if 'GLM' in key:
        print(f"{key}: {value}")