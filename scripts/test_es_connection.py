
from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

load_dotenv('config/.env')

es = Elasticsearch(hosts=[os.getenv("ES_HOST")])

if es.ping():
    print("✅ 成功连接到 Elasticsearch！")
else:
    print("❌ 无法连接到 Elasticsearch。")
