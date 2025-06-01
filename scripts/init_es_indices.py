
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

load_dotenv('config/.env')

es = Elasticsearch(hosts=[os.getenv("ES_HOST")])

mappings = {
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "desc": {"type": "text"},
            "platform": {"type": "keyword"},
            "rank": {"type": "integer"},
            "hot": {"type": "float"},
            "人群类型": {"type": "text"},
            "风险类型": {"type": "text"},
            "素材": {"type": "text"},
            "推荐产品": {"type": "nested"},
            "生成文案": {"type": "text"},
            "润色文案": {"type": "text"}
        }
    }
}

es.indices.create(index="daily_hotspots", body=mappings, ignore=400)
es.indices.create(index="insurance-products", ignore=400)
print("✅ 索引初始化完成")
