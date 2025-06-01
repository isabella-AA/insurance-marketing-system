
import json
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os

load_dotenv('config/.env')

es = Elasticsearch(hosts=[os.getenv("ES_HOST")])

with open("insurance_products.json", "r", encoding="utf-8") as f:
    products = json.load(f)

actions = [
    {"_index": "insurance-products", "_source": prod}
    for prod in products
]

success, _ = helpers.bulk(es, actions, stats_only=True)
print(f"✅ 成功上传 {success} 条保险产品记录")
