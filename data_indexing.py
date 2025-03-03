# 数据索引与查询
## 利用 Elasticsearch 为数据建立索引，以提升后续查询效率。

from elasticsearch import Elasticsearch

# 连接Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def index_data(data):
    """
    将数据索引到Elasticsearch中
    :param data: 清洗后的数据，格式为字典，包含url、title、content、metadata、price、description等字段
    """
    try:
        es.index(index='crawled_data', id=data['url'], body=data)
        print(f"数据索引成功：{data['url']}")
    except Exception as e:
        print(f"数据索引失败：{data['url']}，错误原因：{e}")