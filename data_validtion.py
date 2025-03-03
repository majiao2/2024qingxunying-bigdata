# 数据验证与去重
## 在存储数据之前，验证数据的准确性，并避免存储重复数据。

def validate_data(data):
    """
    验证数据的准确性
    :param data: 清洗后的数据，格式为字典，包含url、title、content、metadata、price、description等字段
    :return: 验证结果，True表示数据有效，False表示数据无效
    """
    if not data.get('url') or not data.get('title') or not data.get('price'):
        print(f"数据验证失败，缺少必要字段：{data}")
        return False
    return True

def check_duplicate(url):
    """
    检查URL是否已存在于数据库中
    :param url: 要检查的URL
    :return: 检查结果，True表示URL已存在，False表示URL不存在
    """
    existing_data = session.query(CrawledData).filter_by(url=url).first()
    return existing_data is not None