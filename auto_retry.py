import logging
from retry import retry

# 配置日志记录
logging.basicConfig(filename='error.log', level=logging.ERROR)

@retry(tries=3, delay=2)
def insert_data_with_retry(data):
    """
    带有重试机制的数据插入函数
    :param data: 清洗后的数据，格式为字典，包含url、title、content、metadata、price、description等字段
    """
    try:
        insert_data(data)
    except Exception as e:
        logging.error(f"数据插入失败：{data['url']}，错误原因：{e}")
        raise