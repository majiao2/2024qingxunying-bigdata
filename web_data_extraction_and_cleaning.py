import requests
from bs4 import BeautifulSoup
import re
import json
import logging
import pandas as pd
import importlib.util

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 使用会话复用
session = requests.Session()

def fetch_page_content(url):
    """
    从指定 URL 获取网页内容
    :param url: 要请求的 URL
    :return: 网页的 HTML 内容，如果请求失败则返回 None
    """
    try:
        response = session.get(url)
        response.raise_for_status()  # 检查请求是否成功
        return response.text
    except requests.RequestException as e:
        logging.error(f"请求 {url} 时出现错误: {e}")
        return None

def load_extraction_rules():
    """
    从 JSON 配置文件中加载数据提取规则
    :return: 包含提取规则的字典
    """
    try:
        with open('extraction_rules.json', 'r', encoding='utf-8') as file:
            rules = json.load(file)
            # 预处理规则，将规则按照域名存储
            domain_rules = {}
            for domain, rule in rules.items():
                domain_rules[domain] = rule.get('selectors', [])
            return domain_rules
    except FileNotFoundError:
        logging.warning("未找到提取规则配置文件 'extraction_rules.json'，将使用默认规则。")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"解析提取规则配置文件时出错: {e}")
        return {}

def extract_data(html_content, url, rules):
    """
    根据规则从 HTML 内容中提取数据
    :param html_content: 网页的 HTML 内容
    :param url: 当前处理的 URL
    :param rules: 提取规则字典
    :return: 提取到的数据列表
    """
    soup = BeautifulSoup(html_content, 'lxml')
    data = []
    for domain, selectors in rules.items():
        if domain in url:
            for selector in selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text()
                    data.append(text)
            break
    if not data:
        # 如果没有匹配到规则，使用默认提取方式（提取所有 <p> 标签）
        paragraphs = soup.find_all('p')
        data = [p.get_text() for p in paragraphs]
    return data

def clean_data(data):
    """
    使用 pandas 对提取到的数据进行清洗
    去除多余的空格和换行符，统一格式，进行数据验证与去重
    :param data: 提取到的数据列表
    :return: 清洗后的数据列表
    """
    df = pd.DataFrame(data, columns=['text'])
    # 去除首尾空格和换行符
    df['text'] = df['text'].str.strip()
    # 去除多余的连续空格
    df['text'] = df['text'].str.replace(r'\s+', ' ', regex=True)
    # 去除空值
    df = df.dropna(subset=['text'])

    # 数据去重
    df = df.drop_duplicates(subset=['text'])

    return df['text'].tolist()

def save_to_json(data, url):
    """
    将清洗后的数据保存到 JSON 文件中
    :param data: 清洗后的数据列表
    :param url: 当前处理的 URL，用于生成文件名
    """
    filename = url.replace('https://', '').replace('http://', '').replace('/', '_') + '.json'
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        logging.info(f"数据已保存到 {filename}")
    except Exception as e:
        logging.error(f"保存数据到 {filename} 时出错: {e}")

def process_url(url, rules):
    """
    处理单个 URL，包括获取内容、提取数据和清洗数据
    :param url: 要处理的 URL
    :param rules: 提取规则字典
    :return: 清洗后的数据列表
    """
    html_content = fetch_page_content(url)
    if html_content:
        extracted_data = extract_data(html_content, url, rules)
        cleaned_data = clean_data(extracted_data)
        save_to_json(cleaned_data, url)
        # 手动释放不再使用的变量
        html_content = None
        extracted_data = None
        return cleaned_data
    return []

if __name__ == "__main__":
    # 动态导入 RedisURLQueue 类
    spec = importlib.util.spec_from_file_location("RedisURLQueueModule", "Redis URL队列实现.py")
    RedisURLQueueModule = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(RedisURLQueueModule)
    RedisURLQueue = RedisURLQueueModule.RedisURLQueue

    # 连接到 Redis 队列
    try:
        queue = RedisURLQueue(host='localhost', port=6379, db=0)
        logging.info("Connected to Redis successfully!")
    except Exception as e:
        logging.error(f"Failed to connect to Redis: {e}")
        raise

    # 加载提取规则
    extraction_rules = load_extraction_rules()

    # 从 Redis 队列中获取 URL 并处理
    while True:
        task = queue.dequeue()
        if not task:
            logging.info("No more URLs in the queue. Exiting...")
            break
        url = task['url']
        logging.info(f"Processing URL: {url}")
        result = process_url(url, extraction_rules)
        if result:
            logging.info("Cleaned data has been saved.")
            # 确认任务完成
            queue.acknowledge_completion(task)
        else:
            logging.info("No valid data was retrieved.")
        logging.info("-" * 50)