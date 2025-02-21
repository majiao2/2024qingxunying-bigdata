import random
import re
import time
import pymongo
import requests
from urllib.parse import quote
import json
import logging
import pandas as pd
import importlib.util
from bs4 import BeautifulSoup

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 使用会话复用
session = requests.Session()

# 配置重试参数
REDIS_MAX_RETRIES = 3
REDIS_RETRY_DELAY = 5  # 秒
REQUEST_MAX_RETRIES = 3
REQUEST_TIMEOUT = 10  # 秒

# 从 config.py 中导入配置
from config import *

# 连接 MongoDB
client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DB]
item_id = set()

def get_proxy():
    '''
    获取代理
    '''
    return requests.get("http://127.0.0.1:5010/get/").text

def fetch_page_content(url):
    """
    从指定 URL 获取网页内容，添加重试机制
    :param url: 要请求的 URL
    :return: 网页的 HTML 内容，如果请求失败则返回 None
    """
    retries = 0
    while retries < REQUEST_MAX_RETRIES:
        try:
            proxy = get_proxy()
            headers = {
                'User_Agent': random.choice(USER_AGENT),
                'Referer': 'https://p4psearch.1688.com/p4p114/p4psearch/offer.htm?keywords=' + quote(
                    KEYWORD) + '&sortType=&descendOrder=&province=&city=&priceStart=&priceEnd=&dis=&provinceValue=%E6%89%80%E5%9C%A8%E5%9C%B0%E5%8C%BA',
                'Cookie': COOKIE,
            }
            proxies = {"http": "http://{}".format(proxy)}
            response = session.get(url, headers=headers, proxies=proxies, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()  # 检查请求是否成功
            return response.text if 'html' in response.headers.get('Content-Type', '') else response.json()
        except requests.RequestException as e:
            logging.error(f"请求 {url} 时出现错误 (尝试第 {retries + 1} 次): {e}")
            retries += 1
            if retries < REQUEST_MAX_RETRIES:
                time.sleep(1)  # 等待 1 秒后重试
    logging.error(f"请求 {url} 失败，已达到最大重试次数。")
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
    if isinstance(html_content, dict):  # 处理 JSON 数据
        try:
            print('正在获取商品信息..')
            items = html_content['data']['content']['offerResult']
            extracted_data = []
            for item in items:
                param1 = item['attr']['company']
                company = param1['name']
                company_type = param1['bizTypeName']
                city = param1['city']
                province = param1['province']
                param2 = item['attr']['tradePrice']['offerPrice']
                originalValue = param2['originalValue']['integer'] + param2['originalValue']['decimals'] / 10
                quantityPrices = param2['value']['integer'] + param2['value']['decimals'] / 10
                param3 = item['attr']['tradeQuantity']
                sales = param3['number']
                saleType = param3['sortType']
                detailUrl = item['eurl']
                imgUrl = item['imgUrl']
                originaltitle = item['title']
                title = re.sub('<.*>', '', originaltitle)
                result = {
                    '标题': title,
                    '原价': originalValue,
                    '最低批发价': quantityPrices,
                    '销售量': sales,
                    '销售形式': saleType,
                    '详细链接': detailUrl,
                    '图片链接': imgUrl,
                    '公司': company,
                    '公司类型': company_type,
                    '城市': city,
                    '省份': province,
                }
                extracted_data.append(result)
            return extracted_data
        except (KeyError, TypeError):
            return []
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
    if all(isinstance(item, dict) for item in data):  # 处理 1688 商品信息字典列表
        cleaned_data = []
        for item in data:
            for key, value in item.items():
                if isinstance(value, str):
                    item[key] = re.sub(r'\s+', ' ', value).strip()
            if item['标题'] not in item_id:
                cleaned_data.append(item)
                item_id.add(item['标题'])
        return cleaned_data
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

def save_to_mongo(data):
    """
    将清洗后的 1688 商品数据保存到 MongoDB
    :param data: 清洗后的商品数据列表
    """
    for item in data:
        if db[MONGO_TABLE].insert_one(item):
            print('成功保存至mongoDB', item['标题'])

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
    try:
        html_content = fetch_page_content(url)
        if html_content:
            extracted_data = extract_data(html_content, url, rules)
            cleaned_data = clean_data(extracted_data)
            if all(isinstance(item, dict) for item in cleaned_data):  # 处理 1688 商品信息
                save_to_mongo(cleaned_data)
            else:
                save_to_json(cleaned_data, url)
            # 手动释放不再使用的变量
            html_content = None
            extracted_data = None
            return cleaned_data
    except Exception as e:
        logging.error(f"处理 URL {url} 时出现错误: {e}")
    return []

if __name__ == "__main__":
    # 动态导入 RedisURLQueue 类
    spec = importlib.util.spec_from_file_location("RedisURLQueueModule", "Redis URL队列实现.py")
    RedisURLQueueModule = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(RedisURLQueueModule)
    RedisURLQueue = RedisURLQueueModule.RedisURLQueue

    # 连接到 Redis 队列，添加重试机制
    retries = 0
    while retries < REDIS_MAX_RETRIES:
        try:
            queue = RedisURLQueue(host='localhost', port=6379, db=0)
            logging.info("Connected to Redis successfully!")
            break
        except Exception as e:
            logging.error(f"Failed to connect to Redis (尝试第 {retries + 1} 次): {e}")
            retries += 1
            if retries < REDIS_MAX_RETRIES:
                logging.info(f"将在 {REDIS_RETRY_DELAY} 秒后重试...")
                time.sleep(REDIS_RETRY_DELAY)
    if retries == REDIS_MAX_RETRIES:
        logging.error("Failed to connect to Redis after multiple attempts. Exiting.")
        raise SystemExit(1)

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
        logging.info("-" * 50)
