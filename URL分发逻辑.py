import redis
import time
import logging
import threading
import signal
import sys
import argparse
import requests

class URLDistributor:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0, queue_name: str = 'url_queue'):
        """
        初始化 Redis 连接，配置日志和队列信息
        :param redis_host: Redis服务器地址
        :param redis_port: Redis服务器端口
        :param redis_db: Redis数据库编号
        :param queue_name: URL队列名称
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.queue_name = queue_name
        self.redis_client = None
        self.running = False  # 控制线程运行的标志

        # 初始化 Redis 连接
        if not self.connect_to_redis():
            raise RuntimeError("Failed to connect to Redis.")

        # 注册信号处理函数
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def connect_to_redis(self, retries: int = 5, delay: int = 2) -> bool:
        """
        连接到 Redis，支持重试机制
        :param retries: 最大重试次数
        :param delay: 每次重试的间隔（秒）
        :return: 是否连接成功
        """
        for i in range(retries):
            try:
                self.redis_client = redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    db=self.redis_db,
                    decode_responses=True
                )
                # 测试连接是否有效
                self.redis_client.ping()
                logging.info("Connected to Redis successfully.")
                return True  # 连接成功
            except redis.RedisError as e:
                logging.error(f"Attempt {i + 1}/{retries} to connect to Redis failed: {e}")
                time.sleep(delay)
        logging.critical("Failed to connect to Redis after several attempts.")
        return False  # 连接失败

    def get_next_url(self, timeout: int = 5):
        """
        从 Redis 队列中获取下一个 URL（阻塞操作）
        :param timeout: 阻塞超时时间（秒）
        :return: 返回 URL 或 None
        """
        try:
            # 使用 blpop 阻塞获取队列元素，直到有元素或超时
            url_item = self.redis_client.blpop(self.queue_name, timeout=timeout)
            if url_item:
                return url_item[1]  # 返回 URL 部分
            logging.info("Queue is empty. Waiting for new URLs...")
            return None
        except redis.RedisError as e:
            logging.error(f"Failed to get URL from Redis: {e}")
            return None

    def distribute_urls(self, num_workers: int = 3, heartbeat_interval: int = 60):
        """
        将 URL 分发给多个爬虫进程或线程，并定期检查 Redis 连接
        :param num_workers: 爬虫进程/线程的数量
        :param heartbeat_interval: 心跳检查间隔（秒）
        """
        self.running = True

        def worker(worker_id):
            """
            工作线程函数，处理 URL
            """
            last_heartbeat = time.time()  # 上次 Redis 心跳时间
            status_heartbeat = time.time()  # 上次状态更新时间
            status_interval = 60  # 定期更新爬虫状态的时间间隔（秒）

            while self.running:
                # 定期检查 Redis 连接
                if time.time() - last_heartbeat > heartbeat_interval:
                    if not self.redis_client.ping():
                        logging.error(f"Worker {worker_id} lost connection to Redis. Reconnecting...")
                        self.connect_to_redis()
                    last_heartbeat = time.time()

                # 定期发送爬虫状态更新请求
                if time.time() - status_heartbeat > status_interval:  # 每隔一定时间更新一次状态
                    try:
                        # 向 Flask API 发送 POST 请求，更新爬虫状态
                        requests.post("http://localhost:5001/update_status", json={"worker_id": worker_id})
                        logging.info(f"Worker {worker_id} status updated.")
                    except requests.RequestException as e:
                        logging.error(f"Failed to update status for worker {worker_id}: {e}")
                    status_heartbeat = time.time()  # 更新状态心跳时间

                # 获取 URL 并处理
                url = self.get_next_url()
                if url:
                    try:
                        logging.info(f"Worker {worker_id} processing URL: {url}")
                        self.process_url(url)  # 处理 URL
                    except Exception as e:
                        logging.error(f"Worker {worker_id} failed to process URL {url}: {e}")
                else:
                    logging.info(f"Worker {worker_id} is idle. Waiting for new URLs...")
                    time.sleep(1)  # 如果队列为空，等待 1 秒后重试

        # 创建并启动多个工作线程
        threads = []
        for i in range(num_workers):
            thread = threading.Thread(target=worker, args=(i,))
            thread.start()
            threads.append(thread)

        # 等待所有线程完成（通常不会结束）
        for thread in threads:
            thread.join()

    def process_url(self, url: str):
        """
        处理 URL 并将结果存储到 Redis
        :param url: 要处理的 URL
        """
        try:
            # 模拟 URL 处理逻辑
            result = f"Processed {url}"
            self.redis_client.hset("url_results", url, result)
            logging.info(f"URL processed: {url}")
        except Exception as e:
            logging.error(f"Failed to process URL {url}: {e}")

    def add_url_to_queue(self, url: str):
        """
        将 URL 添加到 Redis 队列中，并确保 URL 不重复
        :param url: 要添加的 URL
        """
        try:
            if not self.redis_client.sismember("processed_urls", url):
                self.redis_client.rpush(self.queue_name, url)
                self.redis_client.sadd("processed_urls", url)
                logging.info(f"URL added to queue: {url}")
            else:
                logging.info(f"URL already processed: {url}")
        except redis.RedisError as e:
            logging.error(f"Failed to add URL to queue: {e}")

    def add_urls_from_file(self, file_path: str):
        """
        从文件中读取 URL 并添加到队列中
        :param file_path: 文件路径
        """
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    url = line.strip()
                    if url:
                        self.add_url_to_queue(url)
            logging.info(f"URLs from {file_path} added to queue.")
        except Exception as e:
            logging.error(f"Failed to read URLs from file {file_path}: {e}")

    def close(self):
        """
        关闭 Redis 连接
        """
        if self.redis_client:
            self.redis_client.close()
            logging.info("Redis connection closed.")

    def signal_handler(self, signum, frame):
        """
        处理终止信号，优雅退出
        """
        logging.info(f"Received signal {signum}. Shutting down...")
        self.running = False
        self.close()
        sys.exit(0)


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="URL Distributor")
    parser.add_argument('--redis_host', type=str, default='localhost', help='Redis server address')
    parser.add_argument('--redis_port', type=int, default=6379, help='Redis server port')
    parser.add_argument('--redis_db', type=int, default=0, help='Redis database number')
    parser.add_argument('--queue_name', type=str, default='url_queue', help='Redis queue name')
    parser.add_argument('--num_workers', type=int, default=3, help='Number of worker threads')
    parser.add_argument('--url_file', type=str, default=None, help='Path to file containing URLs')
    args = parser.parse_args()

    # 创建 URLDistributor 实例
    distributor = URLDistributor(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        queue_name=args.queue_name
    )

    # 添加 URL 到队列
    if args.url_file:
        distributor.add_urls_from_file(args.url_file)
    else:
        urls_to_add = [
            "https://quotes.toscrape.com/",
            "https://www.example.com/",
            "https://www.test.com/",
            "https://www.demo.com/",
            "https://www.sample.com/",
        ]
        for url in urls_to_add:
            distributor.add_url_to_queue(url)

    # 开始分发 URL
    distributor.distribute_urls(num_workers=args.num_workers)
