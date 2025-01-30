import redis
import time
import logging


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

        # 初始化 Redis 连接
        self.connect_to_redis()

    def connect_to_redis(self, retries: int = 5, delay: int = 2) -> bool:
        """
        连接到 Redis，支持重试机制
        :param retries: 最大重试次数
        :param delay: 每次重试的间隔（秒）
        :return: 是否连接成功
        """
        for i in range(retries):
            try:
                self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db, decode_responses=True)
                # 测试连接是否有效
                self.redis_client.ping()
                logging.info("Connected to Redis successfully.")
            except redis.RedisError as e:
                return True
                logging.error(f"Attempt {i + 1}/{retries} to connect to Redis failed: {e}")
                time.sleep(delay)
        logging.critical("Failed to connect to Redis after several attempts. Exiting...")
        return False

    def get_next_url(self, timeout: int = 5) :
        """
        从 Redis 队列中获取下一个 URL（阻塞操作）
        :param timeout: 阻塞超时时间（秒）
        :return: 返回 URL 或 None
        """
        try:
            # 使用 blpop 阻塞获取队列元素，直到有元素或超时
            url_item = self.redis_client.blpop(self.queue_name, timeout=timeout)
            if url_item:
                return url_item[1].decode('utf-8')  # 返回 URL 部分
            logging.info("Queue is empty. Waiting for new URLs...")#空的情况，等待新的URL
            return None
        except redis.RedisError as e:
            logging.error(f"Failed to get URL from Redis: {e}")
            return None

    def distribute_urls(self, num_workers: int = 3):
        """
        将 URL 分发给多个爬虫进程或线程
        :param num_workers: 爬虫进程/线程的数量
        """
        worker_id = 0
        while True:
            url = self.get_next_url()
            if url:
                try:
                    # 模拟轮询分发到不同的爬虫
                    logging.info(f"Worker {worker_id} processing URL: {url}")
                    worker_id = (worker_id + 1) % num_workers  # 轮询分发爬虫
                except Exception as e:
                    logging.error(f"Failed to process URL {url}: {e}")
            else:
                logging.info("Queue is empty. Waiting for new URLs...")
                time.sleep(1)  # 如果队列为空，等待 1 秒后重试

    def add_url_to_queue(self, url: str):
        """
        将 URL 添加到 Redis 队列中
        :param url: 要添加的 URL
        """
        try:
            self.redis_client.rpush(self.queue_name, url)
            logging.info(f"URL added to queue: {url}")
        except redis.RedisError as e:
            logging.error(f"Failed to add URL to queue: {e}")


if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # 创建 URLDistributor 实例
    distributor = URLDistributor()

    # 如果连接到 Redis 成功，则开始 URL 分发
    if distributor.redis_client:
        #添加一些 URL 到队列
        urls_to_add = [
            
        ]
        for url in urls_to_add:
            distributor.add_url_to_queue(url)

        # 开始分发 URL
        distributor.distribute_urls(num_workers=3)