import socket
import redis

class DNSResolver:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

    def resolve_url(self, url):
        """
        解析 URL 并缓存结果
        """
        cached_ip = self.redis_client.get(f"dns:{url}")
        if cached_ip:
            return cached_ip

        try:
            ip_address = socket.gethostbyname(url)
            self.redis_client.setex(f"dns:{url}", 86400, ip_address)  # 缓存 24 小时
            return ip_address
        except socket.gaierror:
            return None
