import redis
import json

class RedisURLQueue:
    def __init__(self, host='localhost', port=6379, db=0, queue_name='url_queue'):
        """
        初始化Redis连接和队列名称
        :param host: Redis服务器地址
        :param port: Redis服务器端口
        :param db: Redis数据库编号
        :param queue_name: 队列的名称
        """
        try:
            self.redis_client = redis.Redis(host=host, port=port, db=db)
            self.queue_name = queue_name
            # 测试连接
            self.redis_client.ping()
            print("Connected to Redis successfully!")
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            raise

    def enqueue(self, url, metadata=None):
        """
        将URL和可选的元数据加入队列
        :param url: 要加入队列的URL
        :param metadata: 可选的元数据，可以是任意可序列化为JSON的对象
        """
        item = {'url': url}
        if metadata:
            item['metadata'] = metadata
        self.redis_client.rpush(self.queue_name, json.dumps(item))

    def dequeue(self):
        """
        从队列中取出一个URL及其元数据
        :return: URL及其元数据的字典，如果没有数据则返回None
        """
        item = self.redis_client.lpop(self.queue_name)
        if item:
            return json.loads(item)
        return None

    def size(self):
        """
        获取队列中元素的数量
        :return: 队列的长度
        """
        return self.redis_client.llen(self.queue_name)

    def clear(self):
        """
        清空队列
        """
        self.redis_client.delete(self.queue_name)

    def peek(self):
        """
        查看队列头部的元素，但不移除它
        :return: 队列头部的URL及其元数据，如果没有数据则返回None
        """
        item = self.redis_client.lindex(self.queue_name, 0)
        if item:
            return json.loads(item)
        return None


# 示例用法
if __name__ == "__main__":
    try:
        queue = RedisURLQueue()

        # 添加URL到队列
        queue.enqueue("http://example.com", metadata={"priority": 1})
        queue.enqueue("http://example.org", metadata={"priority": 2})
        queue.enqueue("http://example.net")

        print("Queue size:", queue.size())  # 输出队列大小

        # 查看队列头部的元素
        print("Peek at the first item:", queue.peek())

        # 从队列中取出URL
        while True:
            item = queue.dequeue()
            if item is None:
                break
            print("Dequeued item:", item)

        print("Queue size after dequeue:", queue.size())  # 输出队列大小

        # 清空队列
        queue.clear()
        print("Queue size after clear:", queue.size())  # 输出队列大小

    except Exception as e:
        print(f"An error occurred: {e}")