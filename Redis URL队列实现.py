import redis
import json

class RedisURLQueue:
    def __init__(self, host='localhost', port=6379, db=0, queue_name='url_queue', processed_set='processed_urls'):
        try:
            self.redis_client = redis.Redis(host=host, port=port, db=db)
            self.queue_name = queue_name
            self.processed_set = processed_set
            self.redis_client.ping()
            print("Connected to Redis successfully!")
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            raise

    def enqueue_with_dedup(self, url, metadata=None):
        """
        同时去重和加入队列
        :param url: 要加入队列的URL
        :param metadata: 可选的元数据
        """
        # 检查URL是否已经被处理过
        if self.redis_client.sismember(self.processed_set, url):
            print(f"URL {url} already processed, skipping...")
            return
        
        item = {'url': url}
        if metadata:
            item['metadata'] = metadata
        
        # 将URL和元数据加入队列
        self.redis_client.rpush(self.queue_name, json.dumps(item))
        # 将URL标记为已处理
        self.redis_client.sadd(self.processed_set, url)

    def enqueue(self, url, metadata=None):
        """
        将URL和可选的元数据加入队列
        :param url: 要加入队列的URL
        :param metadata: 可选的元数据
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
        self.redis_client.delete(self.processed_set)

    def peek(self):
        """
        查看队列头部的元素，但不移除它
        :return: 队列头部的URL及其元数据，如果没有数据则返回None
        """
        item = self.redis_client.lindex(self.queue_name, 0)
        if item:
            return json.loads(item)
        return None

    def distribute_tasks(self, num_tasks=10):
        """
        分发指定数量的任务到多个消费者
        :param num_tasks: 要分发的任务数量
        :return: 分发的任务列表
        """
        tasks = []
        for _ in range(num_tasks):
            task = self.redis_client.rpoplpush(self.queue_name, 'processing_queue')
            if task:
                tasks.append(json.loads(task))
        return tasks

    def acknowledge_completion(self, task):
        """
        确认任务完成，并从处理队列中移除
        :param task: 完成的任务
        """
        url = task['url']
        self.redis_client.srem(self.processed_set, url)
        self.redis_client.lrem('processing_queue', 0, json.dumps(task))


# 示例用法
if __name__ == "__main__":
    try:
        queue = RedisURLQueue()

        # 添加URL到队列（支持去重）
        queue.enqueue_with_dedup("http://example.com", metadata={"priority": 1})
        queue.enqueue_with_dedup("http://example.org", metadata={"priority": 2})
        queue.enqueue_with_dedup("http://example.net", metadata={"priority": 3})
        queue.enqueue_with_dedup("http://example.com", metadata={"priority": 1})  # 重复URL，会被去重

        print("Queue size:", queue.size())  # 队列大小为3

        # 分发任务
        distributed_tasks = queue.distribute_tasks(num_tasks=2)
        print("Distributed tasks:", distributed_tasks)

        # 模拟任务完成
        if distributed_tasks:
            completed_task = distributed_tasks[0]
            queue.acknowledge_completion(completed_task)
            print("Task completed:", completed_task)

        print("Queue size after completion:", queue.size())  # 队列大小减少

        # 清空队列
        queue.clear()
        print("Queue size after clear:", queue.size())  # 队列大小为0

    except Exception as e:
        print(f"An error occurred: {e}")