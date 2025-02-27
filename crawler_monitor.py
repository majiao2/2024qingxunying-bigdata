import redis
from flask import Flask, jsonify, request

app = Flask(__name__)
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

@app.route('/status', methods=['GET'])
def check_status():
    """
    监控爬虫状态
    """
    workers = redis_client.hgetall("crawler_status")
    return jsonify(workers)

@app.route('/update_status', methods=['POST'])
def update_status():
    """
    爬虫进程定期更新状态
    """
    import time
    data = request.get_json()
    redis_client.hset("crawler_status", data["worker_id"], time.time())
    return jsonify({"status": "updated"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
