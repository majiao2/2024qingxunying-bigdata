from flask import Flask, request, jsonify
import pymongo

app = Flask(__name__)

# 连接 MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["web_crawler"]
collection = db["scraped_data"]

@app.route('/search', methods=['GET'])
def search_data():
    """
    按关键词搜索数据
    """
    query = request.args.get('query', '')
    results = collection.find({"title": {"$regex": query, "$options": "i"}}).limit(10)
    return jsonify([{"title": item["title"], "url": item["url"]} for item in results])

@app.route('/get', methods=['GET'])
def get_data():
    """
    分页获取数据
    """
    page = int(request.args.get('page', 1))
    per_page = 10
    skip = (page - 1) * per_page

    results = collection.find().skip(skip).limit(per_page)
    return jsonify([{"title": item["title"], "url": item["url"]} for item in results])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
