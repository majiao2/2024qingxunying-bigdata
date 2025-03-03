# MySQL 存储
## 将清洗后的数据插入到 MySQL 中。选择使用 SQLAlchemy 库，因为它提供了更高级的数据库操作功能，同时也兼容多种数据库系统。

import pymysql
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# 定义数据库连接信息
DB_USER = 'your_username'
DB_PASSWORD = 'your_password'
DB_HOST = 'localhost'
DB_NAME = 'your_database_name'

# 创建数据库引擎
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')
Session = sessionmaker(bind=engine)
session = Session()

# 定义数据表模型
Base = declarative_base()

class CrawledData(Base):
    __tablename__ = 'crawled_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String(255), unique=True)
    title = Column(String(255))
    content = Column(Text)
    metadata = Column(Text)
    price = Column(String(50))
    description = Column(Text)

# 创建数据表（如果不存在）
Base.metadata.create_all(engine)

def insert_data(data):
    """
    将清洗后的数据插入到MySQL中
    :param data: 清洗后的数据，格式为字典，包含url、title、content、metadata、price、description等字段
    """
    try:
        # 检查URL是否已存在
        existing_data = session.query(CrawledData).filter_by(url=data['url']).first()
        if existing_data:
            print(f"URL {data['url']} 已存在，跳过插入")
            return

        # 验证数据的准确性
        if not data.get('title') or not data.get('price'):
            print(f"数据验证失败，缺少必要字段：{data}")
            return

        # 插入数据
        new_data = CrawledData(
            url=data['url'],
            title=data['title'],
            content=data['content'],
            metadata=data['metadata'],
            price=data['price'],
            description=data['description']
        )
        session.add(new_data)
        session.commit()
        print(f"数据插入成功：{data['url']}")
    except Exception as e:
        # 记录错误日志
        print(f"数据插入失败：{data['url']}，错误原因：{e}")
        session.rollback()
    finally:
        session.close()