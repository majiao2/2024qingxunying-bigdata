from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from config import MYSQL_CONFIG

# 创建数据库引擎
engine = create_engine(f'mysql+pymysql://{MYSQL_CONFIG["user"]}:{MYSQL_CONFIG["password"]}@{MYSQL_CONFIG["host"]}/{MYSQL_CONFIG["database"]}')

# 创建基类
Base = declarative_base()

# 定义数据表模型
class CrawledData(Base):
    __tablename__ = 'crawled_data'
    URL = Column(String, primary_key=True)
    标题 = Column(String)
    内容 = Column(String)
    元数据 = Column(String)

# 创建会话
Session = sessionmaker(bind=engine)

def store_data_to_mysql(data):
    session = Session()
    try:
        for item in data:
            record = CrawledData(
                URL=item['URL'],
                标题=item['标题'],
                内容=item['内容'],
                元数据=item['元数据']
            )
            session.merge(record)  # 使用 merge 避免重复插入主键冲突
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()