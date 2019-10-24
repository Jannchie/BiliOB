from pymongo import MongoClient
import pymysql
import redis
import os
import datetime
env_dist = os.environ
try:
    client = MongoClient(env_dist.get("BILIOB_MONGO_URL"))
    db = client['biliob']  # 获得数据库的句柄
except Exception:
    print('{} MongoDB链接失败'.format(datetime.datetime.now()))
    os.system('systemctl restart mongod')

redis_connect_string = env_dist.get("BILIOB_REDIS_CONNECTION_STRING")

pool = redis.ConnectionPool(redis_connect_string)  # 实现一个连接池
redis_connection = redis.Redis(connection_pool=pool)
