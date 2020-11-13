from pymongo import MongoClient

settings = {
    'MINGO_USER': 'jannchie',
    'MONGO_PSW': '141421',
    'MINGO_HOST': '127.0.0.1'
}
# 链接mongoDB
client = MongoClient(settings['MINGO_HOST'], 27017)
# 数据库登录需要帐号密码
client.admin.authenticate(settings['MINGO_USER'], settings['MONGO_PSW'])
db = client['biliob']  # 获得数据库的句柄

