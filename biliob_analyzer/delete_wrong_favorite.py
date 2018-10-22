from db import settings
from pymongo import MongoClient
# 链接mongoDB
client = MongoClient(settings['MINGO_HOST'], 27017)
# 数据库登录需要帐号密码
client.admin.authenticate(settings['MINGO_USER'], settings['MONGO_PSW'])
db = client['biliob']  # 获得数据库的句柄
coll = db['video']  # 获得collection的句柄
docs = coll.find().batch_size(60)
for each_doc in docs:
    if 'data' in each_doc:
        for each_data in each_doc['data']:
            if 'favorite' not in each_data:
                break
            if each_data['favorite'] == each_data['danmaku']:
                each_data.pop(' favorite')
            coll.update_one({'aid': each_doc['aid']},{'$set':each_doc})
        print('已修复av'+str(each_doc['aid']))