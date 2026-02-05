from db import db
from pymongo import MongoClient
# 链接mongoDB

coll = db['author']  # 获得collection的句柄
docs = coll.find().batch_size(300)
for each_doc in docs:
    if 'data' in each_doc:
        each_doc['data'].sort(key=lambda d: d['datetime'], reverse=True)
        coll.update_one({'mid': each_doc['mid']}, {'$set': each_doc})
        print('已修复av'+str(each_doc['mid']))
