from db import db
from pymongo import MongoClient
# 链接mongoDB

coll = db['author']  # 获得collection的句柄
docs = coll.find({'focus': {'$exists': False}}).batch_size(60)
for each_doc in docs:
    if 'mid' in each_doc:
        each_doc['focus'] = True
        coll.update_one({'mid': each_doc['mid']}, {'$set': each_doc})
        print('已修复mid' + str(each_doc['mid']))
