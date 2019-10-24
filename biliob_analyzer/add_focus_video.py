from db import db
from pymongo import MongoClient
# 链接mongoDB

coll = db['video']  # 获得collection的句柄
docs = coll.find({'focus': {'$exists': False}}).batch_size(60)
for each_doc in docs:
    if 'aid' in each_doc:
        each_doc['focus'] = True
        coll.update_one({'aid': each_doc['aid']}, {'$set': each_doc})
        print('已修复aid' + str(each_doc['aid']))
