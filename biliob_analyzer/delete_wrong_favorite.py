from db import db
from db import db
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