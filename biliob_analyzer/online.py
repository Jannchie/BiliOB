from db import settings
from pymongo import MongoClient
import datetime
client = MongoClient(settings['MINGO_HOST'], 27017)
# 数据库登录需要帐号密码
client.admin.authenticate(settings['MINGO_USER'],
                          settings['MONGO_PSW'])
db = client['biliob']  # 获得数据库的句柄
video_online = db['video_online']
pass
d = video_online.aggregate([
    {
        '$match':
        {
            'data.datetime':
            {
                '$gt': datetime.datetime(2019, 2, 21)
            }
        }
    },
    {
        '$limit': 1
    },
    {
        '$project':
        {
            "title": 1,
            "author": 1,
            "data":
            {
                '$filter':
                {
                    'input': "$data",
                    'as': "item",
                    'cond':
                    {
                        '$gt': ["$$item.datetime", datetime.datetime(2019, 2, 22)]
                    }
                }
            }
        }
    }
])
print(len(next(d)['data']))
pass
