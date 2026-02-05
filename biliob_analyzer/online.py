from db import db
from pymongo import MongoClient
import datetime
db = db
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
