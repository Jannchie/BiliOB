import redis
from db import redis_connect_string
from db import db
from bson import ObjectId
connection = redis.from_url(redis_connect_string)
coll =  db['user_record']
key = 'DanmakuAggregate:start_urls'
oid = str(ObjectId)
url = 'https://api.bilibili.com/x/web-interface/view?aid={}&{}'
coll.update_many({'isExecuted': False},{'$set':{'isExecuted': True}})
# for each in [
#     46657799,
#     43171839,
#     43171839,
#     49435217,
#     49435217,
#     49586449,
#     46517117,
#     34711118,
#     28182280,
#     36640321,
#     777536,
#     30667425,
#     30667425,
#     39321206,
#     47759529,
#     4298666,
#     4298666,
#     4298666,
#     4298666,
#     4298666,
#     4298666,
#     4298666,
#     810872,
#     810872,
#     49784861,
#     47759529,
#     4033926,
# ]:
#     av = each
#     print(av)
#     connection.lpush(key,url.format(av,oid))
