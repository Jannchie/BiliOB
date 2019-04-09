import time
import datetime
from db import settings
from pymongo import MongoClient, DESCENDING
import requests

client = MongoClient(settings['MINGO_HOST'], 27017)
client.admin.authenticate(settings['MINGO_USER'],
                          settings['MONGO_PSW'])

db = client['biliob']  # 获得数据库的句柄
coll = db['author']  # 获得collection的句柄

URL = 'https://api.bilibili.com/x/web-interface/card?mid={}'
while True:
    docs = coll.find({}, {'mid': 1}).sort(
        'cFans', direction=DESCENDING).limit(2)
    mids = map(lambda x: x['mid'], docs)
    for mid in mids:
        try:
            j = requests.get(URL.format(mid)).json()
            pass
            fans = j['data']['card']['fans']
            if fans == 0:
                continue
            coll.update_one({'mid': mid}, {'$set': {'cFans': fans}})
        except Exception as e:
            pass
    time.sleep(5)
