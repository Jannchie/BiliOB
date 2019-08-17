import time
import datetime
from db import settings
from pymongo import MongoClient, DESCENDING
import requests

client = MongoClient(settings['MINGO_HOST'], 27017)
client.admin.authenticate(settings['MINGO_USER'],
                          settings['MONGO_PSW'])

db = client['biliob']  # 获得数据库的句柄
author = db['author']  # 获得collection的句柄
realtime_fans = db['realtime_fans']

URL = 'https://api.bilibili.com/x/web-interface/card?mid={}'
while True:
    docs = author.find({}, {'mid': 1}).sort(
        'cFans', direction=DESCENDING).limit(4)
    mids = map(lambda x: x['mid'], docs)
    date = datetime.datetime.now()
    for mid in mids:
        try:
            j = requests.get(URL.format(mid)).json()
            pass
            fans = j['data']['card']['fans']
            if fans == 0:
                continue
            author.update_one({'mid': mid}, {'$set': {'cFans': fans}})
            realtime_fans.insert_one(
                {'mid': mid, 'fans': fans, 'datetime': date})
        except Exception as e:
            print(e)
            pass
    time.sleep(5)
