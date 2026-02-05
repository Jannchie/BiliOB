from db import db
import time
import requests
from requests.adapters import HTTPAdapter

s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=3))
s.mount('https://', HTTPAdapter(max_retries=3))
author_coll = db['author']
URL = 'https://api.bilibili.com/x/space/acc/info?mid={mid}'

with open('D:/数据/B站/UP主硬币数-19-05-10.csv', 'w', encoding="utf-8-sig") as f:
    for each_author in author_coll.find({}, {'mid': 1, 'name': 1}):
        mid = each_author['mid']
        response = s.get(URL.format(mid=mid))
        j = response.json()

        if 'code' in j and j['code'] != -404 and 'data' in j and 'coins' in j[
                'data']:
            print('"{}","{}"\n'.format(each_author['name'],
                                       j['data']['coins']))
            f.write('"{}","{}"\n'.format(each_author['name'],
                                         j['data']['coins']))
        time.sleep(0.5)
