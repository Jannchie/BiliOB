from db import settings
from db import db
import datetime
coll = db['author']  # 获得collection的句柄

MAGNIFICATION_INCREASE = 5
MAGNIFICATION_DECREASE = 2
FANS_INCREASE_THRESHOLD = 10000
FANS_DECREASE_THRESHOLD = -3000
for each_author in coll.find():
    if 'fansRate' in each_author and len(each_author['fansRate']) > 1:
        index = 1
        while index < len(each_author['fansRate']):
            # 涨粉超高
            c_index = index - 1
            if each_author['fansRate'][c_index][
                    'rate'] > FANS_INCREASE_THRESHOLD and each_author[
                        'fansRate'][c_index]['rate'] > each_author['fansRate'][
                            index]['rate'] * MAGNIFICATION_INCREASE:
                print('检测到大量涨粉：{name},速率：{rate},时间：{datetime}'.format(
                    name=each_author['name'],
                    rate=each_author['fansRate'][c_index]['rate'],
                    datetime=each_author['fansRate'][c_index]['datetime']))

            if each_author['fansRate'][c_index]['rate'] < FANS_DECREASE_THRESHOLD and each_author['fansRate'][index]['rate'] > 1000:
                print('检测到突然掉粉：{name},速率：{rate},时间：{datetime}'.format(
                    name=each_author['name'],
                    rate=each_author['fansRate'][c_index]['rate'],
                    datetime=each_author['fansRate'][c_index]['datetime']))

            elif each_author['fansRate'][c_index][
                    'rate'] < FANS_DECREASE_THRESHOLD and abs(
                        each_author['fansRate'][c_index]['rate']) > abs(
                            each_author['fansRate'][index]['rate']) * MAGNIFICATION_DECREASE:
                print('检测到大量掉粉：{name},速率：{rate},时间：{datetime}'.format(
                    name=each_author['name'],
                    rate=each_author['fansRate'][c_index]['rate'],
                    datetime=each_author['fansRate'][c_index]['datetime']))
            index += 1
