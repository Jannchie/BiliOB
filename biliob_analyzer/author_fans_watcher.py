from db import settings
from db import db
import datetime
from enum import Enum
coll = db['author']  # 获得collection的句柄
event = db['event']  # 获得collection的句柄
video = db['video']  # 获得collection的句柄

# 对于上升趋势的UP主，较上日增加多少倍，才算是巨量涨粉
WTF_INCREASE = 10

# 对于上升趋势的UP主，较上日增加多少倍，才算是超量涨粉
AMAZING_INCREASE = 5

# 对于上升趋势的UP主，较上日增加多少倍，才算是大量涨粉
MAGNIFICATION_INCREASE = 3

# 对于下降趋势的UP主，较上日减少多少倍，才算是大量掉粉
MAGNIFICATION_DECREASE = 2

# 对于上升趋势的UP主，较上日增加多少倍，才算是超量涨粉
AMAZING_DECREASE = 5

# 对于下降趋势的UP主，较上日减少多少倍，才算是巨量掉粉
WTF_DECREASE = 8
 
# 粉丝增加多少，才算大量涨粉
FANS_INCREASE_THRESHOLD = 8000
# 粉丝减少多少，算作大量掉粉
FANS_DECREASE_THRESHOLD = -3000
# 多少粉丝以上才关注掉粉
WATCH_DECREASE = 1000

class Event(Enum):
    increase_1 = 'I级增长'
    increase_2 = 'II级猛增'
    increase_3 = 'III级激增'
    sudden_fall = 'SF级骤减'
    decrease_1 = 'I级减少'
    decrease_2 = 'II级锐减'
    decrease_3 = 'III级暴减'

last_datetime = datetime.datetime(2000,1,1)
print('开始捕捉事件')
if event.count() != 0:
    last_datetime = next(event.find().sort([('datetime',-1)]).limit(1))['datetime']
    
for each_author in coll.find().batch_size(8):
    if 'fansRate' in each_author and len(each_author['fansRate']) > 1:
        index = 1

        def print_data(each_author):
            return '{name},速率：{rate},时间：{datetime}'.format(
                name=each_author['name'],
                rate=each_author['fansRate'][c_index]['rate'],
                datetime=each_author['fansRate'][c_index]['datetime'])

        def insert_event(event_type):
            videos = video.find({'mid':each_author['mid']})
            temp_video = {}
            cause = {'type':'video'}
            for each_v in videos:
                # 相差一日之内
                if (each_author['fansRate'][c_index]['datetime'] - each_v['datetime']).days <= 1:
                    temp_video['aid'] = each_v['aid']
                    temp_video['title'] = each_v['title']
                    temp_video['cView'] = each_v['data'][0]['view']
                    if 'cView' not in temp_video  or 'aid' not in cause or temp_video['cView'] > cause['cView']:
                        cause['aid'] = temp_video['aid']
                        cause['title'] = temp_video['title']
                        cause['cView'] = temp_video['cView']

            event.insert_one({
                'type':
                event_type.value,
                'mid':
                each_author['mid'],
                'author':
                each_author['name'],
                'rate':
                each_author['fansRate'][c_index]['rate'],
                'datetime':
                each_author['fansRate'][c_index]['datetime'],
                'cause': cause
            })

        while index < len(each_author['fansRate']):
            c_datetime = each_author['fansRate'][index]['datetime']
            if c_datetime <= last_datetime:
                break
            # 涨粉超高
            c_index = index - 1
            if each_author['fansRate'][c_index][
                    'rate'] > FANS_INCREASE_THRESHOLD and each_author[
                        'fansRate'][c_index]['rate'] > each_author['fansRate'][
                            index]['rate'] * WTF_INCREASE:
                insert_event(Event.increase_3)
                print(Event.increase_3.value + print_data(each_author))

            elif each_author['fansRate'][c_index][
                    'rate'] > FANS_INCREASE_THRESHOLD and each_author[
                        'fansRate'][c_index]['rate'] > each_author['fansRate'][
                            index]['rate'] * AMAZING_INCREASE:
                insert_event(Event.increase_2)
                print(Event.increase_2.value + print_data(each_author))

            elif each_author['fansRate'][c_index][
                    'rate'] > FANS_INCREASE_THRESHOLD and each_author[
                        'fansRate'][c_index]['rate'] > each_author['fansRate'][
                            index]['rate'] * MAGNIFICATION_INCREASE:
                insert_event(Event.increase_1)
                print(Event.increase_1.value + print_data(each_author))

            # 突然出现大量的掉粉
            if each_author['fansRate'][c_index][
                    'rate'] < FANS_DECREASE_THRESHOLD and each_author[
                        'fansRate'][index]['rate'] > WATCH_DECREASE:
                insert_event(Event.sudden_fall)
                print(Event.sudden_fall.value + print_data(each_author))
            # 一掉再掉
            elif each_author['fansRate'][c_index][
                    'rate'] < FANS_DECREASE_THRESHOLD and abs(
                        each_author['fansRate'][c_index]['rate']) > abs(
                            each_author['fansRate'][index]
                            ['rate']) * WTF_DECREASE:
                insert_event(Event.decrease_3)
                print(Event.decrease_3.value + print_data(each_author))
            # 一掉再掉
            elif each_author['fansRate'][c_index][
                    'rate'] < FANS_DECREASE_THRESHOLD and abs(
                        each_author['fansRate'][c_index]['rate']) > abs(
                            each_author['fansRate'][index]
                            ['rate']) * AMAZING_DECREASE:
                insert_event(Event.decrease_2)
                print(Event.decrease_2.value + print_data(each_author))
            # 一掉再掉
            elif each_author['fansRate'][c_index][
                    'rate'] < FANS_DECREASE_THRESHOLD and abs(
                        each_author['fansRate'][c_index]['rate']) > abs(
                            each_author['fansRate'][index]
                            ['rate']) * MAGNIFICATION_DECREASE:
                insert_event(Event.decrease_1)
                print(Event.decrease_1.value + print_data(each_author))

            index += 1
