from db import settings
from db import db
import datetime
from enum import Enum
from scipy.interpolate import interp1d

from biliob_tracer.task import ProgressTask

author_coll = db['author']  # 获得collection的句柄
video_coll = db['video']  # 获得collection的句柄
fans_variation_coll = db['fans_variation']  # 获得collection的句柄


def dateRange(beginDate, endDate):
    dates = []
    date = beginDate
    while date <= endDate:
        dates.append(date.date())
        date += datetime.timedelta(1)
    return dates


class FansWatcher(object):
    def __insert_event(self, delta_rate, d_daily, author, info, date):
        print('变化率：{}% \n单日涨幅：{} \nUP主：{} \n信息：{}\n日期：{}\n\n'.format(
            delta_rate, d_daily, author['name'], info, date))
        out_data = {
            'variation': int(d_daily),
            'mid': author['mid'],
            'author': author['name'],
            'face': author['face'],
            'deltaRate': delta_rate,
            'datetime': date.strftime("%Y-%m-%d"),
            'info': info,
        }

        videos = video_coll.find({'mid': author['mid']})
        temp_video = {}
        cause = {'type': 'video'}
        for each_v in videos:
            # 相差一日之内
            if (date - each_v['datetime']).days >= -1 and (date - each_v['datetime']).days <= 7:
                temp_video['aid'] = each_v['aid']
                temp_video['title'] = each_v['title']
                temp_video['pic'] = each_v['pic']
                temp_video['cView'] = each_v['data'][0]['view']
                temp_video['channel'] = each_v['channel']
                temp_video['subChannel'] = each_v['subChannel']
                if 'cView' not in temp_video or 'aid' not in cause or temp_video[
                        'cView'] > cause['cView']:
                    cause['aid'] = temp_video['aid']
                    cause['title'] = temp_video['title']
                    cause['pic'] = temp_video['pic']
                    cause['cView'] = temp_video['cView']
                    cause['channel'] = temp_video['channel']
                    cause['subChannel'] = temp_video['subChannel']

        if cause != {'type': 'video'}:
            out_data['cause'] = cause
        fans_variation_coll.replace_one(
            {'mid': out_data['mid'], 'datetime': out_data['datetime']}, out_data, upsert=True)

    def __judge(self, author):
        '''
            一共有这样几种可能：
                1、 大量涨粉        日涨粉数超过上周平均的25倍
                2、 史诗级涨粉      日涨粉数超过上周平均的50倍或单日涨粉超过10W
                3、 传说级涨粉      日涨粉数超过上周平均的100倍或单日涨粉超过20W
                4、 急转直下        上升轨道中的UP主突然掉粉
                5、 大量掉粉        每日掉粉数突破5K
                6、 雪崩级掉粉      每日掉粉数突破2W
                7、 末日级掉粉      每日掉粉数突破5W
                8、 新星爆发         日涨粉超过粉丝总数的20%
        '''

        data = sorted(author['data'], key=lambda x: x['datetime'])
        start_date = data[0]['datetime'].timestamp()
        end_date = data[-1]['datetime'].timestamp()
        x = []
        y = []
        for each in data:
            x.append(each['datetime'].timestamp())
            y.append(each['fans'])
        if len(x) <= 1:
            return
        # 线性插值
        interrupted_fans = interp1d(x, y, kind='linear')
        temp_date = datetime.datetime.fromtimestamp(start_date)
        c_date = datetime.datetime(
            temp_date.year, temp_date.month, temp_date.day).timestamp() + 86400 * 3
        if c_date - 86400 * 2 <= start_date:
            return
        while (c_date <= end_date):
            date = datetime.datetime.fromtimestamp(c_date)
            daily_array = interrupted_fans([c_date - 86400, c_date])
            p_daily_array = interrupted_fans(
                [c_date - 86400 * 2, c_date - 86400])

            # 24小时前涨粉数
            pd_daily = p_daily_array[1] - p_daily_array[0]

            # 每日涨粉数
            d_daily = daily_array[1] - daily_array[0]

            if (d_daily >= 5000 or d_daily <= -2000):

                delta_rate = round(d_daily / pd_daily * 100, 2)
                if (d_daily >= daily_array[1] * 0.50):
                    self.__insert_event(round(d_daily/daily_array[1]*100, 2), d_daily,
                                        author, '新星爆发', date)

                if (d_daily <= 0 and pd_daily >= 0):
                    self.__insert_event('-', d_daily,
                                        author, '急转直下', date)
                    c_date += 86400
                    continue

                if (d_daily <= -50000):
                    # 每日掉粉数突破5K
                    self.__insert_event(delta_rate, d_daily,
                                        author, '末日级掉粉', date)
                    pass
                elif (d_daily <= -20000):
                    # 每日掉粉数突破2W
                    self.__insert_event(delta_rate, d_daily,
                                        author, '雪崩级掉粉', date)
                    pass
                elif (d_daily <= -5000):
                    # 每日掉粉数突破5W
                    self.__insert_event(delta_rate, d_daily,
                                        author, '大量掉粉', date)
                    pass

                if (c_date >= start_date + 86400 * 8 and d_daily > 0):
                    weekly_array = interrupted_fans([
                        c_date - 86400 * 8, c_date - 86400])
                    # 上月平均涨粉数
                    weekly_mean = (weekly_array[1] - weekly_array[0]) / 7
                    # 上周平均涨粉数
                    delta_rate = round(d_daily / weekly_mean * 100, 2)
                    if delta_rate >= 10000 or d_daily >= 200000:
                        # 日涨粉数超过上日的100倍
                        self.__insert_event(delta_rate, d_daily,
                                            author, '传说级涨粉', date)
                        pass
                    elif delta_rate >= 5000 or d_daily >= 100000:
                        # 日涨粉数超过上日的50倍
                        self.__insert_event(delta_rate, d_daily,
                                            author, '史诗级涨粉', date)
                        pass
                    elif delta_rate >= 2500:
                        # 日涨粉数超过上日的25倍
                        self.__insert_event(delta_rate, d_daily,
                                            author, '大量涨粉', date)
                        pass

            c_date += 86400
            pass

    def watchAllAuthor(self):
        author_filter = {'data': {'$exists': True}}
        self.__judge_author(author_filter)

    def watchBigAuthor(self):
        author_filter = {'data': {'$exists': True}, 'cFans': {'$gt': 10000}}
        self.__judge_author(author_filter)

    def __judge_author(self, author_filter):
        author_cursor = author_coll.find(author_filter)
        count = author_cursor.count()
        a = author_coll.aggregate([
            {
                '$match': author_filter
            }, {
                '$project': {
                    "mid": 1,
                    "face": 1,
                    "name": 1,
                    "data": {
                        "$filter": {
                            "input":
                            "$data",
                            "as": "data",
                            "cond": {"$gt": ["$$data.datetime", datetime.datetime.now()-datetime.timedelta(32)]}
                        }
                    }
                }
            }, {
                "$match": {
                    "data.0": {
                        "$exists": True
                    }
                }
            }
        ])
        print("待爬取作者数量：{}".format(count))
        t = ProgressTask("粉丝数变动探测", total_value=count, collection=db['tracer'])
        for each_author in a:
            print(each_author['mid'])
            t.current_value += 1
            self.__judge(each_author)
        t.finished = True
        pass
