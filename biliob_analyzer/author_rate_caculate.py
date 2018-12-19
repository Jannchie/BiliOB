from db import settings
from db import db
import datetime
coll = db['author']  # 获得collection的句柄
for each_author in coll.find().batch_size(4):
    rate = []
    i = 0
    # 数据量小于等于2条
    if ('data' not in each_author or len(each_author['data']) < (i + 2)):
        continue

    def next_c(i):
        return each_author['data'][i]['fans'], each_author['data'][i][
            'datetime'], each_author['data'][i][
                'datetime'] - datetime.timedelta(
                    hours=each_author['data'][i][
                'datetime'].hour, seconds=each_author['data'][i][
                'datetime'].second,microseconds=each_author['data'][i]['datetime'].microsecond,minutes=each_author['data'][i]['datetime'].minute)

    c_fans, c_datetime, c_date = next_c(i)

    def next_p(i):
        return each_author['data'][i + 1]['fans'], each_author['data'][
            i + 1]['datetime'], each_author['data'][
                i + 1]['datetime'] - datetime.timedelta(
                    hours=each_author['data'][i + 1]['datetime'].hour,
                    seconds=each_author['data'][i + 1]['datetime'].second,
                    microseconds=each_author['data'][i + 1]['datetime'].
                    microsecond,
                    minutes=each_author['data'][i + 1]['datetime'].minute)

    p_fans, p_datetime, p_date = next_p(i)


    # 相差粉丝数
    delta_fans = c_fans - p_fans
    # 相差日期数
    days = c_datetime.day - p_datetime.day
    # 相差秒数
    seconds = days + (c_datetime.second - p_datetime.second)

    while i < len(each_author['data']) - 2:
        # 是同一天
        if c_datetime.day == p_datetime.day:
            i += 1
            p_fans, p_datetime, p_date = next_p(i)
            continue

        # 相差一天
        if (c_date - p_date).days == 1:
            delta_fans = c_fans - p_fans
            seconds = days + (c_datetime.second - p_datetime.second)
            rate.append({'rate':int(delta_fans/(1 + seconds/(60*60*24))),'datetime':c_date})
            i += 1
            c_fans, c_datetime, c_date = next_c(i)
            p_fans, p_datetime, p_date = next_p(i)
            delta_fans = c_fans - p_fans
            seconds = days + (c_datetime.second - p_datetime.second)
            continue

        # 相差多天
        days = (c_date - p_date).days
        while days > 1:
            t_rate = delta_fans/(days + seconds/(60*60*24))
            t_date = c_date - datetime.timedelta(1)
            t_fans = c_fans - t_rate
            delta_fans = c_fans - t_fans
            rate.append({'rate':int(delta_fans/(1 + seconds/(60*60*24))),'datetime':c_date})
            c_fans = t_fans
            c_date = t_date
            days -= 1

    if len(rate) != 0:
        coll.update_one({
            'mid': each_author['mid']
        }, {'$set': {
            'fansRate': rate,
            'cRate': rate[0]['rate']
        }}, True)
        pass
