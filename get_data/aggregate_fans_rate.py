from db import db
import datetime
from scipy.interpolate import interp1d

start_date = datetime.datetime(2018, 11, 1)
end_date = datetime.datetime.now()
date_range = 7 * 24 * 60 * 60
delta_date = 0.25 * 24 * 60 * 60
date_format = '%Y-%m-%d %H:%M'
d = {}

current_date = start_date.timestamp()
while (current_date < end_date.timestamp()):
    c_date = datetime.datetime.fromtimestamp(
        current_date).strftime(date_format)
    d[c_date] = []
    current_date += delta_date


for each_author in db['author'].find({'cFans': {'$gt': 200000}}).batch_size(1):

    current_date = start_date.timestamp()

    data = sorted(each_author['data'], key=lambda x: x['datetime'])
    x = list(map(
        lambda each_data: each_data['datetime'].timestamp(), data))
    y = list(map(lambda each_data: each_data['fans'], data))

    if len(x) <= 2:
        continue
    interrupted_fans = interp1d(x, y, kind='linear')
    current_date = start_date.timestamp()

    while (current_date < min(end_date.timestamp(), x[-1])):
        # 出界
        if (current_date - date_range) > x[0] and current_date < x[-1]:
            fans_func = interrupted_fans(
                [current_date - date_range, current_date])
            delta_fans = int(fans_func[1] - fans_func[0])
            pass
            c_date = datetime.datetime.fromtimestamp(
                current_date).strftime(date_format)
            print('"{}","{}","{}"'.format(
                each_author['name'], delta_fans, c_date))
            # d[c_date].append((delta_fans, each_author['name']))

            d[c_date].append((each_author['name'], delta_fans))

            if len(d[c_date]) >= 200:
                d[c_date] = sorted(
                    d[c_date], key=lambda x: x[1], reverse=True)[:20]
        current_date += delta_date

d[c_date] = sorted(
    d[c_date], key=lambda x: x[1], reverse=True)[:20]

with open('D:/数据/B站/fans/190319.csv', 'w', encoding="utf-8-sig") as f:
    f.writelines('date,name,value\n')
    for each_date in d:
        for each_data in d[each_date]:
            f.writelines('"{}","{}","{}"\n'.format(
                each_date, each_data[0], each_data[1]))
