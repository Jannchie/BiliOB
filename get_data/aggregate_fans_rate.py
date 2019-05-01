from db import db
import datetime
from scipy.interpolate import interp1d
from haishoku.haishoku import Haishoku

from face import face
from color import color

start_date = datetime.datetime(2018, 12, 1)
end_date = datetime.datetime.now()
date_range = 30 * 24 * 60 * 60
delta_date = 0.25 * 24 * 60 * 60
date_format = '%Y-%m-%d %H:%M'
d = {}
output_file = 'D:/数据/B站/fans/月结粉丝.csv'

current_date = start_date.timestamp()
while (current_date < end_date.timestamp()):
    c_date = datetime.datetime.fromtimestamp(current_date).strftime(
        date_format)
    d[c_date] = []
    current_date += delta_date

for each_author in db['author'].find({'cFans': {'$gt': 200000}}).batch_size(5):

    current_date = start_date.timestamp()

    data = sorted(each_author['data'], key=lambda x: x['datetime'])
    x = list(map(lambda each_data: each_data['datetime'].timestamp(), data))
    y = list(map(lambda each_data: each_data['fans'], data))

    if len(x) <= 2:
        continue
    interrupted_fans = interp1d(x, y, kind='linear')
    current_date = start_date.timestamp()
    while (current_date < min(end_date.timestamp(), x[-1])):
        begin_date = current_date - date_range
        if begin_date <= x[0]:
            begin_date = x[0]
        # 出界
        if begin_date >= x[0] and current_date < x[-1] and current_date > x[0]:
            fans_func = interrupted_fans([begin_date, current_date])
            delta_fans = int(fans_func[1] - fans_func[0])
            pass
            c_date = datetime.datetime.fromtimestamp(current_date).strftime(
                date_format)
            print('"{}","{}","{}"'.format(each_author['name'], delta_fans,
                                          c_date))
            # d[c_date].append((delta_fans, each_author['name']))

            d[c_date].append((each_author['name'], delta_fans,
                              each_author['face']))

            if len(d[c_date]) >= 200:
                d[c_date] = sorted(
                    d[c_date], key=lambda x: x[1], reverse=True)[:20]
        current_date += delta_date
for c_date in d:
    d[c_date] = sorted(d[c_date], key=lambda x: x[1], reverse=True)[:20]

with open(output_file, 'w', encoding="utf-8-sig") as f:
    f.writelines('date,name,value\n')
    for each_date in d:
        for each_data in d[each_date]:
            f.writelines('"{}","{}","{}"\n'.format(each_date, each_data[0],
                                                   each_data[1]))
authors = []
for each_date in d:
    for each_author in d[each_date]:
        authors.append(each_author[0])
        if each_author[0] not in face:
            face[each_author[0]] = each_author[2]
with open('./get_data/face.py', 'w', encoding="utf-8-sig") as f:
    f.writelines('face = ' + str(face))

for each_author in face:
    if each_author in color:
        continue
    if face[each_author][-3:] == 'gif' or each_author == '开眼视频App':
        color[each_author] = '#000000'
    else:
        color_list = Haishoku.getPalette(face[each_author])
        color_list = sorted(
            color_list, key=lambda x: x[1][0] + x[1][1] + x[1][2])
        color[each_author] = 'rgb' + \
            str(color_list[int(len(color_list)/2)][1])

with open('./get_data/color.py', 'w', encoding="utf-8-sig") as f:
    f.writelines('color = ' + str(color))

min_fans = 99999999
for each_author in authors:
    c_fans = db['author'].find_one({'name': each_author},
                                   {'cFans': True})['cFans']
    if c_fans <= min_fans:
        min_fans = c_fans
print(min_fans)
