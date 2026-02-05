from db import db
import datetime
from scipy.interpolate import interp1d
from haishoku.haishoku import Haishoku
from time import sleep
from face import face
from color import color

start_date = datetime.datetime(2018, 12, 1)
end_date = datetime.datetime.now()
date_range = 30 * 24 * 60 * 60
delta_date = 0.25 * 24 * 60 * 60
date_format = '%Y-%m-%d %H:%M'
d = {}
# output_file = 'D:/DataSource/B站/月结粉絲减少-2019-8-8.csv'

# field = 'cArchive_view'
# field_name = 'archiveView'
field = 'cFans'
field_name = 'fans'
current_date = start_date.timestamp()
while (current_date < end_date.timestamp()):
    c_date = datetime.datetime.fromtimestamp(current_date).strftime(
        date_format)
    d[c_date] = []
    current_date += delta_date


def get_max(interrupted_fans, begin_date, current_date):
    max_value = 0
    while begin_date < current_date:
        c_value = interrupted_fans([begin_date])[0]
        if c_value > max_value:
            max_value = c_value
        begin_date += delta_date
    return max_value


def add_data(mid):
    each_author = db['author'].find_one({'mid': mid})
    if each_author['name'] == "吴织亚切大忽悠":
        each_author['name'] = "*******"
    if each_author['name'] == '大忽悠录屏组':
        each_author['name'] = '***录屏组'
    current_date = start_date.timestamp()
    fix = {}
    data = sorted(each_author['data'], key=lambda x: x['datetime'])

    def get_date(each_data):
        if field_name in each_data and each_data[field_name] != 0 and each_data[field_name] != -1:
            return each_data['datetime'].timestamp()

    def get_value(each_data):
        if field_name in each_data and each_data[field_name] != 0 and each_data[field_name] != -1:
            return each_data[field_name]
    px = list(i for i in map(get_date, data) if i != None)
    py = list(i for i in map(get_value, data) if i != None)
    x = []
    y = []
    for i in range(len(px)):
        if i != 0 and py[i] == py[i - 1]:
            continue
        else:
            x.append(px[i])
            y.append(py[i])
    for index in range(len(x)):
        if index == 0:
            continue
        dy = y[index] - y[index - 1]
        if dy <= -6000 and index < (len(x) - 1):
            pi = index - 1
            ppi = index - 1
            ni = index + 1
            nni = index + 1
            while ppi >= 0 and x[pi] - x[ppi] <= 86400:
                ppi -= 1
            while nni < len(x) - 1 and x[nni] - x[ni] <= 86400:
                nni += 1
            # rp = (y[pi] - y[ppi]) / (x[pi] - x[ppi]) * 86400
            # rn = (y[ni] - y[nni]) / (x[ni] - x[nni]) * 86400
            if ((y[pi] - y[ppi] > -1000) and (y[nni] - y[ni]) > -1000) or((y[ni] - y[index]) > 0 and (y[pi] - y[pi - 1]) > 0):
                print(each_author['name'], '检测到异常掉粉')
                fix[index] = dy
            pass

    if len(x) <= 2:
        return None
    for index in fix:
        idx = index
        while idx < len(y):
            y[idx] -= fix[index]
            idx += 1
    interrupted_fans = interp1d(x, y, kind='linear')
    current_date = start_date.timestamp()
    begin_date = current_date - date_range
    # if begin_date <= x[0]:
    #     begin_date = x[0]
    # 出界
    data_range = []
    while begin_date < x[-1]:
        if begin_date > x[0]:
            data_range.append(begin_date)
        begin_date += delta_date
    fans_func = interrupted_fans(data_range)
    for index in range(int(date_range / delta_date), len(data_range)):
        if data_range[index] < start_date.timestamp():
            continue
        delta_fans = int(
            fans_func[index] - fans_func[index - int(date_range / delta_date):index].max())
        c_date = datetime.datetime.fromtimestamp(data_range[index]).strftime(
            date_format)
        # print('{}\t{}\t{}'.format(each_author['name'], delta_fans,
        #                           c_date))
        # d[c_date].append((delta_fans", each_author['name']))

        d[c_date].append((each_author['name'], delta_fans,
                          each_author['face']))

        if len(d[c_date]) >= 200:
            d[c_date] = sorted(
                d[c_date], key=lambda x: x[1], reverse=False)[:20]
    current_date += delta_date


def get_mid_list():
    mid_list = []
    for each_author in db['author'].find({field: {'$gt': 50000}}, {'mid': 1}).batch_size(200):
        mid_list.append(each_author['mid'])
    return mid_list


output_file = 'D:/DataSource/B站/月结粉絲减少-812-2.csv'
for each_mid in get_mid_list():
    # print(each_mid)
    try:
        add_data(each_mid)
    except (Exception, BaseException):
        add_data(each_mid)
for c_date in d:
    d[c_date] = sorted(d[c_date], key=lambda x: x[1], reverse=False)[:20]

with open(output_file, 'w', encoding="utf-8-sig") as f:
    f.writelines('"date","name","value"\n')
    for each_date in d:
        for each_data in d[each_date]:
            f.writelines('"{}","{}","{}"\n'.format(each_date, each_data[0],
                                                   each_data[1]))
authors = set()
for each_date in d:
    for each_author in d[each_date]:
        authors.add(each_author[0])
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
    if each_author == "*******":
        each_author = "吴织亚切大忽悠"
    if each_author == '***录屏组':
        each_author = '大忽悠录屏组'
    c_fans = db['author'].find_one({'name': each_author},
                                   {field: True})[field]
    if c_fans <= min_fans:
        min_fans = c_fans
print(min_fans)
