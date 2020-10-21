from db import db
import datetime
from scipy.interpolate import interp1d
from haishoku.haishoku import Haishoku

from face import face
from color import color
from time import sleep
start_date = datetime.datetime(2019, 1, 7)
end_date = datetime.datetime.now()
date_range = 30 * 24 * 60 * 60
delta_date = 7 * 24 * 60 * 60
field_name = 'archiveView'
date_format = '%Y-%m-%d %H:%M'
current_date = start_date.timestamp()
d = {}
while (current_date < end_date.timestamp()):
    c_date = datetime.datetime.fromtimestamp(current_date).strftime(
        date_format)
    d[c_date] = []
    current_date += delta_date

author_coll = db['author']

while (current_date < end_date.timestamp()):
    c_date = datetime.datetime.fromtimestamp(current_date).strftime(
        date_format)
    d[c_date] = []
    current_date += delta_date
with open('D:/DataSource/B站/最高播放.csv', 'w', encoding='utf-8-sig') as out_file:
    # for each_author in author_coll.find({'aid': 12719263}):
    for each_author in author_coll.find({'cArchive_view': {'$gt': 100000000}}):
        sleep(10)
        name = each_author['name']
        data = each_author['data']
        current_date = start_date.timestamp()
        while None in data:
            data.remove(None)
        data = sorted(data, key=lambda x: x['datetime'])

        def get_date(each_data):
            if field_name in each_data and each_data[field_name] != 0 and each_data[field_name] != -1 and each_data[field_name] != None:
                return each_data['datetime'].timestamp()

        def get_value(each_data):
            if field_name in each_data and each_data[field_name] != 0 and each_data[field_name] != -1 and each_data[field_name] != None:
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
            pass
        if x[0] > start_date.timestamp():
            avg_rate = (y[-1] - y[0]) / (x[-1] - x[0])
            dx = x[0] - start_date.timestamp()
            y0 = y[0] - dx * avg_rate
            x.insert(0, start_date.timestamp())
            y.insert(0, y0)
        if len(x) <= 2:
            continue
        interrupted_fans = interp1d(x, y, kind='linear')
        current_date = start_date.timestamp()
        while (current_date < min(end_date.timestamp(), x[-1])):
            if current_date > int(x[0]):
                out_file.write('"{}","{}","{}"\n'.format(
                    name, datetime.datetime.fromtimestamp(current_date), interrupted_fans(current_date)))
                print('"{}"\t"{}"\t"{}"'.format(
                    name, datetime.datetime.fromtimestamp(current_date), interrupted_fans(current_date)))
            current_date += delta_date
        pass
