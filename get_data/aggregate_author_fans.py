from db import db
import datetime
from scipy.interpolate import interp1d
from haishoku.haishoku import Haishoku
from time import sleep
from face import face
from color import color

date_range = 3000 * 24 * 60 * 60
delta_date = 0.01 * 24 * 60 * 60
date_format = '%Y-%m-%d %H:%M'
# output_file = 'D:/DataSource/B站/月结粉絲减少-2019-8-8.csv'
# output_file = 'D:/DataSource/B站/总计粉丝排行-2019-8-12.csv'
# field = 'cArchive_view'
# field_name = 'archiveView'
field = 'cFans'
field_name = 'fans'


def get_mid_list():
    mid_list = []
    for each_author in db['author'].find({field: {'$gt': 100000}}, {'mid': 1}).batch_size(200):
        mid_list.append(each_author['mid'])
    return mid_list


start_date = datetime.datetime(2019, 8, 16)


def get_data(mid):
    data = db['author'].aggregate([{"$match": {'mid': mid}}, {
        "$project": {
            "name": 1,
            "mid": 1,
            "face": 1,
            "data": {
                "$filter": {
                    "input": "$data",
                    "as": "eachData",
                    "cond": {
                        "$gt": ["$$eachData.datetime", start_date]
                    }
                }
            }
        }
    }
    ])
    each_author = next(data)
    data = sorted(each_author['data'], key=lambda x: x['datetime'])
    return each_author['name'], each_author['face'], data


end_date = datetime.datetime(2019, 8, 25)
mid_list = get_mid_list()


def int_data(data):
    x = []
    y = []
    for each_data in data:
        if 'datetime' in each_data and 'fans' in each_data and 'fans' != 0:
            x.append(each_data['datetime'].timestamp())
            y.append(int(each_data['fans']))
    if len(x) <= 5:
        return None
    return interp1d(x, y, kind='linear', fill_value='extrapolate')


with open('./origin_data.csv', 'w', encoding="utf-8-sig") as f:
    f.write('"name","date","value"\n')
    for each_mid in mid_list:
        each_name, each_face, each_data = get_data(each_mid)
        int_func = int_data(each_data)
        if int_func == None:
            continue
        c_time = start_date.timestamp()
        while c_time < end_date.timestamp():
            f.write('"{}","{}","{}"\n'.format(
                each_name, datetime.datetime.fromtimestamp(c_time), int_func(c_time)))
            c_time += delta_date
        pass
    pass
