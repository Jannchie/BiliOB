from db import db
from db import db
import datetime
coll = db['video']  # 获得collection的句柄
start_date = datetime.datetime(2018,11,22)
end_date = datetime.datetime(2018,12,22)
value = 'view'
d = {}
for each in coll.find():
    author_name = each['author']
    d[author_name] = []
    each['data'].reverse()
    s_value = None
    s_date = None
    for each_data in each['data']:
        if each_data['datetime'] < start_date:
            continue
        if each_data['datetime'] > end_date:
            continue
        if s_value == None:
            s_value = each_data[value]
            s_date = each_data['datetime']
            d[author_name] = [{'value':0,'date':s_date.date()}]
            continue
        c_value = each_data[value] - s_value
        c_date = each_data['datetime']
        d[author_name].append({'value':c_value,'date':c_date.date()})
        pass
    pass