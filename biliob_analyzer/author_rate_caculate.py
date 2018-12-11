from db import settings
from db import db
import datetime
coll = db['author']  # 获得collection的句柄
for each_author in coll.find({'$or':[{'focus':True},{'forceFocus':True}]}):
    rate = []
    i = 0
    if('data' not in each_author or len(each_author['data'])<(i+2)):
        continue
    c_fans = each_author['data'][i]['fans']
    c_date = each_author['data'][i]['datetime']
    p_fans = each_author['data'][i+1]['fans']
    p_date = each_author['data'][i+1]['datetime']
    while i < len(each_author['data']):
        delta_seconds = (c_date-p_date).seconds
        delta_day = (c_date-p_date).days
        while (delta_day < 1) and i < len(each_author['data'])-2:
            i = i + 1 
            p_fans = each_author['data'][i+1]['fans']
            p_date = each_author['data'][i+1]['datetime']
            delta_day = (c_date-p_date).days
            delta_seconds = (c_date-p_date).seconds
        if(i >= len(each_author['data'])-2) and len(rate) != 0:
            coll.update_one({
                'mid': each_author['mid']
            }, {
                '$set': {
                    'fansRate': rate,
                    'cRate': rate[0]['rate']
                }
            }, True)
            break
        delta_fans = c_fans-p_fans
        day = delta_day+delta_seconds/(60*24*60)
        rate.append({'rate':int(delta_fans/day),'datetime':c_date})
        c_fans = p_fans
        c_date = p_date
    pass
