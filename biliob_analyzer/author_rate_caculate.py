from scipy.interpolate import interp1d
import logging
import datetime
from db import db


def author_fans_rate_caculate():

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
    logger = logging.getLogger(__name__)

    coll = db['author']  # 获得collection的句柄
    logger.info('开始计算粉丝增速')

    c_datetime = datetime.datetime.now()

    end_date = (datetime.datetime(
        c_datetime.year, c_datetime.month, c_datetime.day) - datetime.timedelta(1)).timestamp()
    start_date = (datetime.datetime(
        c_datetime.year, c_datetime.month, c_datetime.day) - datetime.timedelta(2)).timestamp()

    c = 0
    for each in coll.find({}, {'mid': 1, '_id': 0}).batch_size(200):
        c += 1
        ag = coll.aggregate([
            {
                '$match': {
                    'mid': each['mid']
                }
            },
            {
                '$project': {
                    'mid': 1,
                    'data': {
                        "$filter": {
                            "input": "$data",
                            "as": "each_data",
                            "cond": {
                                "$gt": ["$$each_data.datetime", datetime.datetime.now() - datetime.timedelta(7)]
                            }
                        }
                    }
                }
            }
        ]).batch_size(1)
        each_author = next(ag)
        if 'data' in each_author and each_author['data'] != None:
            data = sorted(each_author['data'], key=lambda x: x['datetime'])
            if len(data) >= 2:
                logger.info(each_author['mid'])
                x = tuple(map(lambda x: x['datetime'].timestamp(), data))
                y = tuple(map(lambda x: x['fans'], data))
                inter_fun = interp1d(x, y, kind='linear')
                if start_date > x[0] and end_date < x[-1]:
                    inter_data = inter_fun([start_date, end_date])
                    delta_fans = inter_data[1] - inter_data[0]
                    coll.update_one({'mid': each_author['mid']}, {
                        "$set": {
                            'cRate': int(delta_fans)
                        }
                    })


if __name__ == "__main__":
    author_fans_rate_caculate()
