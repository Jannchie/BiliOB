from scipy.interpolate import interp1d
import logging
import datetime
from db import db
# from biliob_tracer.task import ProgressTask


def remove_error_data():

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

    # task = ProgressTask("计算粉丝增速", coll.count_documents({}),
    #                     collection=db['tracer'])

    c = 0
    for each in coll.find({}, {'mid': 1, '_id': 0}).batch_size(200):
        c += 1
        # task.current_value = c
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
                                "$eq": ["$$each_data.fans", 0]
                            }
                        }
                    }
                }
            }
        ]).batch_size(1)
        each_author = next(ag)

        if each_author['data'] != None and len(each_author['data']) != 0:
            coll.update(
                {'mid': each_author['mid']},
                {'$pull': {"data": {'fans': 0}}}
            )
            logger.info(each_author['mid'])
            pass


remove_error_data()
