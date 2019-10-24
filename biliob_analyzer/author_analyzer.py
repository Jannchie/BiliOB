from db import db
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
import logging

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
logger = logging.getLogger(__name__)


class AuthorAnalyzer(object):
    def __init__(self):
        self.db = db  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄

    def author_filter(self):
        pre_fans = -1
        c_fans = -1
        delta = timedelta(1)
        pre_date = datetime
        c_date = datetime
        count_unfocus = 0
        count_focus = 0
        for each_doc in self.coll.find({'focus': True, 'cFans': {'lt': 50000}}):
            flag_cool = 0
            if 'data' in each_doc:
                each_doc['data'].reverse()
                for each_data in each_doc['data']:
                    if pre_fans == -1:
                        pre_fans = each_data['fans']
                        pre_date = each_data['datetime']
                        continue
                    c_fans = each_data['fans']
                    c_date = each_data['datetime']
                    if pre_date + delta > c_date:
                        continue
                    if abs(c_fans-pre_fans) < 100:
                        flag_cool += 1
                    else:
                        flag_cool = 0
                    pre_fans = c_fans
                    pre_date = c_date

                    # 连续30日日均涨粉小于100且粉丝数小于100000则不追踪
                    if flag_cool > 30 and each_data['fans'] < 100000:
                        focus = False
                        break
                    elif flag_cool > 15 and each_data['fans'] < 5000:
                        focus = False
                        break
                    elif flag_cool > 7 and each_data['fans'] < 1000:
                        focus = False
                        break
                    else:
                        focus = True

                if focus:
                    count_focus += 1
                else:
                    count_unfocus += 1
                pre_fans = -1
                c_fans = -1
        logger.info("· 本轮筛选结果：")
        logger.info("× 不再追踪总数："+str(count_unfocus))
        logger.info("√ 持续追踪总数："+str(count_focus))

    def fans_variation(self):
        pass
