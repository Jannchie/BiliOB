from db import db
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
import logging

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
logger = logging.getLogger(__name__)


class VideoAnalyzer(object):
    def __init__(self):
        self.db = db  # 获得数据库的句柄
        self.coll = self.db['video']  # 获得collection的句柄

    def video_filter(self):
        pre_view = -1
        c_view = -1
        delta = timedelta(1)
        pre_date = datetime
        c_date = datetime
        count_delete = 0
        count_unfocus = 0
        count_focus = 0
        for each_doc in self.coll.find({'focus': True}):
            live_time = 0
            delete = False
            focus = True
            if 'data' in each_doc:
                each_doc['data'].reverse()
                for each_data in each_doc['data']:

                    if pre_view == -1:
                        pre_view = each_data['view']
                        pre_date = each_data['datetime']
                        continue
                    c_view = each_data['view']
                    c_date = each_data['datetime']

                    if pre_date + delta > c_date:
                        continue
                    live_time += 1
                    rate = (c_view-pre_view)
                    pre_view = c_view
                    pre_date = c_date
                    # 三天内播放增长小于3000则被认定为低质量
                    if live_time == 3 and c_view < 3000:
                        delete = False
                        focus = False
                        break
                    # 大于7天后每日播放增长小于10000则停止追踪
                    if live_time > 7 and rate < 10000:
                        focus = False
                        delete = False
                        break
                    # 除此之外的情况则持续追踪
                    else:
                        focus = True
                        delete = False
                if delete:
                    count_delete += 1
                    self.coll.delete_one({'aid': each_doc['aid']})
                elif focus:
                    count_focus += 1
                else:
                    count_unfocus += 1
                    self.coll.update_one({'aid': each_doc['aid']}, {
                                         '$set': {'focus': False}})
                pre_view = -1
                c_view = -1
        logger.info("· 本轮筛选结果：")
        logger.info("! 删除辣鸡总数："+str(count_delete))
        logger.info("× 不再追踪总数："+str(count_unfocus))
        logger.info("√ 持续追踪总数："+str(count_focus))
