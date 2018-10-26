from db import settings
from pymongo import MongoClient 
from datetime import datetime
from datetime import timedelta
class AuthorAnalyzer(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                        settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄
    def author_filter(self):
        pre_fans = -1
        c_fans = -1
        delta = timedelta(1)
        pre_date = datetime
        c_date = datetime
        count_unfocus = 0
        count_focus = 0
        for each_doc in self.coll.find({'focus':True}):
            flag_cool = 0
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
                rate = (c_fans-pre_fans)/((c_date-pre_date).seconds*60*60*24+1)
                pre_fans = c_fans
                pre_date = c_date
                if abs(rate) < 100:
                    flag_cool += 1
                else:
                    flag_cool = 0

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
                print("√ 持续追踪："+each_doc['name'])
                self.coll.update_one({'mid':each_doc['mid']},{'$set':{'focus':True}})
            else:
                count_unfocus += 1
                print("× 不再追踪："+each_doc['name'])
                self.coll.update_one({'mid':each_doc['mid']},{'$set':{'focus':False}})
            pre_fans = -1
            c_fans = -1
        print("· 本轮筛选结果：")
        print("× 不再追踪总数："+str(count_unfocus))
        print("√ 持续追踪总数："+str(count_focus))
    
    def fans_variation(self):
        pass

