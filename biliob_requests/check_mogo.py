from db import db
from time import sleep
import os
import datetime


def check():
    print('启动数据库服务看门狗模块')
    while True:
        sleep(5)
        try:
            if len(db.current_op()['inprog']) >= 100:
                print('{} 链接失败'.format(datetime.datetime.now()))
                os.system('systemctl restart mongod')
                check()
        except Exception:
            print('{} 链接失败'.format(datetime.datetime.now()))
            os.system('systemctl restart mongod')
            check()


check()
