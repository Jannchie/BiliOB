from db import db
from time import sleep
import os
import datetime


def check():
    while True:
        sleep(10)
        try:
            db['blacklist'].find_one()
        except Exception:
            print('{} 链接失败'.format(datetime.datetime.now()))
            os.system('systemctl restart mongod')
            check()


check()
