from db import client
from time import sleep
import os
import datetime


def check():
    db = client['admin']
    while True:
        sleep(5)
        try:
            l = db.command("serverStatus")[
                'globalLock']['currentQueue']['total']
            if l >= 1:
                print('{} 链接失败'.format(datetime.datetime.now()))
                os.system('systemctl restart mongod')
        except Exception:
            print('{} 链接失败'.format(datetime.datetime.now()))
            os.system('systemctl restart mongod')
            check()


# db.currentOp().inprog.forEach(function(cop){
# db.killOp(cop.opid)
# })

check()
