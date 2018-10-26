#!/usr/bin/python3.6
# -*- coding:utf-8 -*-

import schedule
import time
from subprocess import Popen
import logging
import threading


# 第一步，创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Log等级总开关
# 第二步，创建一个handler，用于写入日志文件
rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
log_path = './'
log_name = log_path + rq + '.log'
logfile = log_name
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关
# 第三步，定义handler的输出格式
formatter = logging.Formatter(
    "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
# 第四步，将logger添加到handler里面
logger.addHandler(fh)

def bangumi():
    Popen(["scrapy","crawl","bangumi"])

def donghua():
    Popen(["scrapy","crawl","donghua"])

def update_author():
    Popen(["scrapy","crawl","authorUpdate"])

def auto_add_author():
    Popen(["scrapy","crawl","authorAutoAdd"])

def video_watcher():
    Popen(["scrapy","crawl","videoWatcher"])

def video_spider():
    Popen(["scrapy","crawl","videoSpider"])

def online():
    Popen(['scrapy','crawl','online'])

def data_analyze():
    Popen(['python','run_analyzer.py'])

def run_threaded(job_func):
     job_thread = threading.Thread(target=job_func)
     job_thread.start()

schedule.every().day.at('12:00').do(run_threaded,data_analyze)

schedule.every().day.at('01:00').do(run_threaded,update_author)
schedule.every(120).minutes.do(run_threaded,video_watcher)
schedule.every().day.at('07:00').do(run_threaded,video_spider)
schedule.every().day.at('14:00').do(run_threaded,auto_add_author)
schedule.every().day.at('16:50').do(run_threaded,bangumi)
schedule.every().day.at('16:50').do(run_threaded,donghua)
schedule.every().minute.do(run_threaded,online)


logging.info('开始运行计划任务..')
while True:
    schedule.run_pending()
    time.sleep(60)

