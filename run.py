import schedule
import time
from subprocess import Popen
import logging

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

def update_author():
    logging.info("开始定期更新author数据...")
    Popen("scrapy crawl authorUpdate")

def auto_add_author():
    logging.info("开始定期更新author数据...")
    Popen("scrapy crawl authorAutoAdd")

schedule.every().hour.do(update_author)
schedule.every().day.at('13:00').do(auto_add_author)

while True:
    schedule.run_pending()
    time.sleep(60)
