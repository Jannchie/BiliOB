from datetime import datetime
import threading
from time import sleep
import socket


class STATUS():
    START = 1
    UPDATE = 2
    DEAD = 4
    ALIVE = 5
    WARNING = 6
    TIMEOUT = 8
    FINISHED = 9


class Task(object):

    def get_computer_name(self):
        # 获取计算机名
        hostname = socket.gethostname()
        return hostname

    def __init__(self, task_name, update_frequency=5, timeout=60, collection=None):
        self.collection = collection
        self.task_name = task_name
        self.timeout = timeout
        self.finished = False
        self.start_time = datetime.now()
        self.update_frequency = update_frequency
        self.computer_name = self.get_computer_name()
        self.base_result = {'computer_name': self.computer_name,
                            'class_name': self.__class__.__name__,
                            'task_name': self.task_name,
                            'start_time': self.start_time}
        self.__start_task()
        t_update = threading.Thread(target=self.__update, name='update')
        t_update.start()
        pass

    def __start_task(self):
        data = self._get_start_data()
        start_result = self._merge_result(
            self.base_result, data)
        self.__send_result(start_result)

    def _get_start_data(self):
        return {'status': STATUS.START, 'msg': '开始追踪任务'}

    def _judge_finished(self):
        if self.finished == True:
            return True
        if (datetime.now() - self.start_time).seconds >= self.timeout:
            self.finished = True
        return self.finished

    def __update(self):
        while self._judge_finished() == False:
            sleep(self.update_frequency)
            self.__send_result(self.__get_update_result())
        self.__send_result(self.get_finish_result())

    def __get_update_result(self):
        data = self._get_update_data()
        r = self._merge_result(self.base_result, {
            'update_time': datetime.now()}, data)
        return r

    def _get_update_data(self):
        return {'status': STATUS.UPDATE, 'msg': '更新追踪任务'}

    def __finish_task(self):
        self.__send_result(self.get_finish_result())

    def get_finish_result(self):
        data = self._get_finish_data()
        r = self._merge_result(
            self.base_result, {'update_time': datetime.now()}, data)
        return r

    def _get_finish_data(self):
        return {'status': STATUS.TIMEOUT, 'msg': '预定时间内未收到更新信号，确认失联。'}

    def __send_result(self, result):
        if result != {}:
            self.__output_result(result)

    def __output_result(self, result):
        if self.collection != None:
            try:
                self.collection.update_one(
                    {'task_name': self.task_name, 'computer_name': self.computer_name}, {'$set': result}, True)
            except Exception as error:
                print(error)
                pass
        else:
            print(result)

    def _merge_result(self, *dicts):
        result = {}
        for each_dict in dicts:
            for each_kw in each_dict:
                result[each_kw] = each_dict[each_kw]
        return result


class ExistsTask(Task):
    def __init__(self, task_name, update_frequency=5, collection=None):
        super().__init__(task_name, update_frequency, 0, collection)

    def _judge_finished(self):
        return False

    def _get_update_data(self):
        return {'status': STATUS.ALIVE, 'msg': '任务正常执行中'}


class ProgressTask(Task):

    def __init__(self, task_name, total_value, update_frequency=5, collection=None):
        self.total_value = total_value
        self.current_value = 0
        super().__init__(task_name, update_frequency, 60, collection)

    def _get_start_data(self):
        return {'status': STATUS.START, 'msg': '计划任务开始', 'total_value': self.total_value, 'current_value': 0}

    def _judge_finished(self):
        if self.finished == True:
            return True
        if self.current_value >= self.total_value:
            return True
        else:
            return False

    def _get_update_data(self):
        return {'status': STATUS.UPDATE, 'msg': '计划任务执行中', 'current_value': self.current_value}

    def _get_finish_data(self):
        return {'status': STATUS.FINISHED, 'msg': '计划任务已完成'}


class SpiderTask(ExistsTask):
    def __init__(self, task_name, update_frequency=5, collection=None):
        super().__init__(task_name, update_frequency, collection)
        self.crawl_count = 0
        self.crawl_failed = 0

    def _get_update_data(self):
        return {'crawl_count': self.crawl_count, 'crawl_failed': self.crawl_failed, 'status': STATUS.ALIVE, 'msg': '任务正常执行中'}
