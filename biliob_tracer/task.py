from datetime import datetime
import threading
from time import sleep

import os


class Task(object):

    def get_computer_name(self):
        # 获取计算机名
        return os.environ['COMPUTERNAME']

    def __init__(self, task_name, update_frequency=5, timeout=60):
        self.task_name = task_name
        self.timeout = timeout
        self.finished = False
        self.start_time = datetime.now()
        self.update_frequency = update_frequency
        self.computer_name = self.get_computer_name()
        self.base_result = {'computer_name': self.computer_name,
                            'class_name': self.__class__.__name__,
                            'task_name': self.task_name}
        self.start_task()
        t_update = threading.Thread(target=self.update, name='update')
        t_update.start()
        pass

    def start_task(self):
        start_result = merge_dicts(
            self.base_result, {'start_time': self.start_time})
        self.send_result(start_result)

    def set_finished(self):
        if (datetime.now() - self.start_time).seconds >= self.timeout:
            self.finished = True
        return self.finished

    def update(self):
        while self.set_finished() == False:
            sleep(self.update_frequency)
            self.send_result(self.get_update_result())
        self.send_result(self.get_finish_result())

    def get_update_result(self, data={}):
        r = merge_dicts(self.base_result, {
                        'update_time': datetime.now()}, {'data': data})
        return r

    def finish_task(self):
        self.send_result(self.get_finish_result())

    def get_finish_result(self, data={}):
        r = merge_dicts(self.base_result, {'finish_time': datetime.now()},{'data':data})
        return r

    def send_result(self, result):
        if result != {}:
            self.output_result(result)

    def output_result(self, result):
        print(result)


def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result
