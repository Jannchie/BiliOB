from datetime import datetime
import threading
from time import sleep

import os


class Task(object):

    def get_computer_name(self):
        # 获取计算机名
        return os.environ['COMPUTERNAME']

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
        self.start_task()
        t_update = threading.Thread(target=self.update, name='update')
        t_update.start()
        pass

    def start_task(self):
        data = self.get_start_data()
        start_result = merge_dicts(
            self.base_result, {
                'type': 'start', 'time': self.start_time}, {'data': data})
        self.send_result(start_result)

    def get_start_data(self):
        return {'status': 1, 'msg': 'STARTED'}

    def set_finished(self):
        if (datetime.now() - self.start_time).seconds >= self.timeout:
            self.finished = True
        return self.finished

    def update(self):
        while self.set_finished() == False:
            sleep(self.update_frequency)
            self.send_result(self.get_update_result())
        self.send_result(self.get_finish_result())

    def get_update_result(self):
        data = self.get_update_data()
        r = merge_dicts(self.base_result, {
                        'type': 'update', 'time': datetime.now()}, {'data': data})
        return r

    def get_update_data(self):
        return {'status': 1, 'msg': 'EXECUTING'}

    def finish_task(self):
        self.send_result(self.get_finish_result())

    def get_finish_result(self):
        data = self.get_finish_data()
        r = merge_dicts(self.base_result, {
                        'type': 'finish', 'time': datetime.now()}, {'data': data})
        return r

    def get_finish_data(self):
        return {'status': -1, 'msg': 'TIMEOUT'}

    def send_result(self, result):
        if result != {}:
            self.output_result(result)

    def output_result(self, result):
        if self.collection != None:
            self.collection.update_one(
                self.base_result, {'$set': result}, True)


class ExistsTask(Task):
    def __init__(self, task_name, update_frequency=5, collection=None):
        super().__init__(task_name, update_frequency, 0, collection)

    def set_finished(self):
        return False

    def get_update_data(self):
        return {'status': 1, 'msg': 'ALIVE'}


class ProgressTask(Task):

    def __init__(self, task_name, total_value, update_frequency=5, collection=None):
        self.total_value = total_value
        self.current_value = 0
        super().__init__(task_name, update_frequency, 0, collection)

    def get_start_data(self):
        return {'status': 1, 'msg': 'STARTED', 'total_value': self.total_value}

    def set_finished(self):
        if self.current_value == self.total_value:
            return True
        else:
            return False

    def get_update_data(self):
        return {'status': 1, 'msg': 'PROCESSING', 'value': self.current_value}

    def get_finish_data(self):
        return {'status': 1, 'msg': 'COMPLETE'}


def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result
