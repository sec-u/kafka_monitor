#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-8-16 下午4:01
# @Author  : leon
# @Site    : 
# @File    : processor.py
# @Software: PyCharm


from datetime import datetime


class Processor(object):
    def __init__(self, raw_data_queue, processing_data_queue,  time):
        self.raw_data_queue = raw_data_queue
        self.processing_data_queue = processing_data_queue
        self.old_data = {}
        self.time = time

    def get_data(self):
        for k in self.data.keys():
            data = self.data[k]
            # 获取topic数据
            topic_data = self.processor_data(data)
            if topic_data:
                self.data[k] = topic_data
            else:
                self.data.pop(k)

        # 获取总数据
        self.get_all()
        # 获取速度
        self.get_speed()

    @staticmethod
    def processor_data(data):
        """ 获取topics数据 """
        offset = data.pop('offset')
        logsize = data.pop('logsize')

        # offset logsize 是否有 None
        if not offset and logsize:
            return None

        offset_all = 0
        logsize_all = 0
        date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
        doc_type = '%s_%s' % (data['group_name'], data['topic_name'])

        for i in offset.keys():

            # 从 offset 获取key，并判断 logsize 是否有此 key
            if i not in logsize:
                    continue

            # 判断 key 值是否有None
            if offset[i] and logsize[i]:
                offset_all += offset[i]
                logsize_all += logsize[i]

        lag_all = logsize_all - offset_all
        if lag_all < 0:
            lag_all = 0

        data['offset'] = offset_all
        data['logsize'] = logsize_all
        data['lag'] = lag_all
        data['@timestamp'] = date
        data['my_type'] = doc_type

        return data

    def get_all(self):
        """ 获取总和值 """
        logsize_all = 0
        offset_all = 0
        lag_all = 0

        for k in self.data.keys():
            v = self.data[k]

            logsize_all += v['logsize']
            offset_all += v['offset']
            lag_all += v['lag']

        date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
        doc_type = 'all_all'
        self.data[doc_type] = {'my_type': doc_type, '@timestamp': date,
                               'offset': offset_all, 'logsize': logsize_all,
                               'lag': lag_all}

    def get_speed(self):
        """ 获取速度 """
        for k in self.data.keys():
            if k not in self.old_data:
                continue

            data = self.data[k]
            logsize = data['logsize']
            offset = data['offset']

            old_data = self.old_data[k]
            old_logsize = old_data['logsize']
            old_offset = old_data['offset']

            log_size_speed = (logsize - old_logsize) // self.time
            offset_speed = (offset - old_offset) // self.time

            data['log_size_speed'] = log_size_speed
            data['offset_speed'] = offset_speed

    def work(self):
        while True:
            # Queue 中获取原始数据
            self.data = self.raw_data_queue.get()
            # 处理数据
            self.get_data()
            # 赋值到旧数据
            self.old_data = self.data
            # 处理后数据 添加至 Queue
            self.processing_data_queue.put(self.data)
