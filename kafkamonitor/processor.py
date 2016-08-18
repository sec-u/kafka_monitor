#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-8-16 下午4:01
# @Author  : leon
# @Site    : 
# @File    : processor.py
# @Software: PyCharm


from datetime import datetime


class Processor(object):
    def __init__(self, queue, new_queue,  time):
        self.queue = queue
        self.new_queue = new_queue
        self.old_data = {}
        self.time = time

    def get_topic_data(self):
        for k, v in self.data.items():
            yield v

    def get_data(self):
        processor_data = []
        try:
            while True:
                data_iterator = self.get_topic_data()
                data = data_iterator.next()
                p_data = self.processor_data(data)
                speed_data = self.get_speed(p_data)
                processor_data.append(speed_data)
        except StopIteration:
            return processor_data

    @staticmethod
    def processor_data(data):
        offset = data.pop('offset')
        logsize = data.pop('logsize')

        offset_all = 0
        logsize_all = 0
        date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
        doc_type = '%s_%s' % (data['group_name'], data['topic_name'])

        for i in offset.keys():
            if offset[i] and logsize[i]:
                offset_all += offset
                logsize_all += logsize[i]

        lag_all = offset_all - logsize_all
        if lag_all < 0:
            lag_all = 0

        data['offset'] = offset_all
        data['logsize'] = logsize_all
        data['lag'] = lag_all
        data['@timestamp'] = date
        data['my_type'] = doc_type

        return data

    def get_speed(self, datas):
        new_data = []
        for data in datas:
            doc_type = '%s_%s' % (data['group_name'], data['topic_name'])
            if doc_type not in self.old_data:
                new_data.append(data)
                continue

            logsize = data['logsize']
            offset = data['offset']

            old_data = self.old_data[doc_type]
            old_logsize = old_data['logsize']
            old_offset = old_data['offset']

            log_size_speed = (logsize - old_logsize) // self.time
            offset_speed = (offset - old_offset) // self.time

            data['log_size_speed'] = log_size_speed
            data['offset_speed'] = offset_speed

            new_data.append(data)
        return new_data

    def work(self):
        while True:
            self.data = self.queue.get()
            self.old_data = self.data
            data = self.get_data()
            self.new_queue.put(data)
