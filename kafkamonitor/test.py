#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-8-16 下午8:44
# @Author  : leon
# @Site    : 
# @File    : test.py
# @Software: PyCharm


class processor(object):
    def __init__(self, data):
        self.data = data

    def get_topic_data(self):
        for k, v in self.data.items():
            yield v

    def get_data(self):
        data_iterator = self.get_topic_data()
        data1 = []
        try:
            while True:
                data = data_iterator.next()
                data1.append(data)
        except StopIteration:
            return data1

d1 = {1: 'b', 2: 'b', 3: 'b'}
d2 = {4: 'd', 5: 'd', 6: 'd'}
d = {'d1':d1, 'd2':d2}

l = processor(d)
print l.get_data()