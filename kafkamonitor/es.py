#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-8-16 下午10:06
# @Author  : leon
# @Site    : 
# @File    : es.py
# @Software: PyCharm

import json
import logging
from datetime import datetime
from Queue import Queue
from mythread import MyThread
from elasticsearch import Elasticsearch, helpers


class EsIndex(object):
    def __init__(self, queue, es_index_name,
                 es_ip_port='localhost:9200', bulk_num=0):
        self.elasticsearch_ip_port = es_ip_port
        # 实例化连接es
        self.es = Elasticsearch(hosts=self.elasticsearch_ip_port)
        self.data_queue = queue
        # es 索引名字
        self.es_index_name = es_index_name
        self.bulk_num = bulk_num
        # es 数据queue
        self.es_data_queue = Queue()

    def one_index(self, data):
        try:
            # 获取 doc_type
            doc_type = data['my_type']
            # 转换为 json
            data_json = json.dumps(data)
            # 获取日期
            date = datetime.now().strftime('%Y.%m.%d')
            # es_index_name 加上日期
            index_name = '%s-%s' % (self.es_index_name, date)
            # 插入数据到es
            self.es.index(index=index_name, doc_type=doc_type, body=data_json)
        except Exception as e:
            date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
            logging.exception(e)
            print("%s index数据插入es失败 %s" % (date, e))

    def bulk_index(self, data):
        """ bulk 插入数据 """
        try:
            helpers.bulk(self.es, data)
        except Exception as e:
            date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            logging.exception(e)
            print('%s bulk数据插入es失败 %s' % (date, e))

    def bulk_data(self):
        """ 构造要插入的bulk数据 """
        bulk_data = []
        while True:
            data = self.es_data_queue.get()
            # 获取 doc_type
            doc_type = data['my_type']
            # 转换为 json
            data_json = json.dumps(data)
            # 获取日期
            date = datetime.now().strftime('%Y.%m.%d')
            # es_index_name 加上日期
            index_name = '%s-%s' % (self.es_index_name, date)
            action = {
                "_index": index_name,
                "_type": doc_type,
                "_source": data_json
            }
            bulk_data.append(action)
            if len(bulk_data) == self.bulk_num:
                break
        return bulk_data

    def bulk_worker(self):
        """ bulk 插入 """
        while True:
            bulk_data = self.bulk_data()
            self.bulk_index(bulk_data)

    def one_worker(self):
        """ 单条 index 插入 """
        while True:
            # 从Queue中获取数据
            data = self.es_data_queue.get()
            # 插入数据到es
            self.one_index(data)

    def data_worker(self):
        """ 数据处理为单个字典 """
        while True:
            # 获取原始数据
            data = self.data_queue.get()
            # 原始数据整理、拆分 放入Queue
            for i in data:
                self.es_data_queue.put(i)

    def run(self):
        thread_data = MyThread(self.data_worker)
        thread_data.start()
        if self.bulk_num == 0:
            # 单条提交数据
            thread_es_index = MyThread(self.one_worker())
            thread_es_index.start()
        else:
            # bulk 提交数据
            thread_es_bulk = MyThread(self.bulk_worker())
            thread_es_bulk.start()
