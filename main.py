#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-7-27 上午9:32
# @Author  : leon
# @Site    : 
# @File    : main.py
# @Software: PyCharm

import json
import time
from ConfigParser import ConfigParser
from Queue import Queue
from datetime import datetime
from threading import Thread
from elasticsearch import Elasticsearch, helpers
from kafka import SimpleClient
from kafka.consumer import SimpleConsumer
from kazoo.client import KazooClient
from kafka.structs import OffsetRequestPayload


class MyThread(Thread):
    def __init__(self, func):
        super(MyThread, self).__init__()
        self.func = func

    def run(self):
        self.func()


class MyConsumer(SimpleConsumer):
    def my_pending(self, partitions=None):
        if partitions is None:
            partitions = self.offsets.keys()

        reqs = []
        offset_total = 0

        for partition in partitions:
            reqs.append(OffsetRequestPayload(self.topic, partition, -1, 1))

        resps = self.client.send_offset_request(reqs)
        for resp in resps:
            pending = resp.offsets[0]
            offset_total += pending

        return offset_total


class KafkaMonitor(object):
    def __init__(self, queue, kf_ip_port='localhost',
                 zk_ip_port='localhost', kf_sleep_time=10, all_data_type_name='all'):
        self.zookeepers_hosts = zk_ip_port
        self.kafka_hosts = kf_ip_port
        self.zk = KazooClient(hosts=self.zookeepers_hosts, read_only=True)
        self.data_queue = queue
        self.group_topic = {}
        self.sleep_time = kf_sleep_time
        self.all_data_type_name = all_data_type_name
        self.consumers = {}

    def get_group(self):
        """获取zookeepers下的group"""
        group_name = self.zk.get_children('/consumers')
        return group_name

    def get_topics(self, group):
        """group下的topic"""
        try:
            topics = self.zk.get_children("/consumers/%s/owners" % group)
            return topics
        except:
            return None

    def add_consumer_dict(self, group, topic):
        """添加 consumers 连接"""
        k_name = '%s_%s' % (str(group), str(topic))
        consumer = MyConsumer(self.broker, group, str(topic))
        self.consumers[k_name] = consumer
        return self._get_log_size(consumer)

    def _get_log_size(self, consumer):
        """topic下消费量"""
        offset = consumer.my_pending()
        return offset

    def get_log_size(self, group, topic):
        """取出 consumers 连接"""
        k_name = '%s_%s' % (str(group), str(topic))
        if k_name in self.consumers:
            consumer = self.consumers[k_name]
            return self._get_log_size(consumer)
        else:
            return self.add_consumer_dict(group, topic)

    def get_offset(self, group, topic):
        """topic下生产量"""
        try:
            log_size = 0
            partitions = self.zk.get_children("/consumers/%s/offsets/%s" % (group, topic))
            for partition in partitions:
                log = "/consumers/%s/offsets/%s/%s" % (group, topic, partition)
                if self.zk.exists(log):
                    data, stat = self.zk.get(log)
                    log_size += int(data)
            return log_size
        except:
            return None

    def get_lag(self, log_size, offset):
        """topic下积压量"""
        lag = log_size - offset
        if lag < 0:
            lag = 0
        return lag

    def get_date(self, group, topics):
        """获取一个 group 的所有 topic 的数据"""
        try:
            group_data = []
            lag_all = 0
            log_size_all = 0
            offset_all = 0
            for topic in topics:
                # 获取 topic log_size 值
                log_size = self.get_log_size(group, topic)
                # 获取 topic offset 值
                offset = self.get_offset(group, topic)
                # 获取 topic lag 值
                lag = self.get_lag(log_size, offset)
                # topic 内的数据字典
                topic_data = {'topic_name': topic, 'lag': lag, 'log_size': log_size, 'offset': offset}
                group_data.append(topic_data)
                lag_all += lag
                log_size_all += log_size
                offset_all += offset
            group_all = {'lag': lag_all, 'log_size': log_size_all, 'offset': offset_all}
            group_data.append(group_all)
            return group_data
        except:
            return None

    def worker(self):
        try:
            # 获取 groups_name 列表
            groups_name = self.get_group()
            # 定义本次循环 data 字典
            data = {}
            lag_all = 0
            log_size_all = 0
            offset_all = 0
            for group in groups_name:
                # 一个 group 内的数据列表
                # 获取 group 内的 topics 列表
                topics = self.get_topics(group)
                # 获取 group 数据列表
                group_data = self.get_date(group, topics)
                if group_data:
                    group_all = group_data.pop()
                    lag_all += group_all['lag']
                    log_size_all += group_all['log_size']
                    offset_all += group_all['offset']
                    # group 数据列表加入到本次循环的data 字典
                    data[group] = group_data
            group_all_data_dict = {'topic_name': self.all_data_type_name, 'lag': lag_all, 'log_size': log_size_all,
                                   'offset': offset_all}
            group_all_data_list = [group_all_data_dict]
            data['All'] = group_all_data_list
            # 本次循环的data 字典加入到 Queue
            self.data_queue.put(data)
        except:
            time.sleep(3)

    def run(self):
        self.zk.start()
        self.broker = SimpleClient(hosts=self.kafka_hosts)
        while True:
            self.worker()
            # 睡眠
            time.sleep(self.sleep_time)


class EsIndex(object):
    def __init__(self, queue, es_index_name,
                 es_ip_port='localhost', bulk_num=0):
        self.elasticsearch_ip_port = es_ip_port
        # 实例化连接es
        self.es = Elasticsearch(hosts=self.elasticsearch_ip_port)
        self.data_queue = queue
        # es索引名字
        self.es_index_name = es_index_name
        self.es_data_queue = Queue()
        self.bulk_num = bulk_num

    def get_data(self):
        """Queue 获取数据"""
        data = self.data_queue.get()
        return data

    def process_data(self, data):
        """数据整理、拆分 放入Queue"""
        for k, v in data.items():
            for i in v:
                i['@timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
                self.es_data_queue.put(i)

    def es_index(self, data):
        try:
            # 获取 topic_name
            doc_type = data['topic_name']
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
            print(date, e)
            time.sleep(5)

    def bulk_data(self):
        """ 构造要插入的bulk数据 """
        bulk_data = []
        while True:
            data = self.es_data_queue.get()
            # 获取 topic_name
            doc_type = data['topic_name']
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

    def es_bulk(self, data):
        """ bulk 插入数据 """
        try:
            helpers.bulk(self.es, data)
        except Exception as e:
            date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
            print(date, e)

    def es_bulk_worker(self):
        """ bulk 插入 """
        while True:
            bulk_data = self.bulk_data()
            self.es_bulk(bulk_data)

    def es_worker(self):
        """ 单条 index 插入 """
        while True:
            # 从Queue中获取数据
            data = self.es_data_queue.get()
            # 插入数据到es
            self.es_index(data)

    def data_worker(self):
        """ 数据处理为单个字典 """
        while True:
            # 获取原始数据
            data = self.get_data()
            # 原始数据整理、拆分 放入Queue
            self.process_data(data)

    def run(self):
        thread_data = MyThread(self.data_worker)
        thread_data.start()
        if self.bulk_num == 0:
            thread_es_index = MyThread(self.es_worker)
            thread_es_index.start()
        else:
            thread_es_bulk = MyThread(self.es_bulk_worker)
            thread_es_bulk.start()


if __name__ == '__main__':
    # 配置文件读取
    cf = ConfigParser()
    cf.read('conf')
    kafka_ip_port = cf.get('kafka', 'ip_port')
    zookeepers_ip_port = cf.get('zookeepers', 'ip_port')
    sleep_time = cf.getint('time', 'sleep_time')
    elasticsearch_ip_port = cf.get('elasticsearch', 'ip_port')
    elasticsearch_index_name = cf.get('elasticsearch', 'index_name')
    all_data_type_name = cf.get('elasticsearch', 'all_type_name')
    elasticsearch_bulk_num = cf.getint('elasticsearch', 'bulk_num')
    # Queue 实例化
    data_queue = Queue()
    # KafkaMonitor 实例化
    kafka = KafkaMonitor(data_queue, kf_ip_port=kafka_ip_port,
                         zk_ip_port=zookeepers_ip_port,
                         kf_sleep_time=sleep_time, all_data_type_name=all_data_type_name)
    # EsIndex 实例化
    es = EsIndex(data_queue, es_index_name=elasticsearch_index_name,
                 es_ip_port=elasticsearch_ip_port, bulk_num=elasticsearch_bulk_num)
    # kafka 线程实例化 启动
    thread_kafka = MyThread(kafka.run)
    thread_kafka.start()
    # es 线程实例化 启动
    thread_es = MyThread(es.run)
    thread_es.run()

