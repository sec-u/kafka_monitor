#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-8-16 下午10:40
# @Author  : leon
# @Site    : 
# @File    : main.py
# @Software: PyCharm

from Queue import Queue
from ConfigParser import ConfigParser
from kafkamonitor.mythread import MyThread
from kafkamonitor.kafkamonitor import KafkaMonitor
from kafkamonitor.processor import Processor
from kafkamonitor.es import EsIndex


if __name__ == '__main__':
    # 配置文件读取
    cf = ConfigParser()
    cf.read('conf')

    kafka_ip_port_dict = dict(cf.items('kafka'))
    zookeepers_ip_port_dict = dict(cf.items('zookeepers'))
    sleep_time = cf.getint('time', 'sleep_time')

    es_ip_port = cf.get('elasticsearch', 'ip_port')
    es_index_name = cf.get('elasticsearch', 'index_name')
    all_data_type_name = cf.get('elasticsearch', 'all_type_name')
    es_bulk_num = cf.getint('elasticsearch', 'bulk_num')

    # Queue 实例化
    raw_data_queue = Queue()
    processing_data_queue = Queue()

    for i in kafka_ip_port_dict.keys():
        kafka_ip_port = kafka_ip_port_dict[i]
        zookeepers_ip_port = zookeepers_ip_port_dict.get(i)

        if not zookeepers_ip_port:
            # 配置文件有误 退出
            exit(1)

        # KafkaMonitor 实例化
        kafka = KafkaMonitor(raw_data_queue, kf_ip_port=kafka_ip_port,
                             zk_ip_port=zookeepers_ip_port,
                             sleep_time=sleep_time)

        # kafka 线程实例化 启动
        thread_kafka = MyThread(kafka.run)
        thread_kafka.start()

    # processor 实例化
    processor = Processor(raw_data_queue=raw_data_queue,
                          processing_data_queue=processing_data_queue,
                          time=sleep_time)

    # EsIndex 实例化
    es = EsIndex(processing_data_queue, es_index_name=es_index_name,
                 es_ip_port=es_ip_port, bulk_num=es_bulk_num)

    # processor 线程实例化 启动
    thread_processor = MyThread(processor.work)
    thread_processor.start()

    # es 线程实例化 启动
    thread_es = MyThread(es.run)
    thread_es.start()
