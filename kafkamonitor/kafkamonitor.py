#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-8-16 下午2:53
# @Author  : leon
# @Site    : 
# @File    : kafkamonitor.py
# @Software: PyCharm

import time
import logging
from datetime import datetime
from kafka import SimpleClient
from kafka.structs import OffsetRequestPayload
from kazoo.client import KazooClient


def get_logsize(client, topic, partitions=None):
    if partitions is None:
        partitions = client.get_partition_ids_for_topic(topic)

    data = {}

    for partition in partitions:
        reqs = [OffsetRequestPayload(topic, partition, -1, 1)]

        resps = client.send_offset_request(reqs)
        for resp in resps:
            pending = resp.offsets[0]
            data[partition] = pending

    return data


class KafkaMonitor(object):
    def __init__(self, kf_ip_port='localhost',
                 zk_ip_port='localhost'):
        # 连接 kafka
        self.kafka_hosts = kf_ip_port
        self.broker = SimpleClient(hosts=self.kafka_hosts)
        # 连接zookeeper
        self.zookeepers_hosts = zk_ip_port
        self.zk = KazooClient(hosts=self.zookeepers_hosts, read_only=True)

    def get_group(self):
        """获取zookeepers下的group"""
        group_name = self.zk.get_children('/consumers')
        return group_name

    def get_topics(self, group):
        """group下的topic"""
        try:
            topics = self.zk.get_children("/consumers/%s/owners" % group)
            return topics
        except Exception:
            return None

    def get_log_size(self, topic):
        return get_logsize(self.broker, topic)

    def get_partition_offset(self, group, topic, partition):
        path = "/consumers/%s/offsets/%s/%s" % (group, topic, partition)

        if self.zk.exists(path):
            data, stat = self.zk.get(path)
            return data
        else:
            return None

    def get_offset(self, group, topic):
        """topic下消费量"""
        try:
            offset = {}

            # 获取 partitions
            path = "/consumers/%s/offsets/%s" % (group, topic)
            partitions = sorted(self.zk.get_children(path))

            for partition in partitions:
                data = self.get_partition_offset(group, topic, partition)
                offset[partition] = data

            return offset

        except Exception:
            return None

    def get_group_data(self, group, topics):
        """获取一个 group 的所有 topic 的数据"""
        try:
            group_data = {}
            for topic in topics:

                # 获取 topic log_size 值
                log_size = self.get_log_size(topic)
                # 获取 topic offset 值
                offset = self.get_offset(group, topic)

                # topic 内的数据字典
                topic_data = dict(topic_name=topic, log_size=log_size,
                                  offset=offset, group_name=group)
                key = '%s_%s' % (group, topic)
                group_data[key] = topic_data

            return group_data

        except Exception as e:

            date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            group = group.encode('utf-8')
            print("%s 获取group %s 数据失败" % (date, group))

            logging.exception(e)
            return None

    def get_data(self):
        try:
            # 获取 groups_name 列表
            groups_name = self.get_group()
            # 定义本次循环 data 字典
            data = {}

            for group in groups_name:

                # 获取 group 内的 topics 列表
                topics = self.get_topics(group)
                if not topics:
                    continue

                # 获取 group 数据列表
                group_data = self.get_group_data(group, topics)
                if group_data:
                    # group 数据加入到本次循环的data 字典
                    data = dict(data, **group_data)

            return data

        except Exception as e:
            date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            print(date)

            logging.exception(e)
            time.sleep(3)

    def run(self):
        self.zk.start()
        while True:
            data = self.get_data()
            yield data
