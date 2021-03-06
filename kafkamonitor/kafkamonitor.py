#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-8-16 下午2:53
# @Author  : leon
# @Site    : 
# @File    : kafkamonitor.py
# @Software: PyCharm

import time
import logging
from kafka import SimpleClient
from kafka.structs import OffsetRequestPayload
from kazoo.client import KazooClient


logger = logging.getLogger('KafkaMonitor')


def get_logsize(client, topic, partitions=None):
    if partitions is None:
        # 获取 partitions
        partitions = client.get_partition_ids_for_topic(topic)

    data = {}

    # 判断 topic 是否在 client.topic_partitions, 不在则创建
    # client.topic_partitions = {}   # topic -> partition -> leader
    if topic not in client.topic_partitions:
        client.load_metadata_for_topics(topic)

    # 验证 partition 是否有问题
    for partition in partitions:
        if client.topic_partitions[topic][partition] == -1:
            # 有问题则删除
            partitions.remove(partition)
            data[partition] = None

    reqs = []
    for partition in partitions:
        reqs.append(OffsetRequestPayload(topic, partition, -1, 1))

    resps = client.send_offset_request(reqs)

    for resp in resps:
        pending = resp.offsets[0]
        partition = resp.partition
        data[partition] = pending

    return data


class KafkaMonitor(object):
    def __init__(self, queue, kf_ip_port='localhost',
                 zk_ip_port='localhost', sleep_time=10):
        # 连接 kafka
        self.kafka_hosts = kf_ip_port
        self.broker = SimpleClient(hosts=self.kafka_hosts)
        # 连接zookeeper
        self.zookeepers_hosts = zk_ip_port
        self.zk = KazooClient(hosts=self.zookeepers_hosts, read_only=True)
        # 数据存放
        self.queue = queue
        # 时间间隔
        self.sleep_time = sleep_time - 1

    def get_group(self):
        """获取zookeepers下的group"""
        group_name = self.zk.get_children('/consumers')
        return group_name

    def get_topics(self, group):
        """group下的topic"""
        try:
            topics = self.zk.get_children("/consumers/%s/owners" % group)
            return topics
        except Exception as e:
            logging.error('get group topics failed! %s' % e)

    def get_logsize(self, topic):
        return get_logsize(self.broker, topic)

    def get_partition_offset(self, group, topic, partition):
        path = "/consumers/%s/offsets/%s/%s" % (group, topic, partition)

        if self.zk.exists(path):
            data, stat = self.zk.get(path)
            return data
        else:
            logger.error('get group:%s topic:%s partition:%s offset failed!' % (group, topic, partition))
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
                offset[int(partition)] = int(data)

            return offset

        except Exception as e:
            logger.error('get group:%s topic:%s offset failed! %s' % (group, topic, e))
            return None

    def get_group_data(self, group, topics):
        """获取一个 group 的所有 topic 的数据"""
        try:
            group_data = {}
            for topic in topics:

                # 获取 topic log_size 值
                log_size = self.get_logsize(topic)
                # 获取 topic offset 值
                offset = self.get_offset(group, topic)

                # topic 内的数据字典
                topic_data = dict(topic_name=topic, logsize=log_size,
                                  offset=offset, group_name=group)
                key = '%s_%s' % (group, topic)
                group_data[key] = topic_data

            return group_data

        except Exception as e:
            logger.error('get group:%s offset failed! %s' % (group, e))
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
            logger.error('get offset failed! %s' % e)
            time.sleep(3)

    def run(self):
        self.zk.start()
        while True:
            t0 = time.clock()
            data = self.get_data()
            self.queue.put(data)
            # 执行间隔时间
            t = time.clock() - t0

            # 睡眠时间减去执行时间 保证间隔时间相等
            sleep_time = self.sleep_time - t
            if sleep_time < 0:
                sleep_time = 0

            time.sleep(sleep_time)
