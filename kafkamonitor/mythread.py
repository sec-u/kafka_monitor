#!/usr/bin/env python
# -*- coding: utf-8 -*-


# @Time    : 16-8-16 下午4:04
# @Author  : leon
# @Site    : 
# @File    : mythread.py
# @Software: PyCharm

from threading import Thread


class MyThread(Thread):
    def __init__(self, func):
        super(MyThread, self).__init__()
        self.func = func

    def run(self):
        self.func()
