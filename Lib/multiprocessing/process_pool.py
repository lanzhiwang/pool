#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = ['Pool']

import threading
import Queue
import itertools
import collections
import time

from multiprocessing import Process, cpu_count
from multiprocessing.util import Finalize, debug

#
# Constants representing the state of a pool
#
RUN = 0
CLOSE = 1
TERMINATE = 2


job_counter = itertools.count()


def worker(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None):
    """
    inqueue: 用于分发存储内部结构化的任务 inqueue = Pool._inqueue = SimpleQueue()
    outqueue: 用于分发存储任务的执行结果 outqueue = Pool._outqueue = SimpleQueue()
    initializer: 启动每个 worker 进程后执行的函数 initializer = Pool._initializer = initializer
    initargs: initializer 函数的参数 initializer = Pool._initargs = initargs
    maxtasks: 指定每个 worker 进程最多的处理任务数 maxtasks = Pool._maxtasksperchild = maxtasksperchild

    args=(self._inqueue, self._outqueue,
          self._initializer,
          self._initargs, self._maxtasksperchild)
    """
    if initializer is not None:
        initializer(*initargs)

    task = inqueue.get()
    print 'worker process: ', task  # (0, None, <function sqr at 0x7f8c0462e938>, (5,), {})
    job, i, func, args, kwds = task
    result = (True, func(*args, **kwds))
    outqueue.put((job, i, result))


class Pool(object):
    """process pool.
    Attributes:
        _inqueue: 用于分发存储内部结构化的任务 self._inqueue = SimpleQueue()
        _outqueue: 用于分发存储任务的执行结果 self._outqueue = SimpleQueue()
        _quick_put: _inqueue 队列 put方法 self._quick_put = self._inqueue._writer.send
        _quick_get: _outqueue 队列 get 方法 self._quick_get = self._outqueue._reader.recv
        _taskqueue: 用于分发存储用户输入的任务 self._taskqueue = Queue.Queue()
        _cache: Pool 实例和 ApplyResult 实例共享数据，用于存储任务以及任务结果 self._cache = {}
        _state: 标识主进程状态 self._state = RUN
        _maxtasksperchild: 指定每个 worker 进程最多的处理任务数 self._maxtasksperchild = maxtasksperchild
        _initializer: 启动每个 worker 进程后执行的函数 self._initializer = initializer
        _initargs: _initializer 函数的参数 self._initargs = initargs
        _processes: 标识 worker 进程的数量 self._processes = processes
        _pool: 存储所有的 worker 进程实例 self._pool = []
        _worker_handler: Thread实例
        _task_handler: Thread实例, 该线程从 _taskqueue 队列获取任务，将任务 put 进 _inqueue 队列
        _result_handler: Thread实例，该线程从  _outqueue 队列获取结果，更新 ApplyResult 对象的相关属性
        _terminate = Finalize实例 self._terminate = Finalize()
    """

    Process = Process

    def __init__(self, processes=None, initializer=None, initargs=(),
                 maxtasksperchild=None):
        self._setup_queues()
        self._taskqueue = Queue.Queue()  # 用于分发存储用户输入的任务
        self._cache = {}  #  Pool 实例和 ApplyResult 实例共享数据，用于存储任务以及任务结果
        self._state = RUN  # 标识主进程状态
        self._maxtasksperchild = maxtasksperchild  # 指定每个 worker 进程最多的处理任务数
        self._initializer = initializer  # 启动每个 worker 进程后执行的函数
        self._initargs = initargs  # _initializer 函数的参数

        if processes is None:
            try:
                processes = cpu_count()
            except NotImplementedError:
                processes = 1
        if processes < 1:
            raise ValueError("Number of processes must be at least 1")

        if initializer is not None and not hasattr(initializer, '__call__'):
            raise TypeError('initializer must be a callable')

        self._processes = processes
        self._pool = []
        """
        w = self.Process()
        self._pool.append(w)
        """
        self._repopulate_pool()

        self._worker_handler = threading.Thread(
            target=Pool._handle_workers,
            args=(self, )
            )
        self._worker_handler.daemon = True
        self._worker_handler._state = RUN
        self._worker_handler.start()

        self._task_handler = threading.Thread(
            target=Pool._handle_tasks,
            args=(self._taskqueue, self._quick_put, self._outqueue,
                  self._pool, self._cache)
            )
        self._task_handler.daemon = True
        self._task_handler._state = RUN
        self._task_handler.start()

        self._result_handler = threading.Thread(
            target=Pool._handle_results,
            args=(self._outqueue, self._quick_get, self._cache)
            )
        self._result_handler.daemon = True
        self._result_handler._state = RUN
        self._result_handler.start()

        self._terminate = Finalize(
            self, self._terminate_pool,
            args=(self._taskqueue, self._inqueue, self._outqueue, self._pool,
                  self._worker_handler, self._task_handler,
                  self._result_handler, self._cache),
            exitpriority=15
            )

    @classmethod
    def _terminate_pool(cls, taskqueue, inqueue, outqueue, pool,
                        worker_handler, task_handler, result_handler, cache):
        pass

    @staticmethod
    def _handle_workers(pool):
        """
        pool: Pool实例对象本身
        args=(self, )
        """
        pass

    @staticmethod
    def _handle_tasks(taskqueue, put, outqueue, pool, cache):
        """
        taskqueue: 用于分发存储用户输入的任务队列
        put: _inqueue 队列 put方法, 其中 _inqueue: 用于分发存储内部结构化的任务
        outqueue: 用于分发存储任务的执行结果队列
        pool: 存储所有的 worker 进程实例
        cache: Pool 实例和 ApplyResult 实例共享数据，用于存储任务以及任务结果

        args=(self._taskqueue, self._quick_put, self._outqueue,
              self._pool, self._cache)
        """
        taskseq = taskqueue.get()
        print 'thread handle_tasks: ', taskseq  # ([(0, None, <function sqr at 0x7f1fabd7c938>, (5,), {})], None)
        task = taskseq[0][0]
        print 'thread handle_tasks: ', task  # (0, None, <function sqr at 0x7f8aace37938>, (5,), {})
        put(task)

    @staticmethod
    def _handle_results(outqueue, get, cache):
        """
        outqueue: 用于分发存储任务的执行结果队列
        get: _inqueue 队列 put 方法，其中 _inqueue 用于分发存储内部结构化的任务
        cache: Pool 实例和 ApplyResult 实例共享数据，用于存储任务以及任务结果

        args=(self._outqueue, self._quick_get, self._cache)
        """
        taskseq = outqueue.get()
        print 'handle_results: ', taskseq  # (0, None, (True, 25))
        job, i, obj = taskseq
        cache[job]._set(i, obj)

    def _repopulate_pool(self):
        """实例化 worker 进程
        """
        for i in range(self._processes - len(self._pool)):
            w = self.Process(target=worker,
                             args=(self._inqueue, self._outqueue,
                                   self._initializer,
                                   self._initargs, self._maxtasksperchild)
                            )

            self._pool.append(w)
            w.name = w.name.replace('Process', 'PoolWorker')
            w.daemon = True
            w.start()
            print w.pid
            debug('added worker')

    def _setup_queues(self):
        """初始化分发存储内部任务和任务结果的队列"""
        from .queues import SimpleQueue
        self._inqueue = SimpleQueue()  # 用于分发存储内部结构化的任务
        self._outqueue = SimpleQueue()  # 用于分发存储任务的执行结果
        self._quick_put = self._inqueue._writer.send  # _inqueue 队列 put方法
        self._quick_get = self._outqueue._reader.recv  # _outqueue 队列 get 方法

    def apply(self, func, args=(), kwds={}):
        '''
        Equivalent of `apply()` builtin
        '''
        print '=============================method apply in class Pool '
        assert self._state == RUN
        print 'self._state:  {} type: {}' . format(self._state, type(self._state))
        apply_async_result = self.apply_async(func, args, kwds)
        result = apply_async_result.get()
        return result

    def map(self, func, iterable, chunksize=None):
        pass

    def imap(self, func, iterable, chunksize=1):
        pass

    def imap_unordered(self, func, iterable, chunksize=1):
        pass

    def apply_async(self, func, args=(), kwds={}, callback=None):
        '''
        Asynchronous equivalent of `apply()` builtin
        '''
        print '=============================method apply_async in class Pool '
        assert self._state == RUN
        print 'self._state:  {} type: {}' . format(self._state, type(self._state))
        result = ApplyResult(self._cache, callback)
        print 'self._cache:  {} type: {}' . format(self._cache, type(self._cache))
        task = ([(result._job, None, func, args, kwds)], None)
        print task  # ([(0, None, <function sqr at 0x7fefaabc1938>, (5,), {})], None)
        self._taskqueue.put(task)
        return result

    def map_async(self, func, iterable, chunksize=None, callback=None):
        pass

    def close(self):
        pass

    def terminate(self):
        pass

    def join(self):
        pass

    def __reduce__(self):
        raise NotImplementedError(
              'pool objects cannot be passed between processes or pickled'
              )


class ApplyResult(object):
    def __init__(self, cache, callback):
        print '=============================method __init__ in class ApplyResult '
        self._cond = threading.Condition(threading.Lock())
        self._job = job_counter.next()  # job_counter = itertools.count() 创建一个迭代器，从 0 开始计算
        self._cache = cache
        self._ready = False
        self._callback = callback
        cache[self._job] = self
        print 'self._cond:  {} type: {}' . format(self._cond, type(self._cond))
        print 'self._job:  {} type: {}' . format(self._job, type(self._job))
        print 'self._cache:  {} type: {}' . format(self._cache, type(self._cache))
        print 'self._ready:  {} type: {}' . format(self._ready, type(self._ready))
        print 'self._callback:  {} type: {}' . format(self._callback, type(self._callback))
        print cache

    def wait(self, timeout=None):
        self._cond.acquire()  #
        try:
            if not self._ready:
                self._cond.wait(timeout)
        finally:
            self._cond.release()

    def get(self, timeout=None):
        self.wait(timeout)  # 等待直到 timeout
        if not self._ready:
            raise TimeoutError
        if self._success:
            return self._value
        else:
            raise self._value

    """
    print taskseq  # (0, None, (True, 25))
    job, i, obj = taskseq
    cache[job]._set(i, obj)
    """
    def _set(self, i, obj):
        self._success, self._value = obj
        if self._callback and self._success:
            self._callback(self._value)
        self._cond.acquire()
        try:
            self._ready = True
            self._cond.notify()
        finally:
            self._cond.release()
        del self._cache[self._job]

def sqr(x, wait=0.0):
    time.sleep(wait)
    return x*x


if __name__ == '__main__':
    p = Pool(2)
    print p
    result = p.apply(sqr, (5,))
    print result
