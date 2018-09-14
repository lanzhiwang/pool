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

def mapstar(args):
    return map(*args)

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

    completed = 0
    while maxtasks is None or (maxtasks and completed < maxtasks):
        task = inqueue.get()
        print 'worker process: ', task
        """
        apply
        (0, None, <function sqr at 0x7ff5f6f999b0>, (5,), {})

        map
        (0, 1, <function mapstar at 0x7facbec610c8>, ((<function sqr at 0x7facbcf829b0>, (2, 3)),), {})
        """
        job, i, func, args, kwds = task
        result = (True, func(*args, **kwds))
        print 'worker process: ', result
        """
        apply
        (True, 25)
        (job, i, (True, 25))

        map
        (True, [4, 9])
        (job, i, (True, [4, 9]))
        """
        outqueue.put((job, i, result))
        completed += 1


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

        """
        apply task 结构
        ([(0, None, <function sqr at 0x7f1fabd7c938>, (5,), {})], None)

        map task 结构
        (<generator object <genexpr> at 0x7fc0970d65f0>, None)
        (0, 0, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (0, 1)),), {})
        (0, 1, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (2, 3)),), {})
        (0, 2, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (4, 5)),), {})
        (0, 3, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (6, 7)),), {})
        (0, 4, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (8, 9)),), {})

        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        [(0, 1), (2, 3), (4, 5), (6, 7), (8, 9)]
        [(function, (0, 1)), (function, (2, 3)), (function, (4, 5)), (function, (6, 7)), (function, (8, 9))]
        [         0,                  1,                  2,                  3,                  4        ]

        """

        # taskseq = taskqueue.get()
        # print 'thread handle_tasks: ', taskseq
        # task = taskseq[0][0]
        # print 'thread handle_tasks: ', task  # (0, None, <function sqr at 0x7f8aace37938>, (5,), {})
        # put(task)

        for taskseq, set_length in iter(taskqueue.get, None):
            print 'thread handle_tasks: ', taskseq, set_length
            """
            apply
            [(0, None, <function sqr at 0x7f59caab59b0>, (5,), {})] None

            map
            <generator object <genexpr> at 0x7ff856193870> None

            imap
            <generator object <genexpr> at 0x7fc8683cf8c0> <bound method IMapIterator._set_length of <__main__.IMapIterator object at 0x7fc8683c1f10>>
            """
            for i, task in enumerate(taskseq):
                print 'enumerate thread handle_tasks: ', i, task
                """
                apply
                0 (0, None, <function sqr at 0x7f83e68279b0>, (5,), {})

                map
                0 (0, 0, <function mapstar at 0x7ff857ec20c8>, ((<function sqr at 0x7ff8561e39b0>, (0, 1)),), {})
                1 (0, 1, <function mapstar at 0x7ff857ec20c8>, ((<function sqr at 0x7ff8561e39b0>, (2, 3)),), {})
                2 (0, 2, <function mapstar at 0x7ff857ec20c8>, ((<function sqr at 0x7ff8561e39b0>, (4, 5)),), {})
                3 (0, 3, <function mapstar at 0x7ff857ec20c8>, ((<function sqr at 0x7ff8561e39b0>, (6, 7)),), {})
                4 (0, 4, <function mapstar at 0x7ff857ec20c8>, ((<function sqr at 0x7ff8561e39b0>, (8, 9)),), {})

                imap
                0 (0, 0, <function sqr at 0x7fc86841f9b0>, (0,), {})
                1 (0, 1, <function sqr at 0x7fc86841f9b0>, (1,), {})
                """
                put(task)
            else:
                if set_length:
                    print 'set_length thread handle_tasks: ', i  # 9
                    set_length(i+1)

    @staticmethod
    def _handle_results(outqueue, get, cache):
        """
        outqueue: 用于分发存储任务的执行结果队列
        get: _inqueue 队列 put 方法，其中 _inqueue 用于分发存储内部结构化的任务
        cache: Pool 实例和 ApplyResult 实例共享数据，用于存储任务以及任务结果

        args=(self._outqueue, self._quick_get, self._cache)
        """

        while 1:
            taskseq = outqueue.get()
            print 'handle_results: ', taskseq
            """
            apply
            (0, None, (True, 25))

            map
            (0, 1, (True, [4, 9]))

            imap
            (0, 0, (True, 0))
            (0, 2, (True, 4))
            (0, 1, (True, 1))
            (0, 3, (True, 9))
            (0, 5, (True, 25))
            (0, 4, (True, 16))
            (0, 6, (True, 36))
            (0, 7, (True, 49))
            (0, 8, (True, 64))
            (0, 9, (True, 81))
            """
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
        '''
        Equivalent of `map()` builtin
        '''
        assert self._state == RUN
        return self.map_async(func, iterable, chunksize).get()

    def imap(self, func, iterable, chunksize=1):
        '''
        Equivalent of `itertools.imap()` -- can be MUCH slower than `Pool.map()`
        '''
        assert self._state == RUN
        if chunksize == 1:
            result = IMapIterator(self._cache)
            # generator_task = ((result._job, i, func, (x,), {}) for i, x in enumerate(iterable))
            # for gene_task in generator_task:
            #     print gene_task
            #     """
            #     (0, 0, <function sqr at 0x7ff37a4289b0>, (0,), {})
            #     (0, 1, <function sqr at 0x7ff37a4289b0>, (1,), {})
            #     (0, 2, <function sqr at 0x7ff37a4289b0>, (2,), {})
            #     (0, 3, <function sqr at 0x7ff37a4289b0>, (3,), {})
            #     (0, 4, <function sqr at 0x7ff37a4289b0>, (4,), {})
            #     (0, 5, <function sqr at 0x7ff37a4289b0>, (5,), {})
            #     (0, 6, <function sqr at 0x7ff37a4289b0>, (6,), {})
            #     (0, 7, <function sqr at 0x7ff37a4289b0>, (7,), {})
            #     (0, 8, <function sqr at 0x7ff37a4289b0>, (8,), {})
            #     (0, 9, <function sqr at 0x7ff37a4289b0>, (9,), {})
            #     """
            task = (((result._job, i, func, (x,), {}) for i, x in enumerate(iterable)), result._set_length)
            print task  # (<generator object <genexpr> at 0x7f2c7fd638c0>, <bound method IMapIterator._set_length of <__main__.IMapIterator object at 0x7f2c7fd55f10>>)
            """
            (
            <generator object <genexpr> at 0x7f2c7fd638c0>,
            <bound method IMapIterator._set_length of <__main__.IMapIterator object at 0x7f2c7fd55f10>>
            )
            """
            print type(task)  # <type 'tuple'>
            self._taskqueue.put(task)
            return result
        else:
            assert chunksize > 1
            task_batches = Pool._get_tasks(func, iterable, chunksize)
            result = IMapIterator(self._cache)
            self._taskqueue.put((((result._job, i, mapstar, (x,), {})
                     for i, x in enumerate(task_batches)), result._set_length))
            return (item for chunk in result for item in chunk)

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
        '''
        Asynchronous equivalent of `map()` builtin
        '''
        assert self._state == RUN
        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        if chunksize is None:
            chunksize, extra = divmod(len(iterable), len(self._pool) * 4)
            if extra:
                chunksize += 1
        if len(iterable) == 0:
            chunksize = 0

        task_batches = Pool._get_tasks(func, iterable, chunksize)
        # print task_batches
        result = MapResult(self._cache, chunksize, len(iterable), callback)
        # generator_task = ((result._job, i, mapstar, (x,), {}) for i, x in enumerate(task_batches))
        # for gene_task in generator_task:
        #     print gene_task
        #     """
        #     (0, 0, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (0, 1)),), {})
        #     (0, 1, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (2, 3)),), {})
        #     (0, 2, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (4, 5)),), {})
        #     (0, 3, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (6, 7)),), {})
        #     (0, 4, <function mapstar at 0x7f6f718070c8>, ((<function sqr at 0x7f6f6fb289b0>, (8, 9)),), {})
        #     """
        task = (((result._job, i, mapstar, (x,), {}) for i, x in enumerate(task_batches)), None)
        print task  # (<generator object <genexpr> at 0x7fc0970d65f0>, None)
        print type(task)  # <type 'tuple'>
        self._taskqueue.put(task)
        return result


    def close(self):
        pass

    def terminate(self):
        pass

    def join(self):
        pass

    @staticmethod
    def _get_tasks(func, it, size):
        it = iter(it)
        while 1:
            x = tuple(itertools.islice(it, size))
            if not x:
                return
            yield (func, x)

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


AsyncResult = ApplyResult       # create alias -- see #17805

#
# Class whose instances are returned by `Pool.map_async()`
#

class MapResult(ApplyResult):

    def __init__(self, cache, chunksize, length, callback):
        ApplyResult.__init__(self, cache, callback)
        self._success = True
        self._value = [None] * length
        self._chunksize = chunksize
        if chunksize <= 0:
            self._number_left = 0
            self._ready = True
            del cache[self._job]
        else:
            self._number_left = length//chunksize + bool(length % chunksize)
        print 'self._success:  {} type: {}' . format(self._success, type(self._success))
        print 'self._value:  {} type: {}' . format(self._value, type(self._value))
        print 'self._chunksize:  {} type: {}' . format(self._chunksize, type(self._chunksize))
        print 'self._number_left:  {} type: {}' . format(self._number_left, type(self._number_left))
        print 'self._ready:  {} type: {}' . format(self._ready, type(self._ready))
        """
        self._success:  True type: <type 'bool'>
        self._value:  [None, None, None, None, None, None, None, None, None, None] type: <type 'list'>
        self._chunksize:  2 type: <type 'int'>
        self._number_left:  5 type: <type 'int'>
        self._ready:  False type: <type 'bool'>
        """

    def _set(self, i, success_result):
        success, result = success_result
        print 'success:  {} type: {}' . format(success, type(success))
        print 'result:  {} type: {}' . format(result, type(result))
        """
        success:  True type: <type 'bool'>
        result:  [0, 1] type: <type 'list'>
        """
        if success:
            self._value[i*self._chunksize:(i+1)*self._chunksize] = result
            print 'self._value:  {} type: {}' . format(self._value, type(self._value))
            # self._value:  [0, 1, None, None, None, None, None, None, None, None] type: <type 'list'>
            self._number_left -= 1
            if self._number_left == 0:
                if self._callback:
                    self._callback(self._value)
                del self._cache[self._job]
                self._cond.acquire()
                try:
                    self._ready = True
                    self._cond.notify()
                finally:
                    self._cond.release()

        else:
            self._success = False
            self._value = result
            del self._cache[self._job]
            self._cond.acquire()
            try:
                self._ready = True
                self._cond.notify()
            finally:
                self._cond.release()

class IMapIterator(object):

    def __init__(self, cache):
        self._cond = threading.Condition(threading.Lock())
        self._job = job_counter.next()
        self._cache = cache
        self._items = collections.deque()
        self._index = 0
        self._length = None
        self._unsorted = {}
        cache[self._job] = self
        # print 'self._cond:  {} type: {}' . format(self._cond, type(self._cond))
        # print 'self._job:  {} type: {}' . format(self._job, type(self._job))
        # print 'self._cache:  {} type: {}' . format(self._cache, type(self._cache))
        # print 'self._items:  {} type: {}' . format(self._items, type(self._items))
        # print 'self._index:  {} type: {}' . format(self._index, type(self._index))
        # print 'self._length:  {} type: {}' . format(self._length, type(self._length))
        # print 'self._unsorted:  {} type: {}' . format(self._unsorted, type(self._unsorted))
        # print cache
        """
        self._cond:  <Condition(<thread.lock object at 0x7fb628356390>, 0)> type: <class 'threading._Condition'>
        self._job:  0 type: <type 'int'>
        self._cache:  {0: <__main__.IMapIterator object at 0x7fb62656d0d0>} type: <type 'dict'>
        self._items:  deque([]) type: <type 'collections.deque'>
        self._index:  0 type: <type 'int'>
        self._length:  None type: <type 'NoneType'>
        self._unsorted:  {} type: <type 'dict'>
        {0: <__main__.IMapIterator object at 0x7fb62656d0d0>}
        """

    def __iter__(self):
        return self

    def next(self, timeout=None):
        self._cond.acquire()
        try:
            try:
                item = self._items.popleft()
            except IndexError:
                if self._index == self._length:
                    raise StopIteration
                self._cond.wait(timeout)
                try:
                    item = self._items.popleft()
                except IndexError:
                    if self._index == self._length:
                        raise StopIteration
                    raise TimeoutError
        finally:
            self._cond.release()

        success, value = item
        if success:
            return value
        raise value

    __next__ = next                    # XXX

    def _set(self, i, obj):
        """
        0, (True, 0)
        1, (True, 1)
        2, (True, 4)
        3, (True, 9)
        5, (True, 25)
        6, (True, 36)
        4, (True, 16)
        7, (True, 49)
        8, (True, 64)
        9, (True, 81)
        """
        self._cond.acquire()
        try:
            if self._index == i:
                self._items.append(obj)
                self._index += 1
                while self._index in self._unsorted:
                    obj = self._unsorted.pop(self._index)
                    self._items.append(obj)
                    self._index += 1
                self._cond.notify()
            else:
                self._unsorted[i] = obj

            if self._index == self._length:
                del self._cache[self._job]
        finally:
            self._cond.release()

    def _set_length(self, length):
        print 'class IMapIterator _set_length:', length  # 10
        self._cond.acquire()
        try:
            self._length = length
            if self._index == self._length:
                self._cond.notify()
                del self._cache[self._job]
        finally:
            self._cond.release()


def sqr(x, wait=0.0):
    time.sleep(wait)
    return x*x

if __name__ == '__main__':
    p = Pool(2)
    print p
    # result = p.apply(sqr, (5,))
    # result = p.map(sqr, range(10))
    # print result  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

    it = p.imap(sqr, range(10))
    print it  # <__main__.IMapIterator object at 0x7f901081f2d0>
    print type(it)  # <class '__main__.IMapIterator'>
    print list(it)  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

    """it = self.pool.imap(sqr, range(1000), chunksize=100)
    for i in range(1000):
        self.assertEqual(it.next(), i*i)
    self.assertRaises(StopIteration, it.next)"""
