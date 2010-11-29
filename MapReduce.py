#!/usr/bin/env python
# -*- coding: utf-8 -*-


from __future__ import with_statement

import threading
import Queue
import operator


class SynchronizedDict(dict):
    def __init__(self, *args, **kwargs):
        super(SynchronizedDict, self).__init__(*args, **kwargs)
        self.lock = threading.Lock()


    def __getitem__(self, k):
        with self.lock:
            return super(SynchronizedDict, self).__getitem__(k)


    def __setitem__(self, k, v):
        with self.lock:
            return super(SynchronizedDict, self).__setitem__(k, v)


    def __contains__(self, k):
        with self.lock:
            return super(SynchronizedDict, self).__contains__(k)


    def get(self, k, default=None):
        with self.lock:
            return super(SynchronizedDict, self).get(k, default=default)


    def set_append(self, k, v):
        with self.lock:
            if super(SynchronizedDict, self).__contains__(k):
                super(SynchronizedDict, self).__setitem__(k, super(SynchronizedDict, self).__getitem__(k) + [v, ])
            else:
                super(SynchronizedDict, self).__setitem__(k, [v, ])


    def items(self):
        with self.lock:
            return super(SynchronizedDict, self).items()



class MapReduce(object):
    def __init__(self):
        self.worksNum = 1
        self.data = None
        self.sd = SynchronizedDict()


    def parse(self):
        pass


    def map(self, k, v):
        """
        (k, v) --> [(k1, v1), (k2, v2), ...]
        """
        pass


    def __merge(self, k, v):
        self.sd.set_append(k, v)


    def reduce(self, k, l):
        """
        (k, (v1, v2, ...)) --> (k, list(v1, v2, ...))
        """
        pass


    def process_queue(self, inputQueue, selector):
        outputQueue = Queue.Queue()

        def worker():
            while not inputQueue.empty():
                k, v = inputQueue.get()

                if selector in ("map", "reduce"):
                    if selector == "map":
                        returnList = self.map(k, v)
                        for resultTuple in returnList:
                            outputQueue.put(resultTuple)
                    else:
                        outputQueue.put(self.reduce(k, v))
                elif selector == "merge":
                    self.__merge(k, v)
                else:
                    raise Exception("Base selector: %s" % selector)

                inputQueue.task_done()

        for i in range(self.worksNum):
            workerThread = threading.Thread(target=worker)
            workerThread.setDaemon(True)
            workerThread.start()

        # wait for worker threads to finish
        inputQueue.join()

        if selector == "merge":
            outputList = sorted(self.sd.items(), key=operator.itemgetter(0))
            for v in outputList:
                outputQueue.put(v)

        return outputQueue


    def run(self):
        inputQueue = self.parse()

        for selector in ("map", "merge", "reduce"):
            inputQueue = self.process_queue(inputQueue, selector)

        return inputQueue



class WordCount(MapReduce):
    def __init__(self):
        super(WordCount, self).__init__()


    def parse(self):
        f = open(self.data)
        q = Queue.Queue()

        for line in f.readlines():
            q.put((line, None))

        f.close()

        return q


    def map(self, k, v):
        words = []
        for w in k.split():
            words.append((w, 1))

        return words


    def reduce(self, k, l):
        return (k, sum(l))


if __name__ == "__main__":
    wc = WordCount()
    wc.data = "zenOfPython"
    q = wc.run()
    
    while not q.empty():
        print "%s: %s" % q.get()
