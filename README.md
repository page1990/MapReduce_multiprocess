# MapReduce_multiprocess
#让你的python代码通过多进程使用MapReduce
经常写代码的同学肯定会有大批量的数据处理的问题，在python中，使用多线程+队列的方式或者使用celery这个分布式的框架通常是一个比较常用的方案。
前几天看到一个老外写的文章，里面提到了使用python的multiprocessing来使用MapReduce，这样子也能使你的程序支持并发，并且简单快速。

##原理
使用多进程的pool来进行MapReduce，先看下面的代码:
```
import itertools
import multiprocessing


class SimpleMapReduce(object):

    def __init__(self, map_func, reduce_func, num_workers=None):
        """
        map_func

          Function to map inputs to intermediate data. Takes as
          argument one input value and returns a tuple with the key
          and a value to be reduced.

        reduce_func

          Function to reduce partitioned version of intermediate data
          to final output. Takes as argument a key as produced by
          map_func and a sequence of the values associated with that
          key.

        num_workers

          The number of workers to create in the pool. Defaults to the
          number of CPUs available on the current host.
        """
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)

    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        mapped_values format:
        [(k, v), (k, v), (k, v)]
        """
        partitioned_data = {}
        for key, value in mapped_values:
            partitioned_data[key] = value

        return partitioned_data.items()

    def __call__(self, inputs, chunksize=1):
        """Process the inputs through the map and reduce functions given.

        inputs
          An iterable containing the input data to be processed.

        chunksize=1
          The portion of the input data to hand to each worker.  This
          can be used to tune performance during the mapping phase.
        map_responses format:
        [[(), (), (),....,()]]
        partitioned_data format:
        [(), (),.....,()]
        """
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values
```

要理解下面的几个概念:
+ chunksize: 每次从pool中取的数量
+ chain: 经典的python方法，可以把某些可迭代的对象全部扩展起来，成为一个更大的迭代对象

MapReduce，顾名思义，先map(映射)，然后reduce(归纳)。因此这里需要传递这两个函数。当然，这里其实是已经把队列的细节实现封装起来了，用兴趣的话可以去看看队列的实现(推荐这么做)

如何使用？
首先我们要明确一点，哪些东西是可以分隔的,比如下面的需求，我可以把所有的文件全部分隔为一个个的文件，那么对每个文件的操作（这里就是计算大小）就是一个map方法。同时，计算好了文件大小后，我们还要通过一定的数据格式把他归纳出来，这里就是reduce。

##需求
假设我要查看linux某个目录下面的前20个最大的文件(当然这里这是个简单的例子，实际中只要使用```dh -h```的命令就能出来)。解决问题的思路是使用多进程，每个进程拿取一个文件，通过Map方法，来计算文件的大小，然后Reduce方法来统计。

首先创建一些文件:
```
cd /tmp

for i in {1..20};
do
    dd if=/dev/zero of=./${i}.rst bs=${i}M count=1
done
```

查看刚才创建的这些文件
```
➜  /tmp ll *.rst
-rw-r--r--. 1 root root  10M 2016-09-20 19:29 10.rst
-rw-r--r--. 1 root root  11M 2016-09-20 19:29 11.rst
-rw-r--r--. 1 root root  12M 2016-09-20 19:29 12.rst
-rw-r--r--. 1 root root  13M 2016-09-20 19:29 13.rst
-rw-r--r--. 1 root root  14M 2016-09-20 19:29 14.rst
-rw-r--r--. 1 root root  15M 2016-09-20 19:29 15.rst
-rw-r--r--. 1 root root  16M 2016-09-20 19:29 16.rst
-rw-r--r--. 1 root root  17M 2016-09-20 19:29 17.rst
-rw-r--r--. 1 root root  18M 2016-09-20 19:29 18.rst
-rw-r--r--. 1 root root  19M 2016-09-20 19:29 19.rst
-rw-r--r--. 1 root root 1.0M 2016-09-20 19:29 1.rst
-rw-r--r--. 1 root root  20M 2016-09-20 19:29 20.rst
-rw-r--r--. 1 root root 2.0M 2016-09-20 19:29 2.rst
-rw-r--r--. 1 root root 3.0M 2016-09-20 19:29 3.rst
-rw-r--r--. 1 root root 4.0M 2016-09-20 19:29 4.rst
-rw-r--r--. 1 root root 5.0M 2016-09-20 19:29 5.rst
-rw-r--r--. 1 root root 6.0M 2016-09-20 19:29 6.rst
-rw-r--r--. 1 root root 7.0M 2016-09-20 19:29 7.rst
-rw-r--r--. 1 root root 8.0M 2016-09-20 19:29 8.rst
-rw-r--r--. 1 root root 9.0M 2016-09-20 19:29 9.rst
```

执行结果如下:
```
PoolWorker-1 reading /tmp/18.rst
PoolWorker-1 reading /tmp/19.rst
PoolWorker-1 reading /tmp/10.rst
PoolWorker-1 reading /tmp/8.rst
PoolWorker-1 reading /tmp/4.rst
PoolWorker-1 reading /tmp/12.rst
PoolWorker-1 reading /tmp/13.rst
PoolWorker-1 reading /tmp/6.rst
PoolWorker-1 reading /tmp/2.rst
PoolWorker-1 reading /tmp/15.rst
PoolWorker-1 reading /tmp/5.rst
PoolWorker-1 reading /tmp/9.rst
PoolWorker-1 reading /tmp/17.rst
PoolWorker-1 reading /tmp/1.rst
PoolWorker-1 reading /tmp/11.rst
PoolWorker-1 reading /tmp/7.rst
PoolWorker-1 reading /tmp/16.rst
PoolWorker-1 reading /tmp/14.rst
PoolWorker-1 reading /tmp/20.rst
PoolWorker-1 reading /tmp/3.rst
/tmp/20.rst:        20
/tmp/19.rst:        19
/tmp/18.rst:        18
/tmp/17.rst:        17
/tmp/16.rst:        16
/tmp/15.rst:        15
/tmp/14.rst:        14
/tmp/13.rst:        13
/tmp/12.rst:        12
/tmp/11.rst:        11
```


<a href="https://pymotw.com/2/multiprocessing/mapreduce.html" target="_blank">原文链接</a>

参考文件：
    <a href="https://en.wikipedia.org/wiki/MapReduce" target="_blank">MapReduce</a>
