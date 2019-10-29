
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def sortrecord(rdd):
    l = rdd.sortBy(lambda x:x[1],ascending=False).collect()[:3]
    l = list(map(lambda x:x[0],l))
    print(*l,sep=',')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: task2.py <Windows size> <batch duration>", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="ThreeMostCommonHashtags")
    ssc = StreamingContext(sc, int(sys.argv[2]))

    lines = ssc.socketTextStream('localhost', 9009)
    lines=lines.window(int(sys.argv[1]),slideDuration=None)
    counts = lines.map(lambda line: line.split(';')[7])\
                  .flatMap(lambda x:x.split(','))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)
    counts.foreachRDD(sortrecord)
    #counts.pprint(3)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
