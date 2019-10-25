
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: task2.py <Windows size> <batch duration>", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="ThreeMostCommonHashtags")
    ssc = StreamingContext(sc, int(sys.argv[2]))

    lines = ssc.socketTextStream('localhost', 9009)#.window(sys.argv[1])
    counts = lines.map(lambda line: line.split(';')[7])\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)\
                  #.collect() transformedDstream has no attribute collect
                  #.sortByValues()\   has no attribute sortbyvalues!
    counts.pprint(3)

    ssc.start()
    ssc.awaitTermination()
