import sys
from operator import add
from pyspark.sql import SparkSession
from datetime import datetime
from itertools import islice


def count(lines):
    temp = int(sys.argv[2])
    date = bool(sys.argv[3])
    counts = lines.map(lambda x: x.strip().split(',')[temp])
    if date:
        counts = counts.map(lambda x: datetime.fromtimestamp(int(x) / 1000).month)
    counts = counts.map(lambda x: (x, 1)) \
        .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PythonWordCount") \
        .getOrCreate()
    beijing_code = [110101, 110102, 110105, 110106, 110107, 110108, 110111, 110112, 110114, 110115]
    beijing_code = [str(i) for i in beijing_code]
    test = spark.read.csv(sys.argv[1])
    lines = spark.read.text(sys.argv[1]).rdd.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it).map(lambda r: r[0]).filter(
        lambda x: datetime.fromtimestamp(int(x.strip().split(',')[5]) / 1000).month in [11, 1, 2] and
                  x.strip().split(',')[0] in beijing_code)
    lines.saveAsTextFile("file:///a")
    # count(lines)
    spark.stop()
