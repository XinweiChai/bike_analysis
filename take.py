import sys
from operator import add
from pyspark.sql import SparkSession, types
from datetime import datetime
from itertools import islice
from pyspark.sql.functions import col, from_unixtime

beijing_code = [str(i) for i in
                [110101, 110102, 110105, 110106, 110107, 110108, 110111, 110112, 110114, 110115, 110120]]

spring = [3, 4, 5]
summer = [6, 7, 8]
autumn = [9, 10, 11]
winter = [12, 1, 2]


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


def read_file(fn):
    # return spark.read.text(sys.argv[1]).rdd.mapPartitionsWithIndex(
    #     lambda idx, it: islice(it, 1, None) if idx == 0 else it).map(lambda r: r[0]).flatMap(
    #     lambda x: x.strip().split(','))
    return spark.read.option("header", "true").format("csv").load(fn)


def condition(string):
    tmp = string
    return int(tmp[5]) != 0 and datetime.fromtimestamp(int(tmp[5]) / 1000).month in winter and tmp[0] in beijing_code


def filter_winter(fn):
    read_file(fn).rdd.filter(lambda x: int(x.positionTime) != 0 and datetime.fromtimestamp(
        int(x.positionTime) / 1000).month in winter and x.areaCode in beijing_code).toDF().write.mode(
        saveMode="overwrite").csv("out", header=True)


def regroup_data(fn):
    x = read_file(fn)
    # x = x.select(from_unixtime(col("positionTime") / 1000, 'yyyy-MM-dd'))
    x.createOrReplaceTempView("spatialDf")
    # x = spark.sql("SELECT *, from_unixtime(sysTime / 1000, 'yyyy-MM-dd,HH') AS date, from_unixtime(sysTime / 1000, 'HH:mm:ss') AS time FROM spatialDf")
    # x = spark.sql("SELECT *, from_unixtime(sysTime / 1000, 'yyyy') AS year FROM spatialDf")
    x = spark.sql("SELECT *, month(from_unixtime(sysTime / 1000, 'yyyy-MM-dd')) AS month FROM spatialDf")
    x.show()
    # x.write.partitionBy("date").mode("overwrite").csv("out")
    x.write.partitionBy("month").mode("overwrite").csv("out")


def group_by(fn):
    x = read_file(fn)
    x.createOrReplaceTempView("spatialDf")
    # x = spark.sql("SELECT areaCode, companyCode, COUNT(areaCode) as count FROM spatialDf GROUP BY areaCode, companyCode")
    # x.createOrReplaceTempView("spatialDf")
    x = spark.sql("SELECT from_unixtime(_c5 / 1000, 'yyyy-MM-dd-HH:mm:ss') AS date FROM spatialDf")
    x.createOrReplaceTempView("spatialDf")
    x = spark.sql("SELECT date, COUNT(date) as count FROM spatialDf WHERE date<='2017-11-16' GROUP BY date").sort(
        "date")
    x.createOrReplaceTempView("spatialDf")
    x.show()
    x.write.mode("overwrite").csv("out")


def sql(fn):
    x = read_file(fn)
    x.createOrReplaceTempView("spatialDf")
    x = spark.sql(
        "SELECT year(date) as year, month(date) as month, SUM(cast(num_bikes as int)) FROM spatialDf GROUP BY year(date), month(date)").sort(
        ["year", "month"])
    x.createOrReplaceTempView("spatialDf")
    x.show()
    x.write.mode("overwrite").csv("out", sep='\t')


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PythonCount") \
        .getOrCreate()
    # regroup_data(sys.argv[1])
    # sql()
    # group_by(sys.argv[1])
    # count(lines)
    # filter_winter(sys.argv[1])
    spark.stop()
