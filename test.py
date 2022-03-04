import geohash2
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator

from utils import *
import re
import csv
import pandas
from datetime import datetime
from os import path
from pyspark.sql import SparkSession

from geospark.register import upload_jars
from geospark.register import GeoSparkRegistrator


# fn = "bj_fishnet1km"
# fn = "bj_fishnet500km"
# fn = "Sum20200122with1km"
# sf = shapefile.Reader("C:/Users/Administrator/Desktop/scala/resources/building_contour/building_countour_subset.shp")
# sf = shapefile.Reader("C:/Users/Administrator/Desktop/" + fn + "/" + fn)
# shapes = sf.shapes()
# info = sf.records()
# for i in info:
#     if i["Join_Count"] != 0:
#         print(i)
# print(shapes[1].parts)
# print(len(shapes))


def take_lines(chunk):
    y1 = datetime.utcfromtimestamp(chunk["sysTime"].iloc[0] / 1000).year
    y2 = datetime.utcfromtimestamp(chunk["sysTime"].iloc[-1] / 1000).year
    if y1 == y2:
        chunk.to_csv(str(y1) + ".csv", header=not path.exists(str(y1) + ".csv"), index=False, mode="a+")
    else:
        mid = chunk.shape[0] // 2
        take_lines(chunk.iloc[:mid, :])
        take_lines(chunk.iloc[mid:, :])


def swap(fn):
    with open(fn, "r") as f:
        with open(fn + "_modif", "w+") as f2:
            for line in f:
                z = line[:-1].split(',')
                f2.write(z[1] + ',' + z[0] + "\n")


def rectangles(pos):
    code = geohash2.encode(pos[1], pos[0], 8)
    [x, y, dx, dy] = geohash2.decode_exactly(code)
    return [y + dy, x + dx, y - dy, x - dx]


def write_rectangles(f_in, f_out):
    with open(f_in, 'r') as f1:
        with open(f_out, 'w+') as f2:
            for line in f1:
                temp = re.split(",|\t", line[:-1])
                pos = list(map(lambda x: float(x), temp))
                rect = rectangles(pos)
                f2.write(','.join(map(str, rect)) + '\n')


def write_rectangles2(f_in, f_out):
    with open(f_out, 'w+') as f2:
        for i in f_in:
            [x, y, dx, dy] = geohash2.decode_exactly(i)
            rect = [y + dy, x + dx, y - dy, x - dx]
            f2.write(','.join(map(str, rect)) + '\n')


def read_stations(fn):
    with open(fn, "r", encoding="utf-8") as f:
        with open(fn + "sorted", "w+", encoding="utf-8") as f1:
            reader = csv.reader(f)
            header_row = next(reader)
            for row in reader:
                if row[3] == "公交车站相关" or row[3] == "地铁站":
                    f1.write(",".join(row) + "\n")


if __name__ == "__main__":
    # x = 1
    # chunksize = 5
    # for chunk in pandas.read_csv("points.csv", chunksize=chunksize):
    #     take_lines(chunk)
    # fn = "C:/Users/Administrator/Desktop/b_code_stats2.csv"
    # fn = "C:/Users/Administrator/Desktop/ShareBikes/b_code_stats.txt"
    # swap("C:/Users/Administrator/Desktop/b_code_stats1.csv")
    # write_rectangles(fn, fn + "pos")
    # x= register_area(7)
    # write_rectangles2(x, fn + "pos")
    # fn = "C:/Users/Administrator/Desktop/transport2.csv"
    # read_stations(fn)
    # upload_jars()

    spark = SparkSession.builder \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) \
        .getOrCreate()

    GeoSparkRegistrator.registerAll(spark)

    counties = spark. \
        read. \
        option("delimiter", "|"). \
        option("header", "true"). \
        csv("counties.csv")

    counties.createOrReplaceTempView("county")
    counties.show()
    counties_geom = spark.sql(
        "SELECT county_code, st_geomFromWKT(geom) as geometry from county"
    )
    counties.createOrReplaceTempView("county")
    counties.show()
