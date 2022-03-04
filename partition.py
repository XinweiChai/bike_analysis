import os

import geohash2
from pyproj import Proj, transform
from geospark.core.enums import GridType, IndexType
from geospark.core.formatMapper.shapefileParser import ShapefileReader
from geospark.core.spatialOperator import JoinQuery
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator
from geospark.utils.adapter import Adapter
from geospark.core import SpatialRDD
from geospark.core.SpatialRDD import CircleRDD, PointRDD
from geospark.core.geom.envelope import Envelope
import datetime
from pyspark.sql import SparkSession
from geospark.register import upload_jars
from geospark.register import GeoSparkRegistrator
from colocation import load_properties
from shapely.geometry.polygon import Polygon
from pyspark.sql.functions import udf

resPath = os.getcwd() + '/'
configPath = "conf/"
joinQueryPartitioningType = GridType.QUADTREE
prop = load_properties(configPath + "colo.point.properties")

pointInputLocation = resPath + prop["inputLocation"]
pointLatOffset = int(prop["latOffset"])
pointLonOffset = int(prop["lonOffset"])
pointOffset = int(prop["offset"])
pointIndexType = IndexType.RTREE
fromTime = prop["fromTime"]
toTime = prop["toTime"]
start = datetime.datetime.strptime(fromTime, "%Y-%m-%dT%H:%M:%S")
end = datetime.datetime.strptime(toTime, "%Y-%m-%dT%H:%M:%S")

prop = load_properties(configPath + "colo.poi.properties")
poiInputLocation = resPath + prop["inputLocation"]
poiInputLocation2 = resPath + prop["inputLocation2"]

prop = load_properties(configPath + "colo.polygon.properties")
polygonInputLocation = resPath + prop["inputLocation"]

prop = load_properties(configPath + "colo.linestring.properties")
lineStringInputLocation = resPath + prop["inputLocation"]

prop = load_properties(configPath + "colo.run.properties")
outputMode = int(prop["outputMode"])
analysis = prop["analysis"]
outputPath = prop["outputPath"]
correspondingTable = bool(prop["correspondingTable"])
firstX = int(prop["firstX"])
epsg3857 = prop["epsg3857"] == "true"
radius = float(prop["radius"])
radius2 = float(prop["radius2"])


def elements_in_geohash_grid(filename, precision):
    df = spark.read.csv(filename, header=True, inferSchema=True)
    df.createOrReplaceTempView("df")

    def encode(lat, lon):
        temp = geohash2.encode(lat, lon, precision=precision)
        # lat, lon, lat_err, lon_err = geohash2.decode_exactly(temp)
        # inProj = Proj(init='epsg:4326')
        # outProj = Proj(init='epsg:3857')
        # x1, y1 = transform(inProj, outProj, lon - lon_err, lat - lat_err)
        # x2, y2 = transform(inProj, outProj, lon + lon_err, lat + lat_err)
        # return str([x1, x2, y1, y2])
        # return str([lat - lat_err, lat + lat_err, lon - lon_err, lon + lon_err])
        return temp

    def trans_time(t):
        return datetime.datetime.fromtimestamp(t / 1000).strftime('%Y-%m-%d-%H')

    df = spark.sql(f"SELECT * FROM df WHERE sysTime > UNIX_TIMESTAMP('{start.strftime('%Y-%m-%d')}','yyyy-MM-dd') * 1000 AND sysTime < UNIX_TIMESTAMP('{end.strftime('%Y-%m-%d')}','yyyy-MM-dd') * 1000")
    # df.createOrReplaceTempView("df")
    # df.show()
    df = df.withColumn("date", udf(trans_time)("sysTime"))
    df = df.withColumn("envelope", udf(encode)("lat", "lng"))
    df.show()
    x = df.groupBy("envelope", "date").count()
    x.write.option("header", "True").mode("overwrite").partitionBy("date").csv(outputPath)


def elements_in_partition(filename):
    spatialDf = spark.read.format("csv").option("delimiter", ",").option("header", "True").load(filename)
    spatialDf.createOrReplaceTempView("spatialDf")
    points = Adapter.toSpatialRdd(spark.sql(
        f"select ST_Point(cast(lat as Decimal(24, 14)), cast(lng as Decimal(24, 14))) from spatialDf"))
    points.analyze()
    points.spatialPartitioning(partitioning=GridType.EQUALGRID)
    points.buildIndex(IndexType.QUADTREE, buildIndexOnSpatialPartitionedRDD=False)

    def f(num_partition, iterator): yield [num_partition, sum(iterator)]

    k = points.jvmSpatialPartitionedRDD.jsrdd.count()  # .mapPartitionsWithIndex(f)
    print(k)
    return 0


if __name__ == "__main__":
    spark = SparkSession.builder \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) \
        .appName("Colocation") \
        .getOrCreate()

    GeoSparkRegistrator.registerAll(spark)
    # rdd = spark.sparkContext.parallelize([1, 2, 3, 4], 4)
    # def f(splitIndex, iterat):
    #     yield [splitIndex, sum(iterat)]

    # x = rdd.mapPartitionsWithIndex(f).collect()
    # print(x)
    # elements_in_partition(pointInputLocation)
    elements_in_geohash_grid(pointInputLocation, precision=8)

    print("output: " + outputPath)
    print("Tasks finished")
    spark.stop()
