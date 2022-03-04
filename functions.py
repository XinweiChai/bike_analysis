import os
from geospark.core.enums import GridType, IndexType
from geospark.core.formatMapper.shapefileParser import ShapefileReader
from geospark.core.spatialOperator import JoinQuery
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator
from geospark.utils.adapter import Adapter
from geospark.core import SpatialRDD
from geospark.core.SpatialRDD import CircleRDD
import datetime
from pyspark.sql import SparkSession
# from geospark.register import upload_jars
from geospark.register import GeoSparkRegistrator
from colocation import write_to_file


def readWithDF(filename, spark, epsg3857):
    spatialDf = spark.read.format("csv").option("delimiter", ",").option("header", "True").load(filename)
    spatialDf.createOrReplaceTempView("spatialDf")
    points = SpatialRDD.SpatialRDD()
    points.rawSpatialRDD = Adapter.toSpatialRdd(spark.sql(
        "select ST_Point(cast(spatialDf.lat as Decimal(24, 14)), cast(spatialDf.lng as Decimal(24, 14))) from spatialDf"))
    if epsg3857:
        points.CRSTransform("epsg:4326", "epsg:3857")
    points.analyze()
    return points


def spatialJoinQueryUsingIndex(spark, poi_input_location, point_input_location, epsg3857, joinQueryPartitioningType,
                               pointIndexType, radius):
    pois = readWithDF(poi_input_location, spark, epsg3857)
    pointRDD = readWithDF(point_input_location, spark, epsg3857)
    pointRDD.spatialPartitioning(joinQueryPartitioningType)
    pointRDD.buildIndex(pointIndexType, True)
    queryWindowRDD = CircleRDD(pois, radius)
    queryWindowRDD.analyze()
    queryWindowRDD.spatialPartitioning(pointRDD.getPartitioner())
    result = JoinQuery.SpatialJoinQuery(pointRDD, queryWindowRDD, True, False)
    write_to_file(result)


def distanceJoinQueryUsingIndex(spark, pointInputLocation, lineStringInputLocation, epsg3857, radius,
                                joinQueryPartitioningType, pointIndexType):
    pointRDD = readWithDF(pointInputLocation, spark, epsg3857)
    lineStringRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, lineStringInputLocation)
    rawSpatialDf = Adapter.toDf(lineStringRDD, spark)
    rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
    rawSpatialDf = spark.sql("SELECT ST_GeomFromWKT(geometry) AS geom FROM rawSpatialDf")
    rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
    rawSpatialDf.show()
    spatialDf = spark.sql(f"SELECT ST_Buffer(geom, {radius}) AS buff FROM rawSpatialDf")
    spatialDf.createOrReplaceTempView("spatialDf")
    spatialDf.show()
    # queryWindowRDD = SpatialRDD.SpatialRDD()
    queryWindowRDD = Adapter.toSpatialRdd(spatialDf)
    if epsg3857:
        queryWindowRDD.CRSTransform("epsg:3857", "epsg:3857")
    queryWindowRDD.analyze()
    pointRDD.spatialPartitioning(joinQueryPartitioningType)
    queryWindowRDD.spatialPartitioning(pointRDD.getPartitioner())
    pointRDD.buildIndex(pointIndexType, True)
    result = JoinQuery.SpatialJoinQuery(pointRDD, queryWindowRDD, True, False)
    write_to_file(result)


def distanceJoinQuerySQL(spark, pointInputLocation, lineStringInputLocation, start, end, radius, outputPath):
    pointDf, query = load_point(spark, pointInputLocation, "point", time=True, start=start, end=end)
    pointDf = spark.sql(query)
    pointDf.createOrReplaceTempView("pointDf")
    pointDf.show()
    lineStringRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, lineStringInputLocation)
    lineStringDf = Adapter.toDf(lineStringRDD, spark)
    lineStringDf.createOrReplaceTempView("lineStringDf")
    lineStringDf = spark.sql("SELECT *, ST_GeomFromWKT(geometry) AS geom FROM lineStringDf")
    lineStringDf.createOrReplaceTempView("lineStringDf")
    lineStringDf = spark.sql(f"SELECT *, ST_Buffer(geom, {radius}) AS buff FROM lineStringDf")
    lineStringDf.createOrReplaceTempView("bufferDf")
    distJoin = spark.sql(
        "SELECT ID, date, COUNT(*) as count FROM pointDf, bufferDf WHERE ST_Within(point, buff) GROUP BY ID, date ORDER BY date, ID")
    distJoin.createOrReplaceTempView("distJoinDf")
    distJoin.write.option("header", "True").mode("overwrite").csv(outputPath)


def distanceJoinPoints(spark, pointInputLocation, poiInputLocation, start, end, radius, outputPath):
    pointDf, query = load_point(spark, pointInputLocation, "point", time=True, start=start, end=end)
    pointDf = spark.sql(query)
    pointDf.createOrReplaceTempView("pointDf")
    pointDf = spark.sql(
        "SELECT point, date FROM pointDf WHERE ST_Within(point, ST_Transform(ST_GeomFromText('POLYGON((116.230633649 39.979627788,116.230633649 39.863608449,116.44120325 39.863608449,116.44120325 39.979627788,116.230633649 39.979627788))'),'epsg:4326', 'epsg:3857'))")
    pointDf.show(truncate=False)
    poiDf, query = load_point(spark, poiInputLocation, "poi", time=False)
    poiDf = spark.sql(query)
    poiDf.createOrReplaceTempView("poiDf")
    poiDf.show()
    distJoin = spark.sql(
        f"SELECT date, COUNT(*) as Join_Count FROM pointDf, poiDf WHERE ST_Distance(point, poi) <= {radius} GROUP BY date ORDER BY date")
    distJoin.createOrReplaceTempView("distJoinDf")
    distJoin.write.option("header", "True").mode("overwrite").csv(outputPath)


def doubleJoin(spark, pointInputLocation, poiInputLocation, poiInputLocation2, start, end, radius, radius2, outputPath):
    poiDf, query = load_point(spark, poiInputLocation, "poi", time=False)
    poiDf = spark.sql(query)
    poiDf.createOrReplaceTempView("poiDf")
    poiDf.show()  # normal areas
    poiDf2, query = load_point(spark, poiInputLocation2, "poi2", time=False)
    poiDf2 = spark.sql(query)
    poiDf2.createOrReplaceTempView("poi2Df")
    # distJoinPoi = spark.sql(f"SELECT OBJECTID, poi, date FROM poiDf, poiDf2 WHERE ST_Distance(poi, poi2) <= {radius2}")
    # distJoinPoi.createOrReplaceTempView("poiDf")
    # distJoinPoi.show()
    distJoinPoi = spark.sql(f"SELECT distinct OBJECTID FROM poiDf, poi2Df WHERE ST_Distance(poi, poi2) <= {radius2}")
    distJoinPoi.show()
    print(distJoinPoi.count())
    #        distJoinPoi = spark.sql("SELECT (UNIX_TIMESTAMP(poiDf.date,'yyyy-MM-dd')+864000)*1000 FROM poiDf")
    #        distJoinPoi.show()
    pointDf, query = load_point(spark, pointInputLocation, "point", time=True, start=start, end=end)
    pointDf = spark.sql(query)
    pointDf.createOrReplaceTempView("pointDf")
    pointDf = spark.sql(
        "SELECT point, date, sysTime FROM pointDf WHERE ST_Within(point, ST_Transform(ST_GeomFromText('POLYGON((116.230633649 39.979627788,116.230633649 39.863608449,116.44120325 39.863608449,116.44120325 39.979627788,116.230633649 39.979627788))'),'epsg:4326', 'epsg:3857'))")
    pointDf.createOrReplaceTempView("pointDf")
    pointDf.show(truncate=False)
    distJoin = spark.sql(
        f"SELECT OBJECTID, pointDf.date, COUNT(*) as Join_Count FROM pointDf, poiDf WHERE ST_Distance(point, poi) <= {radius} AND sysTime>= (UNIX_TIMESTAMP(poiDf.date,'yyyy-MM-dd')-864000)*1000 AND sysTime <= (UNIX_TIMESTAMP(poiDf.date,'yyyy-MM-dd')+864000)*1000 GROUP BY OBJECTID, pointDf.date ORDER BY pointDf.date")
    distJoin.createOrReplaceTempView("distJoinDf")
    distJoin.write.option("header", "True").mode("overwrite").csv(outputPath)


def linestringBuffer(spark, lineStringInputLocation, radius, outputPath):
    lineStringRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, lineStringInputLocation)
    rawSpatialDf = Adapter.toDf(lineStringRDD, spark)
    rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
    query = "SELECT ST_GeomFromWKT(geometry) AS geom FROM rawSpatialDf"
    rawSpatialDf = spark.sql(query)
    rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
    spatialDf = spark.sql(f"SELECT ST_Buffer(geom, {radius}) AS buff, geom FROM rawSpatialDf")
    spatialDf.createOrReplaceTempView("spatialDf")
    spatialDf.show()
    # spatialDf.rdd.map(lambda x: [str(x[0]), str(x[1])]).saveAsTextFile(outputPath + analysis + "Result")
    spatialDf.write.mode("overwrite").json(outputPath + "Result")


def pointBuffer(spark, filename, lonOffset, latOffset, outputPath):
    spatialDf = spark.read.format("csv").option("delimiter", ",").option("header", "False").load(filename)
    spatialDf.createOrReplaceTempView("spatialDf")
    spatialDf = spark.sql(
        f"select ST_Point(cast(spatialDf._c{latOffset} as Decimal(24, 18)), cast(spatialDf._c{lonOffset} as Decimal(24, 18))) as geometry, spatialDf._c2 as name from spatialDf")
    spatialDf.createOrReplaceTempView("spatialDf")
    spatialDf = spark.sql(
        "SELECT ST_Transform(geometry, 'epsg:4326','epsg:3857') AS buff, geometry, name FROM spatialDf")
    spatialDf.createOrReplaceTempView("spatialDf")
    spatialDf.rdd.map(lambda x: [str(x[0]), str(x[1])]).saveAsTextFile(outputPath + "Result")
    # spatialDf.rdd.sortBy(lambda x: str(x)).saveAsTextFile(outputPath + analysis + "Result")


def load_point(spark, inputLocation, name, time, start=None, end=None):
    pointDf = spark.read.format("csv").option("delimiter1", ",").option("header", "True").load(inputLocation)
    pointDf.createOrReplaceTempView(name + "Df")
    query = "SELECT sysTime, ST_Transform(ST_Point(cast(lng AS Decimal(24, 14)), cast(lat AS Decimal(24, 14))), " \
            f"'epsg:4326', 'epsg:3857') AS {name}"
    if time:
        query += ",from_unixtime(sysTime/1000,'yyyy-MM-dd') AS date, from_unixtime(sysTime/1000,'HH:mm:ss') AS time "
    query += f"FROM {name + 'Df'} "
    if start:
        query += f" WHERE sysTime > UNIX_TIMESTAMP('{start.strftime('%Y-%m-%d')}','yyyy-MM-dd') * 1000 AND sysTime < UNIX_TIMESTAMP('{end.strftime('%Y-%m-%d')}','yyyy-MM-dd') * 1000"
    return pointDf, query


def join_labeled(spark, pointInputLocation, poiInputLocation, start, end, outputPath):
    poiRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, poiInputLocation)
    poiDf = Adapter.toDf(poiRDD, spark)
    poiDf.createOrReplaceTempView("poiDf")
    poiDf = spark.sql("SELECT ST_GeomFromWKT(geometry) AS geom FROM poiDf")
    poiDf.createOrReplaceTempView("poiDf")
    pointDf, query = load_point(spark, pointInputLocation, "point", time=True, start=start, end=end)
    pointDf = spark.sql(query)
    pointDf.createOrReplaceTempView("pointDf")
    distJoin = spark.sql(
        f"SELECT date, COUNT(*) as Join_Count FROM pointDf, poiDf WHERE HOUR(time)=8 AND ST_Within(point, geom) GROUP BY date ORDER BY date")
    distJoin.write.option("header", "True").mode("overwrite").csv(outputPath)
