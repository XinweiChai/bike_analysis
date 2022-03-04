import findspark

findspark.init()
from functions import *


def load_properties(filepath, sep='=', comment_char='#'):
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"')
                props[key] = value
    return props


def write_to_file(result):
    if outputMode == 1:
        with open(outputPath + analysis + "Result", "w+") as f:
            f.write(result.count().toString)
    elif outputMode == 2:
        result.saveAsTextFile(outputPath + analysis + "_result")
    elif outputMode == 3:
        result.sortBy(lambda x: len(x[1]) / x[0].getArea, ascending=False).saveAsTextFile(
            outputPath + analysis + "_result")
    elif outputMode == 4:
        x = result.sortBy(lambda x: len(x[1]) / x[0].getArea, ascending=False).take(firstX)
        with open(outputPath + analysis + "Result", "w+") as f:
            for i in x:
                f.write(str(i[0]) + "," + str(len(i[1])) + "," + str(len(i[1]) / i[0].getArea) + '\n')
    elif outputMode == 5:
        x = result.sortBy(lambda x: len(x[1]), ascending=False).take(firstX)
        with open(outputPath + analysis + "Result", "w+") as f:
            for i in x:
                f.write(str(i[0]) + "," + str(len(i[1])) + '\n')
    elif outputMode == 6:
        print(result.count())


if __name__ == "__main__":
    configPath = "conf/"
    joinQueryPartitioningType = GridType.QUADTREE
    prop = load_properties(configPath + "colo.point.properties")

    pointInputLocation = prop["inputLocation"]
    pointLatOffset = int(prop["latOffset"])
    pointLonOffset = int(prop["lonOffset"])
    pointOffset = int(prop["offset"])
    pointIndexType = IndexType.RTREE
    fromTime = prop["fromTime"]
    toTime = prop["toTime"]
    start = datetime.datetime.strptime(fromTime, "%Y-%m-%dT%H:%M:%S")
    end = datetime.datetime.strptime(toTime, "%Y-%m-%dT%H:%M:%S")

    prop = load_properties(configPath + "colo.poi.properties")
    poiInputLocation = prop["inputLocation"]
    poiInputLocation2 = prop["inputLocation2"]

    prop = load_properties(configPath + "colo.polygon.properties")
    polygonInputLocation = prop["inputLocation"]

    prop = load_properties(configPath + "colo.linestring.properties")
    lineStringInputLocation = prop["inputLocation"]

    prop = load_properties(configPath + "colo.run.properties")
    outputMode = int(prop["outputMode"])
    analysis = prop["analysis"]
    outputPath = prop["outputPath"]
    correspondingTable = bool(prop["correspondingTable"])
    firstX = int(prop["firstX"])
    epsg3857 = prop["epsg3857"] == "true"
    radius = float(prop["radius"])
    radius2 = float(prop["radius2"])

    spark = SparkSession.builder \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) \
        .appName("Colocation") \
        .getOrCreate()

    GeoSparkRegistrator.registerAll(spark)


    print("output: " + outputPath)
    print("task: " + analysis)
    if analysis == "distanceJoin":
        print("point input: " + pointInputLocation)
        print("linestring input: " + lineStringInputLocation)
        distanceJoinQueryUsingIndex(spark, pointInputLocation, lineStringInputLocation, epsg3857, radius,
                                    joinQueryPartitioningType, pointIndexType)
    elif analysis == "spatialJoin":
        print("point input: " + pointInputLocation)
        print("poi input: " + poiInputLocation)
        spatialJoinQueryUsingIndex(spark, pointInputLocation, poiInputLocation, epsg3857, joinQueryPartitioningType,
                                   pointIndexType, radius)
    elif analysis == "linestringBuffer":
        print("linestring input: " + lineStringInputLocation)
        linestringBuffer(spark, lineStringInputLocation, radius, outputPath)
    # elif analysis == "pointBuffer":
    #    print("point input: " + poiInputLocation)
    #    pointBuffer(poiInputLocation, poiLonOffset, poiLatOffset)
    elif analysis == "distJoinSQL":
        print("point input: " + pointInputLocation)
        print("linestring input: " + lineStringInputLocation)
        distanceJoinQuerySQL(spark, pointInputLocation, lineStringInputLocation, start, end, radius, outputPath)
    elif analysis == "distanceJoinPoints":
        print("point input: " + pointInputLocation)
        print("poi input: " + poiInputLocation)
        distanceJoinPoints(spark, pointInputLocation, poiInputLocation, start, end, radius, outputPath)
    elif analysis == "doubleJoin":
        print("point input: " + pointInputLocation)
        print("poi input: " + poiInputLocation)
        print("poi input2: " + poiInputLocation2)
        doubleJoin(spark, pointInputLocation, poiInputLocation, poiInputLocation2, start, end, radius, radius2,
                   outputPath)
    elif analysis == "join_labeled":
        print("point input: " + pointInputLocation)
        print("poi input: " + poiInputLocation)
        join_labeled(spark, pointInputLocation, poiInputLocation, start, end, outputPath)
    print("Tasks finished")
    spark.stop()
