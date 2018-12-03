package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  spark.udf.register("prune",(X: Int, Y: Int, Z: Int)=> HotcellUtils.prunePoints(X, Y, Z, minX.toInt, maxX.toInt, minY.toInt, maxY.toInt, minZ.toInt, maxZ.toInt))
  pickupInfo.createOrReplaceTempView("taxidata")
  pickupInfo = spark.sql("select * from taxidata where prune(taxidata.x,taxidata.y,taxidata.z)")

  pickupInfo = pickupInfo.select(concat(pickupInfo.col("x"), lit(","), pickupInfo.col("y"),lit(","), pickupInfo.col("z")).alias("coordinates"))
  pickupInfo = pickupInfo.groupBy("coordinates").agg(count("coordinates").alias("hotness"))
  pickupInfo.createOrReplaceTempView("taxidata")
  //pickupInfo.show()

  val mean =  pickupInfo.agg(sum("hotness")).first.getLong(0).*(1.0)./(numCells)
  val deviation = scala.math.sqrt(pickupInfo.withColumn("hotness2", pow("hotness", 2)).agg(sum("hotness2")).first.getDouble(0)./(numCells).-(mean.*(mean)))
  val coordMap = pickupInfo.collect().map(tuple => (tuple.getString(0),tuple.getLong(1))).toMap
  spark.udf.register("getisOrd",(coordinates: String)=> HotcellUtils.getisOrd(coordinates, coordMap, mean, deviation, minX.toInt, maxX.toInt, minY.toInt, maxY.toInt, minZ.toInt, maxZ.toInt, numCells.toInt))
  pickupInfo = spark.sql("select taxidata.coordinates, getisOrd(taxidata.coordinates) as zscore from taxidata")
  pickupInfo = pickupInfo.sort(desc("zscore"))
  //pickupInfo.show()
  pickupInfo = pickupInfo.selectExpr("split(coordinates, ',')[0] as x","split(coordinates, ',')[1] as y","split(coordinates, ',')[2] as z")

  return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}
