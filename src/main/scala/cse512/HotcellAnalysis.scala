package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
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
  pickupInfo.createOrReplaceTempView("dataset")
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
  

    /**
     * Collect all the points which are within the given space.
     */
    val pointsInsideSpace = spark.sql(("select x, y, z, count(*) as numberOfPoints from dataset where x >= %s and x <= %s" +
      " and y >= %s and y <= %s and z >= %s and z <= %s group by z, y, x order by z, y, x").format(
      minX, maxX, minY, maxY, minZ, maxZ
    ))
    pointsInsideSpace.createOrReplaceTempView("pointsInsideSpace")

    /**
     * Calculate Mean and Standard Deviation
     */
    spark.udf.register("squareOfNumber", (no: Int) => HotcellUtils.squareOfNumber(no))
    val sumOfNumberOfPoints = spark.sql("select sum(numberOfPoints) as Sum, sum(squareOfNumber(numberOfPoints)) as SumOfSquare " +
      "from pointsInsideSpace")
    sumOfNumberOfPoints.createOrReplaceTempView("sumOfNumberOfPoints")

    val sum = sumOfNumberOfPoints.select("Sum").rdd.map(relation => relation(0)).collect()(0).toString.toInt
    val sumOfSquare = sumOfNumberOfPoints.select("SumOfSquare").rdd.map(relation => relation(0)).collect()(0).toString.toDouble

    val meanValue = sum / numCells.toDouble
    val standardDeviationValue = math.sqrt((sumOfSquare / numCells.toDouble) - (meanValue.toDouble * meanValue.toDouble))

    /**
     * Let's now calc neighbours of a cell and it's required results
     */
    spark.udf.register("noOfNeighbouringCells", (X: Int, Y: Int, Z: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int)
    => HotcellUtils.noOfNeighbouringCells(X, Y, Z, minX, minY, minZ, maxX, maxY, maxZ))
    spark.udf.register("areNeighbours", (x: Int, y: Int, z: Int, neighbourX: Int, neighbourY: Int, neighbourZ: Int)
    => HotcellUtils.areNeighbours(x: Int, y: Int, z: Int, neighbourX: Int, neighbourY: Int, neighbourZ: Int))

    var neighbourerOfCell = spark.sql(("SELECT pis1.x as x, pis1.y as y, pis1.z as z, sum(pis2.numberOfPoints) as sumOfNoOfPoints" +
      ", noOfNeighbouringCells(pis1.x, pis1.y, pis1.z, " + minX + ", " + minY + ", " + minZ + ", " + maxX + "" +
      ", " + maxY + ", " + maxZ + ") as NoOfNeighbourer" +
      " from pointsInsideSpace as pis1, pointsInsideSpace as pis2" +
      " where areNeighbours(pis1.x, pis1.y, pis1.z, pis2.x, pis2.y, pis2.z)" +
      " group by pis1.z, pis1.y, pis1.x order by  pis1.z, pis1.y, pis1.x"))
    neighbourerOfCell.createOrReplaceTempView("neighbourerOfCell")

    /**
     * Finally calc Getis-Ord statistic(gScore)
     */
    spark.udf.register("calcGScore", (X: Int, Y: Int, Z: Int, NoOfNeighbourer: Int, sumOfNoOfPoints: Int, numCells: Int, meanValue: Double,
                                      standardDeviationValue: Double)
    => HotcellUtils.calcGScore(X, Y, Z, NoOfNeighbourer, sumOfNoOfPoints, numCells, meanValue, standardDeviationValue))
    var gScore = spark.sql(("SELECT x, y, z, calcGScore(x, y, z, NoOfNeighbourer, sumOfNoOfPoints, %s, %s, %s) as gScoreValue " +
      "from neighbourerOfCell order by gScoreValue desc").format(
      numCells, meanValue, standardDeviationValue
    ))
    gScore.createOrReplaceTempView("gScore")

    /**
     * Just written x, y, z as desired output without gScore.
     */
    var outputHC = spark.sql("SELECT x, y, z from gScore")
    outputHC.createOrReplaceTempView("outputHC")
    outputHC.show()

    return outputHC // YOU NEED TO CHANGE THIS PART
  }
}
