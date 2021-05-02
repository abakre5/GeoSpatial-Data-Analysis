package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  /**
   * calc square of real number
   */
  def squareOfNumber(no: Int): Double = {
    return (no * no).toDouble
  }

  /**
   * Calc number of neighbours for a cell on the basis of its position in the space time.
   */
  def noOfNeighbouringCells(X: Int, Y: Int, Z: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int): Int = {
    val noOfAxisOnWhichPointLies = checkBorderCondition(X, minX, maxX) + checkBorderCondition(Y, minY, maxY) + checkBorderCondition(Z, minZ, minZ)
    noOfAxisOnWhichPointLies match {
      case 3 => return 7
      case 2 => return 11
      case 1 => return 17
      case _ => return 26
    }
    return 26
  }

  /**
   * check whether the co-ordinate lie on the axis
   */
  def checkBorderCondition(coOrdinate: Int, min: Int, max: Int): Int = {
    if (coOrdinate == min || coOrdinate == max) {
      return 1
    }
    return 0
  }

  /**
   * Check if point[p(x,y,z) is the neighbour of point[neighbour(x,y,z)]
   */
  def areNeighbours(x: Int, y: Int, z: Int, neighbourX: Int, neighbourY: Int, neighbourZ: Int): Boolean = {
    if ((neighbourX == x + 1 || neighbourX == x || neighbourX == x - 1) &&
        (neighbourY == y + 1 || neighbourY == y || neighbourY == y - 1) &&
        (neighbourZ == z + 1 || neighbourZ == z || neighbourZ == z - 1)) {
      return true
    }
    return false
  }


  /**
   * Calc getis-ord gi* statistic for a space time co-ordinate
   */
  def calcGScore(X: Int, Y: Int, Z: Int, NoOfNeighbourer: Int, sumOfNoOfPoints: Int, numCells: Int, meanValue: Double,
                 standardDeviationValue: Double): Double = {
      return calcNumerator(sumOfNoOfPoints, meanValue, NoOfNeighbourer) / calcDemominator(standardDeviationValue, numCells, NoOfNeighbourer)
  }

  def calcNumerator(sumOfNoOfPoints: Int, meanValue: Double, NoOfNeighbourer: Int): Double = {
      return sumOfNoOfPoints.toDouble - (meanValue.toDouble * NoOfNeighbourer.toDouble).toDouble
  }

  def calcDemominator(standardDeviationValue: Double, numCells: Int, NoOfNeighbourer: Int): Double = {
      return (standardDeviationValue * math.sqrt(((numCells.toDouble * NoOfNeighbourer.toDouble) - (NoOfNeighbourer.toDouble * NoOfNeighbourer.toDouble)) / (numCells.toDouble - 1.0)).toDouble).toDouble
  }
}