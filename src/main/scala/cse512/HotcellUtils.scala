package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ListBuffer

//8668079776
//1587716

object HotcellUtils {
  val coordinateStep = 0.01

  def prunePoints(X: Int, Y: Int, Z :Int, minX: Int, maxX: Int, minY: Int, maxY:Int, minZ: Int, maxZ: Int ): Boolean =
  {
    if(X >= minX && X <= maxX && Y >= minY && Y <= maxY && Z >= minZ && Z <= maxZ)
      return true
    false

  }

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

  def getisOrd(coordinates: String, coordMap: Map[String, Long], mean: Double, deviation: Double, minX: Int, maxX: Int, minY: Int, maxY:Int, minZ: Int, maxZ: Int, numCells: Int): Double =
  {
    var adjCells = new ListBuffer[Long]()
    val x = coordinates.split(",")(0)
    val y = coordinates.split(",")(1)
    val z = coordinates.split(",")(2)

    for(lat <- x.toInt.-(1) to x.toInt.+(1))
      for(long <- y.toInt.-(1) to y.toInt.+(1))
        for(dTime <- z.toInt.-(1) to z.toInt.+(1))
          if(prunePoints(lat,long,dTime,minX,maxX,minY,maxY,minZ,maxZ))
            if (coordMap.contains(lat.toString +','+ long.toString +','+ dTime.toString))
              adjCells += coordMap(lat.toString +','+ long.toString +','+ dTime.toString)
            else
              adjCells += 0
    val adjCount = adjCells.size
    val numerator = adjCells.sum.-(mean.*(adjCount))
    val denominator = deviation.*(scala.math.sqrt(adjCount.*(numCells).-(adjCount.*(adjCount))./(numCells.-(1))))
    val getisOrd = numerator/denominator

    getisOrd
  }
}
