package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ListBuffer

//8668079776
//1587716

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

  // YOU NEED TO CHANGE THIS PART


  def filterCoordinate(inputX: Int, inputY: Int, inputZ:Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY:Int, maxZ: Int ): Boolean =
  {
    if(inputX < minX || inputX > maxX) return false
    if(inputY < minY || inputY > maxY) return false
    if(inputZ < minZ || inputZ > maxZ) return false
    true
  }

  def calculateZScore(cellId: String, hotnessMap: Map[String, Long], mean: Double, sd: Double, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY:Int, maxZ: Int, numCells: Int): Double =
  {
    var neighbours = new ListBuffer[Long]()
    val x :: y :: z :: _ = cellId.split(",").toList
    for(day <- z.toInt.-(1) to z.toInt.+(1))
      for(lat <- x.toInt.-(1) to x.toInt.+(1))
        for(long <- y.toInt.-(1) to y.toInt.+(1))
          if(filterCoordinate(lat,long,day,minX,minY,minZ,maxX,maxY,maxZ))
            if (hotnessMap.contains(lat.toString +','+ long.toString +','+ day.toString))
              neighbours += hotnessMap(lat.toString +','+ long.toString +','+ day.toString)
            else neighbours += 0
    val neighbourscnt = neighbours.size
    val zscore = neighbours.sum.-(mean.*(neighbourscnt))./(sd.*(scala.math.sqrt(neighbourscnt.*(numCells).-(neighbourscnt.*(neighbourscnt))./(numCells.-(1)))))
    zscore
  }
}
