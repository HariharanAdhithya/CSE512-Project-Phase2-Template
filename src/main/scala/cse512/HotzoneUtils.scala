package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rect:Array[String] = queryRectangle.split(",")
    val point:Array[String] = pointString.split(",")

    val rectD = rect.map( x => x.toDouble)
    val pointD = point.map( x => x.toDouble)

    val rx1 = rectD(0)
    val ry1 = rectD(1)
    val rx2 = rectD(2)
    val ry2 = rectD(3)

    val px = pointD(0)
    val py = pointD(1)

    if( rx1 > rx2 && ry1 > ry2)
    {
      if( rx2 <= px && px <= rx1 && ry2 <= py && py <= ry1 )
      {
        return true
      }
    }
    else if( rx1 < rx2 && ry1 > ry2)
    {
      if( rx2 >= px && px >= rx1 && ry2 <= py && py <= ry1 )
      {
        return true
      }
    }
    else if( rx1 < rx2 && ry1 < ry2)
    {
      if( rx2 >= px && px >= rx1 && ry2 >= py && py >= ry1 )
      {
        return true
      }
    }
    else if( rx1 > rx2 && ry1 < ry2)
    {
      if( rx2 <= px && px <= rx1 && ry2 >= py && py >= ry1 )
      {
        return true
      }
    }

    false
  }



  // YOU NEED TO CHANGE THIS PART

}
