package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01
  val minX = -74.50/coordinateStep
  val maxX = -73.70/coordinateStep
  val minY = 40.50/coordinateStep
  val maxY = 40.90/coordinateStep
  val minZ = 1
  val maxZ = 31

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

  def checkNeighbours(i_x: Int, i_y: Int, i_z: Int, j_x: Int, j_y: Int, j_z: Int) : Boolean =
  {
//    if((j_x == i_x) && (j_y == i_y) && (j_z == i_z))
//      return false
    if((j_x <= i_x + 1) && (j_x >= i_x - 1) && (j_y <= i_y + 1) && (j_y >= i_y - 1) && (j_z <= i_z + 1) && (j_z >= i_z - 1))
      return true
    else
      return false
  }

  def checkbound(i_x: Long, i_y: Long, i_z: Long) : Boolean =
    {
      if(i_x > maxX || i_x < minX || i_y > maxY || i_y < minY || i_z > maxZ || i_z < minZ)
        return false
      else
        return  true
    }

  // YOU NEED TO CHANGE THIS PART
}
