package cse512
import scala.math.min
import scala.math.max

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    val coordinates=queryRectangle.split(",")
    val x1=coordinates(0).toDouble
    val y1=coordinates(1).toDouble
    val x2=coordinates(2).toDouble
    val y2=coordinates(3).toDouble
    val x_min=min(x1,x2)
    val x_max=max(x1,x2)
    val y_min=min(y1,y2)
    val y_max=max(y1,y2)
    val point_coordinates=pointString.split(",")
    val x_point=point_coordinates(0).toDouble
    val y_point=point_coordinates(1).toDouble
    if(x_point<=x_max && x_point>=x_min && y_point>=y_min && y_point<=y_max)
      return  true
    else
      return false
    // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
