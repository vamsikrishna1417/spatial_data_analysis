package cse512

import org.apache.spark.sql.SparkSession
import scala.math.min
import scala.math.max
import scala.math.pow
object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")


    val contains = (queryRectangle:String, pointString:String) => {
      val coordinates=queryRectangle.split(",")
      val x1=coordinates(0).toFloat
      val y1=coordinates(1).toFloat
      val x2=coordinates(2).toFloat
      val y2=coordinates(3).toFloat
      val x_min=min(x1,x2)
      val x_max=max(x1,x2)
      val y_min=min(y1,y2)
      val y_max=max(y1,y2)
      val point_coordinates=pointString.split(",")
      val x_point=point_coordinates(0).toFloat
      val y_point=point_coordinates(1).toFloat
      if(x_point<=x_max && x_point>=x_min && y_point>=y_min && y_point<=y_max)
        true
      else
        false

    }

    spark.udf.register("ST_Contains",contains)

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }


  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    val contains = (queryRectangle:String, pointString:String) => {
      val coordinates=queryRectangle.split(",")
      val x1=coordinates(0).toFloat
      val y1=coordinates(1).toFloat
      val x2=coordinates(2).toFloat
      val y2=coordinates(3).toFloat
      val x_min=min(x1,x2)
      val x_max=max(x1,x2)
      val y_min=min(y1,y2)
      val y_max=max(y1,y2)
      val point_coordinates=pointString.split(",")
      val x_point=point_coordinates(0).toFloat
      val y_point=point_coordinates(1).toFloat
      if(x_point<=x_max && x_point>=x_min && y_point>=y_min && y_point<=y_max)
        true
      else
        false

    }



    spark.udf.register("ST_Contains",contains)


    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    val within=(pointString1:String, pointString2:String, distance:Double)=>{
      val point1=pointString1.split(",")
      val point2=pointString2.split(",")
      val x1=point1(0).toDouble
      val y1=point1(1).toDouble
      val x2=point2(0).toDouble
      val y2=point2(1).toDouble
      val cal_dist=(math.sqrt(pow((x1-x2),2)+pow((y1-y2),2)))
      if(cal_dist<=distance)
        true
      else
        false
    }
    spark.udf.register("ST_Within",within)

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")
    val within=(pointString1:String, pointString2:String, distance:Double)=>{
      val point1=pointString1.split(",")
      val point2=pointString2.split(",")
      val x1=point1(0).toDouble
      val y1=point1(1).toDouble
      val x2=point2(0).toDouble
      val y2=point2(1).toDouble
      val cal_dist=(math.sqrt(pow((x1-x2),2)+pow((y1-y2),2)))
      if(cal_dist<=distance)
        true
      else
        false
    }
    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",within)
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
