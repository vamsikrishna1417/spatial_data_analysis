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

  spark.udf.register("checkNeighbours",(i_x:Int,i_y:Int,i_z:Int, j_x:Int,j_y:Int,j_z:Int)
  =>(HotcellUtils.checkNeighbours(i_x,i_y,i_z, j_x,j_y,j_z)))

  spark.udf.register("checkbound",(i_x:Int,i_y:Int,i_z:Int)
  =>(HotcellUtils.checkbound(i_x,i_y,i_z)))

  pickupInfo.createOrReplaceTempView("pickupInfo")

    val filteredcells = spark.sql("select x, y, z from pickupInfo where checkbound(x,y,z)")
  filteredcells.createOrReplaceTempView("filteredcells")

  val pickupcounts = filteredcells.groupBy("x","y","z").count().persist()

  pickupcounts.createOrReplaceTempView("pickupcounts")

  val x = filteredcells.count()/numCells //spark.sql("select avg(count) from pickupcounts")
  val mean = x.toDouble
  print("mean", mean)

  spark.udf.register("square", (x: Int) => ((x * x).toDouble))

  val y = spark.sql("select sum(square(count)) from pickupcounts").persist()
  val squaresum = y.first().getDouble(0)
  print("squaresum", squaresum)
  //y.show()
  val std_dev = math.sqrt((squaresum / numCells.toDouble) - (mean * mean))
  print("stdev",std_dev)

  spark.udf.register("getneighbourcount",(i_x:Int,i_y:Int,i_z:Int) =>
  {
    var count = 0
    for (x <- (i_x - 1) to (i_x + 1)) {
      for (y <- (i_y - 1) to (i_y + 1)) {
        for (z <- (i_z - 1) to (i_z + 1)) {

          if(HotcellUtils.checkbound(x,y,z))
            {
              count+=1
            }
        }
      }

    }
    count

  })

  val neighbours = spark.sql("select i.x, i.y, i.z, sum(j.count) as neighbourpickups, getneighbourcount(i.x, i.y, i.z) as neighbourcounts," +
    " square(getneighbourcount(i.x, i.y, i.z)) as sqaureneighbourcounts from " +
    "pickupcounts as i, pickupcounts as j where checkNeighbours(i.x,i.y,i.z,j.x,j.y,j.z) group by i.x, i.y, i.z").persist()

 neighbours.createOrReplaceTempView("neighbours")


  //pickupcounts.show(20)
  //neighbours.show(20)

  spark.udf.register("getGscore", (neighbourpickups: Double, neighbourcounts: Double, sqaureneighbourcounts: Double) =>
    (neighbourpickups - (mean*neighbourcounts))/(std_dev * math.sqrt(((numCells*neighbourcounts) - sqaureneighbourcounts)/numCells-1)))

  val finalresult = spark.sql("select x, y, z from neighbours order by getGscore(neighbourpickups, neighbourcounts, sqaureneighbourcounts) DESC")

  finalresult.show(500)
  return finalresult // YOU NEED TO CHANGE THIS PART
}
}
