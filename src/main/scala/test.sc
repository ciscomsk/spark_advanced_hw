import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable

Logger
  .getLogger("org")
  .setLevel(Level.ERROR)

val spark: SparkSession =
  SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

//val df =
//  spark
//    .range(0, 10)
////    .withColumn("str", lit("string"))
//
//df.show()
//
//val jsonSchema =
//  df
//    .schema
//    .json
//
//val fromJson = DataType.fromJson("""{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""")

//val res: Array[((String, Int), Int)] = Array(("str", 100)).zipWithIndex

class mapAdd {
  val map: mutable.Map[String, (Int, Int)] = mutable.Map.empty

  def updateMap(): Unit = {
    map("key1") = (1, 1)
    map("key2") = (2, 2)
  }
}

//val mMap: mutable.Map[String, (Int, Int)] = mutable.Map.empty[String, (Int, Int)]
//mMap("key1") = (1, 1)
//mMap("key2") = (2, 2)
//println(mMap)

val mapCl = new mapAdd
println(mapCl.map)
mapCl.updateMap()
println(mapCl.map)