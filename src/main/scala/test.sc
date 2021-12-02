import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructType}

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

val res: Array[((String, Int), Int)] = Array(("str", 100)).zipWithIndex