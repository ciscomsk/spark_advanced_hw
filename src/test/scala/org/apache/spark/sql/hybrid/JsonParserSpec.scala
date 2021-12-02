package org.apache.spark.sql.hybrid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/** testOnly org.apache.spark.sql.hybrid.JsonParserSpec */
class JsonParserSpec extends AnyFlatSpec with should.Matchers {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder
      .master("local[1]")
      .appName("test")
      .getOrCreate

  val schema: StructType =
    StructType(List(
      StructField("foo", IntegerType),
      StructField("bar", StringType),
      StructField("baz", TimestampType),
      StructField("arr", ArrayType(IntegerType)),
      StructField("struct", StructType(List(
        StructField("inner1", IntegerType),
        StructField("inner2", StringType)
      )))
    ))

  val jsonParser: JsonParser = new JsonParser(schema)
  val rawString: String = """ { "foo": 0, "bar" : "hello world", "baz": 0, "arr": [1, 2], "struct": { "inner1": 999, "inner2": "hello" } } """

  "Parser" should s"parse $rawString" in {
    val isStreaming: Boolean = false
    val iRowIter: Iterator[InternalRow] = jsonParser.toRow(Iterator(rawString))

    val iRowRdd: RDD[InternalRow] =
      spark
        .sparkContext
        .parallelize(iRowIter.toList)

    val df: DataFrame = spark.internalCreateDataFrame(iRowRdd, schema, isStreaming)
    df.show
    df.printSchema
  }


}
