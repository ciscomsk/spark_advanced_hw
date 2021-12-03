package org.apache.spark.sql.hybrid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class PredicatePushdownSpec extends AnyFlatSpec with should.Matchers {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

  import spark.implicits._

  "Predicate pushdown" should "work" in {
    spark
      .range(0, 20, 1, 1)
      .withColumn("id", $"id".cast(IntegerType))
      .withColumn("id2", $"id" * 2)
      .write
      .format("hybrid-json")
      .option("path", "/home/mike/_learn/repos/newprolab/spark_2/labs/src/test/resources/foo.json")
      .option("objectName", "test01")
      .save()

    val df: DataFrame =
      spark
        .read
        .format("hybrid-json")
        .option("objectName", "test01")
        .load()

    val filteredDf: Dataset[Row] = df.filter($"id2" >= 38)
    filteredDf.explain(extended = true)
    filteredDf.show(20, truncate = false)
  }

}
