package org.apache.spark.sql.hybrid

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.lang

/** testOnly org.apache.spark.sql.hybrid.HybridRelationSpec */
/**
 * https://www.mongodb.com/compatibility/docker
 * docker run --name mongodb -d -p 27017:27017 mongo
 * docker exec -it 787a7f6ea3ce mongo
 *
 * use index_store
 * db.file_index.findOne()
 * db.file_index.find()
 * db.file_index.deleteMany( { "objectName" : "test01"} )
 *
 * db.schema_index.find()
 *
 */
class HybridRelationSpec extends AnyFlatSpec with should.Matchers {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

  val writeDf: Dataset[lang.Long] = spark.range(0, 10, 1, 1)

//  "Hybrid JSON" should "write" in {
//    writeDf
//      .write
//      .format("hybrid-json")
//      .option("path", "/home/mike/_learn/repos/newprolab/spark_2/labs/src/test/resources/foo.json")
//      .option("objectName", "test01")
//      .save()
//  }

  "Hybrid JSON" should "read" in {
    val readDf: DataFrame =
      spark
        .read
        .format("hybrid-json")
        .option("objectName", "test01")
        .load()

    readDf.printSchema()
    readDf.show(50, truncate = false)
  }
}
