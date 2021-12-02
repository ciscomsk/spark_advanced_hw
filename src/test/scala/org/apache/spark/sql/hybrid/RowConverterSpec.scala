package org.apache.spark.sql.hybrid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.lang

/** testOnly org.apache.spark.sql.hybrid.RowConverterSpec */
class RowConverterSpec extends AnyFlatSpec with should.Matchers {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder
      .master("local[1]")
      .appName("test")
      .getOrCreate

  "Converter" should "work" in {
    val dataDs: Dataset[lang.Long] = spark.range(1)

    val iRowIter: Iterator[InternalRow] =
      dataDs
        .queryExecution
        .toRdd
        .collect
        .toIterator

    val converter: RowConverter = new RowConverter(dataDs.schema)
    val jsonIter: Iterator[String] = converter.toJsonString(iRowIter)

    jsonIter.foreach(println)
  }


}
