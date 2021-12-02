package org.apache.spark.sql.hybrid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.io.CharArrayWriter
import java.lang

class RowConverter(dataType: StructType) {
  val writer: CharArrayWriter = new CharArrayWriter

  val parserOptions: JSONOptions = new JSONOptions(Map.empty[String, String], "UTC")
  val gen: JacksonGenerator = new JacksonGenerator(dataType, writer, parserOptions)

  def toJsonString(input: Iterator[InternalRow]): Iterator[String] =
    input.map { iRow =>
      gen.write(iRow)
      gen.flush
      val json: String = writer.toString
      writer.reset()
      /** ??? насколько нужно UTF8String */
      UTF8String.fromString(json).toString
    }

}

object Test2 extends App {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder
      .master("local[1]")
      .appName("test")
      .getOrCreate


  val dataDs: Dataset[lang.Long] = spark.range(1)
  val dataDsSchema: StructType = dataDs.schema

  val iRowIter: Iterator[InternalRow] =
    dataDs
      .queryExecution
      .toRdd
      .collect
      .toIterator

  val firstIRow: InternalRow = iRowIter.next

  val writer: CharArrayWriter = new CharArrayWriter

//  val ciMap: CaseInsensitiveMap[String] = CaseInsensitiveMap[String](Map.empty[String, String])
  val parserOptions: JSONOptions = new JSONOptions(Map.empty[String, String], "utc")

  val gen: JacksonGenerator = new JacksonGenerator(dataDsSchema, writer, parserOptions)

  gen.write(firstIRow)
  gen.flush
  val json = writer.toString
  writer.reset()
  val res = UTF8String.fromString(json)

  println(res)

}