package org.apache.spark.sql.hybrid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.CharArrayWriter
import java.lang
import scala.collection.mutable

class RowConverter(dataType: StructType) {
  val writer: CharArrayWriter = new CharArrayWriter

  val parserOptions: JSONOptions = new JSONOptions(Map.empty[String, String], "UTC")
  val gen: JacksonGenerator = new JacksonGenerator(dataType, writer, parserOptions)

  val intCols: Array[String] =
    dataType
      .fields
      .collect { case sf if sf.dataType == IntegerType => sf.name }

  val statistics: mutable.Map[String, (Int, Int)] = mutable.Map.empty[String, (Int, Int)]

  def toJsonString(input: Iterator[InternalRow]): Iterator[String] =
    input.map { iRow =>
      intCols.foreach { colName =>
//        println(colName)
        val colValue: Int = iRow.getInt(dataType.fieldIndex(colName))
//        println(colValue)
        val colStats: Option[(Int, Int)] = statistics.get(colName)
//        println(colStats)

        colStats match {
          case Some((min, max)) =>
            if (colValue > max) statistics(colName) = (min, colValue)
            if (colValue < min) statistics(colName) = (colValue, max)

          case None =>
            statistics(colName) = (colValue, colValue)
        }
      }
//      println(statistics)

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