package org.apache.spark.sql.hybrid

import com.fasterxml.jackson.core
import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Failure, Success, Try}

class JsonParser(schema: StructType) {
  /** ??? В тестах 1970-01-01 03:00:00, а не 00:00:00 */
  val parserOptions: JSONOptions = new JSONOptions(Map.empty[String, String], "UTC", "")
  val parser: JacksonParser = new JacksonParser(schema, parserOptions)

  val stringParser: (JsonFactory, String) => core.JsonParser =
    CreateJacksonParser.string(_: JsonFactory, _: String)

  def stringToUTF8String(value: String): UTF8String =
    UTF8String.fromBytes(value.getBytes, 0, value.length)

  def toRow(input: Iterator[String]): Iterator[InternalRow] =
    input.map { str =>
      val parseResult: Try[Seq[InternalRow]] = Try { parser.parse(str, stringParser, stringToUTF8String) }

      parseResult match {
        case Success(value) =>
          value.head

        case Failure(_) =>
          val fieldCount: Int = schema.fields.length
          InternalRow((1 to fieldCount).map(_ => null): _*)
      }
    }
}


object Test extends App {
//  val schema: StructType = StructType(List(
//    StructField("foo", IntegerType)
//  ))
//
//  val rawString: String = """ { "foo": 0 } """

  val schema: StructType =
    StructType(List(
      StructField("foo", IntegerType),
      StructField("bar", StringType)
    ))

//  val jsonParser: JsonParser = new JsonParser(schema)
  val rawString: String = """ { "foo": 0, "bar" : "hello world" """

  def textToUTF8String(value: String): UTF8String = {
    UTF8String.fromBytes(value.getBytes, 0, value.length)
  }

//  val ciMap: CaseInsensitiveMap[String] = CaseInsensitiveMap[String](Map.empty[String, String])
//  val parserOptions: JSONOptions = new JSONOptions(ciMap, "utc", "")
  val parserOptions: JSONOptions = new JSONOptions(Map.empty[String, String], "utc")

  val parser: JacksonParser = new JacksonParser(schema, parserOptions)

//  val textParser: (JsonFactory, Text) => core.JsonParser =
//    parser
//      .options
//      .encoding
//      .map(enc => CreateJacksonParser.string(_: JsonFactory, _: String))
//      .getOrElse(CreateJacksonParser.string(_: JsonFactory, _: String))

  val textParser: (JsonFactory, String) => core.JsonParser =
    CreateJacksonParser.string(_: JsonFactory, _: String)

  val res: Try[Seq[InternalRow]] = Try { parser.parse(rawString, textParser, textToUTF8String) }
  println(res)


//  val textParser: (JsonFactory, Text) => core.JsonParser =
//    parser.options.encoding
//      .map(enc => CreateJacksonParser.text(enc, _: JsonFactory, _: Text))
//      .getOrElse(CreateJacksonParser.text(_: JsonFactory, _: Text))
//
//  val safeParser: FailureSafeParser[Text] =
//    new FailureSafeParser[Text](
//      input => parser.parse(input, textParser, textToUTF8String),
//      parser.options.parseMode,
//      schema,
//      parser.options.columnNameOfCorruptRecord)

}