package org.apache.spark.sql.hybrid

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.MongoHelper.GenericObservable
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, PrunedFilteredScan, RelationProvider, TableScan}
import org.apache.spark.sql.types.{DataType, LongType, Metadata, StructField, StructType}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.and
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, MongoDatabase}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object MongoInteraction {
  def connect(mongoUri: String): MongoDatabase = {
    val mongoClient: MongoClient = MongoClient(mongoUri)
    mongoClient.getDatabase("index_store")
  }

  def writeDocument(database: MongoDatabase, collection: String, document: Document): Seq[Completed] = {
    val targetCollection: MongoCollection[Document] = database.getCollection(collection)

    targetCollection
      .insertOne(document)
      .results()
  }

  def checkSchemaUniqueness(database: MongoDatabase,
                            collection: String,
                            objectName: String,
                            schema: String): Boolean = {

    val targetCollection: MongoCollection[Document] = database.getCollection(collection)

    targetCollection
      .find(and(Filters.eq("objectName", objectName), Filters.eq("schemaRef", schema)))
      .results()
      .isEmpty
  }

  def getSchemas(database: MongoDatabase,
                 collection: String,
                 objectName: String): Seq[Document] = {

    val targetCollection: MongoCollection[Document] = database.getCollection(collection)

    targetCollection
      .find(Filters.eq("objectName", objectName))
      .results()
  }
}

case class SchemaRec(_id: ObjectId, objectName: String, schemaRef: String, commitMillis: Long)
case class StatsRec(name: String, min: Int, max: Int)
case class FileRec(_id: ObjectId, objectName: String, path: String, commitMillis: Long, columnStats: List[StatsRec])

class HybridRelation extends CreatableRelationProvider
  with RelationProvider
  with DataSourceRegister
  with Logging {

  override def shortName(): String = "hybrid-json"

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    log.info(s"${this.logName} has been created")
    log.info(s"SaveMode=$mode")
    log.info(s"${parameters.mkString(", ")}")
    log.info(s"${data.schema.simpleString}")

    val iRdd: RDD[InternalRow] =
      data
        .queryExecution
        .toRdd

    val saveDirectory: String = parameters.getOrElse("path", throw new IllegalArgumentException("path must be set"))
    log.info(s"saveDirectory: $saveDirectory")
    FileHelper.ensureDirectory(saveDirectory)

    val schema: StructType = data.schema
    log.info(s"schema: $schema")

    val objectName: String = parameters.getOrElse("objectName", throw new IllegalArgumentException("objectName must be set"))
    log.info(s"objectName: $objectName")

    val mongoUri: String = System.getenv("MONGO_URI")
    log.info(s"MONGO_URI: $mongoUri")

    iRdd.foreachPartition { partition =>
      val rowConverter: RowConverter = new RowConverter(schema)
      val jsonStringIter: Iterator[String] = rowConverter.toJsonString(partition)
      val stats: mutable.Map[String, (Int, Int)] = rowConverter.statistics
      /** !!! IS EMPTY */
      println(stats)
      val filePath: String = s"$saveDirectory/${java.util.UUID.randomUUID().toString}.json"

      FileHelper.write(filePath, jsonStringIter)

      val mongoStats: List[Document] =
        stats
          .map { case (colName, (min, max)) => Document("name" -> colName, "min" -> min, "max" -> max) }
          .toList

      /** !!! NOT EMPTY */
      println(mongoStats)

      val doc: Document = Document(
        "objectName" -> objectName,
        "path" -> filePath,
        "commitMillis" -> System.currentTimeMillis(),
        "columnStats" -> mongoStats
      )

      val mongoConnection: MongoDatabase = MongoInteraction.connect(mongoUri)
      MongoInteraction.writeDocument(mongoConnection, "file_index", doc)
    }

    val mongoConnection: MongoDatabase = MongoInteraction.connect(mongoUri)

    val jsonSchema: String =
      schema
        .asNullable
        .json

    log.info(s"jsonSchema: $jsonSchema")

    val schemaIsUnique: Boolean =
      MongoInteraction.checkSchemaUniqueness(mongoConnection, "schema_index", objectName, jsonSchema)

    log.info(s"schemaIsUnique: $schemaIsUnique")

    if (schemaIsUnique) {
      val doc: Document = Document(
        "objectName" -> objectName,
        "schemaRef" -> jsonSchema,
        "commitMillis" -> System.currentTimeMillis()
      )

      MongoInteraction.writeDocument(mongoConnection, "schema_index", doc)
    }

    new BaseRelation {
      override def sqlContext: SQLContext = ???
      override def schema: StructType = ???
    }
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    log.info(s"${this.logName} has been created")
    log.info(s"${parameters.mkString(", ")}")

    val objectName: String = parameters.getOrElse("objectName", throw new IllegalArgumentException("objectName must be set"))
    log.info(s"objectName: $objectName")

    val mongoUri: String = System.getenv("MONGO_URI")
    log.info(s"MONGO_URI: $mongoUri")

    import org.mongodb.scala.bson.codecs.Macros._
    import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
    import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

    val codecRegistry: CodecRegistry =
      fromRegistries(
        fromProviders(classOf[SchemaRec], classOf[FileRec], classOf[StatsRec]),
        DEFAULT_CODEC_REGISTRY
      )

    val mongoConnection: MongoDatabase = MongoInteraction.connect(mongoUri).withCodecRegistry(codecRegistry)
    val schemasCollection: MongoCollection[SchemaRec] = mongoConnection.getCollection("schema_index")

    val schemasList: List[DataType] =
      schemasCollection
        .find(Filters.eq("objectName", objectName))
        .results()
        .map(el => DataType.fromJson(el.schemaRef))
        .toList

    val mergedSchema: StructType =
      schemasList
        .tail
        .foldLeft(schemasList.head) { (acc, schema) =>
          StructType.merge(acc, schema)
        }
        .asInstanceOf[StructType]

    log.info(s"mergedSchema: $mergedSchema")

    val schemaWithMillis: StructType = {
      val millisField: StructField = StructField("__commitMillis", LongType, nullable = true, Metadata.empty)
      val initialStruct: StructType = (new StructType).add(millisField)

      mergedSchema
        .fields
        .foldLeft(initialStruct) { (acc, sf) => acc.add(sf) }
    }

    log.info(s"schemaWithMillis: $schemaWithMillis")

    val filesCollection: MongoCollection[FileRec] = mongoConnection.getCollection("file_index")

    val params: Array[(String, Long, Map[String, (Int, Int)])] =
      filesCollection
        .find(Filters.eq("objectName", objectName))
        .results()
        .map(file => (file.path, file.commitMillis, file.columnStats.map(st => st.name -> (st.min, st.max)).toMap))
        .toArray

    log.info(s"params: ${params.mkString("Array(", ", ", ")")}")

    new HybridBaseRelation(params, schemaWithMillis)
  }
}

class HybridBaseRelation(parameters: Array[(String, Long, Map[String, (Int, Int)])], usedSchema: StructType) extends BaseRelation
  with PrunedFilteredScan
  with Logging {

  override def sqlContext: SQLContext =
    SparkSession
      .active
      .sqlContext

  override def schema: StructType = usedSchema

  override def needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.info(s"Used filters: ${filters.mkString("Array(", ", ", ")")}")

    val integerFilters =
      filters
        .filter {
          case EqualTo(_, _) => false
          case GreaterThan(_, _) => false
          case GreaterThanOrEqual(_, _) => false
          case LessThan(_, _) => false
          case LessThanOrEqual(_, _) => false
          case _ => true
        }

    val pushedFilters: Array[Filter] = filters.diff(integerFilters)
    log.info(s"Pushed filters: ${pushedFilters.mkString("Array(", ", ", ")")}")

    new HybridJsonRdd(parameters, schema, pushedFilters).asInstanceOf[RDD[Row]]
  }

}

class HybridJsonRdd(parameters: Array[(String, Long, Map[String, (Int, Int)])],
                    schema: StructType,
                    filters: Array[Filter]) extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val hybridJsonPartition: HybridJsonPartition = split.asInstanceOf[HybridJsonPartition]
    val path: String = hybridJsonPartition.path
    val millis: Long = hybridJsonPartition.millis
    val stats: Map[String, (Int, Int)] = hybridJsonPartition.stats

    val meetsCondition: Boolean =
      filters
        .map {
          case EqualTo(attribute, value) =>
            if (value.asInstanceOf[Int] >= stats(attribute)._1 && value.asInstanceOf[Int] <= stats(attribute)._2) true
            else false

          case GreaterThan(attribute, value) =>
            if (value.asInstanceOf[Int] < stats(attribute)._2) true
            else false

          case GreaterThanOrEqual(attribute, value) =>
            if (value.asInstanceOf[Int] <= stats(attribute)._2) true
            else false

          case LessThan(attribute, value) =>
            if (value.asInstanceOf[Int] > stats(attribute)._1) true
            else false

          case LessThanOrEqual(attribute, value) =>
            if (value.asInstanceOf[Int] >= stats(attribute)._1) true
            else false
        }
        .reduce(_ && _)

    if (filters.isEmpty || meetsCondition) {
      println("Meets conditions")
      val file: BufferedSource = Source.fromFile(path)
      val lines: Iterator[String] = file.getLines

      new JsonParser(schema)
        .toRow(lines)
        .map { iRow =>
          iRow.update(0, millis)
          iRow
        }
    } else {
      println("Doesn't meets conditions")
      Iterator.empty
    }

  }

  override protected def getPartitions: Array[Partition] =
    parameters
      .zipWithIndex
      .map {
        case ((file, millis, map), idx) => HybridJsonPartition(file, millis, idx, map)
      }
}

case class HybridJsonPartition(path: String, millis: Long, index: Int, stats: Map[String, (Int, Int)]) extends Partition

