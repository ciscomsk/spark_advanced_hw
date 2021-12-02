package org.apache.spark.sql.hybrid

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.MongoHelper.GenericObservable
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, TableScan}
import org.apache.spark.sql.types.{DataType, LongType, Metadata, StructField, StructType}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.and
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, MongoDatabase}

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

case class FileRec(_id: ObjectId, objectName: String, path: String, commitMillis: Long)

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
      val jsonStringIter: Iterator[String] = new RowConverter(schema).toJsonString(partition)
      val filePath: String = s"$saveDirectory/${java.util.UUID.randomUUID().toString}.json"

      FileHelper.write(filePath, jsonStringIter)

      val doc: Document = Document(
        "objectName" -> objectName,
        "path" -> filePath,
        "commitMillis" -> System.currentTimeMillis()
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

//    val mongoConnection: MongoDatabase = MongoInteraction.connect(mongoUri).withCodecRegistry(codecRegistry)

//    val docs: Seq[Document] = MongoInteraction.getSchemas(mongoConnection, "schema_index", objectName)
//    println(docs)

    import org.mongodb.scala.bson.codecs.Macros._
    import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
    import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

    val codecRegistrySchema: CodecRegistry =
      fromRegistries(fromProviders(classOf[SchemaRec]), DEFAULT_CODEC_REGISTRY )

    val mongoConnectionSchema: MongoDatabase = MongoInteraction.connect(mongoUri).withCodecRegistry(codecRegistrySchema)
    val schemasCollection: MongoCollection[SchemaRec] = mongoConnectionSchema.getCollection("schema_index")

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

    val codecRegistryFile: CodecRegistry =
      fromRegistries(fromProviders(classOf[FileRec]), DEFAULT_CODEC_REGISTRY )

    val mongoConnectionFile: MongoDatabase = MongoInteraction.connect(mongoUri).withCodecRegistry(codecRegistryFile)
    val filesCollection: MongoCollection[FileRec] = mongoConnectionFile.getCollection("file_index")

    val params: Array[(String, Long)] =
      filesCollection
        .find(Filters.eq("objectName", objectName))
        .results()
        .map(file => (file.path, file.commitMillis))
        .toArray

    log.info(s"filesList: ${parameters.mkString("Array(", ", ", ")")}")

    new HybridBaseRelation(params, schemaWithMillis)
  }
}

class HybridBaseRelation(parameters: Array[(String, Long)], usedSchema: StructType) extends BaseRelation
  with TableScan
  with Logging {

  override def sqlContext: SQLContext =
    SparkSession
      .active
      .sqlContext

  override def schema: StructType = usedSchema

  override def needConversion: Boolean = false

  override def buildScan(): RDD[Row] = new HybridJsonRdd(parameters, schema).asInstanceOf[RDD[Row]]
}

class HybridJsonRdd(parameters: Array[(String, Long)], schema: StructType) extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) with Logging {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val hybridJsonPartition: HybridJsonPartition = split.asInstanceOf[HybridJsonPartition]
    val path: String = hybridJsonPartition.path
    val millis: Long = hybridJsonPartition.millis

    val file: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = file.getLines

    new JsonParser(schema)
      .toRow(lines)
      .map { iRow =>
        iRow.update(0, millis)
        iRow
      }
  }

  override protected def getPartitions: Array[Partition] =
    parameters
      .zipWithIndex
      .map {
        case ((file, millis), idx) => HybridJsonPartition(file, millis, idx)
      }
}

case class HybridJsonPartition(path: String, millis: Long, index: Int) extends Partition

