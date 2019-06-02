package com.adelpozo.streaming.services

import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduSession}
import org.apache.kudu.{ColumnSchema, Schema, Type}
import com.adelpozo.streaming.entities.TablesInfo.T_Data._
import com.adelpozo.streaming.utils.TimestampExtractor

import scala.collection.JavaConverters._
import scala.io.Source

object KuduService {

  var kuduClient: KuduClient = _

  val configPath: String = getClass.getClassLoader.getResource(".").getPath

  def initKudu: Unit = {
    kuduClient = new KuduClient.KuduClientBuilder("localhost:7051").build()

    createKuduTable(TABLENAME, s"${configPath}/data/data_schema.csv")
    loadKuduTable(TABLENAME, s"${configPath}/data/data.csv")

  }

  def closeKudu: Unit = kuduClient.close()

  private def createKuduTable(tableName: String, schema_file: String): Unit = {
    val schema = new Schema(getColumnsSchemas(schema_file).asJava)
    val createTableOptions = new CreateTableOptions
    createTableOptions.setNumReplicas(1)
    createTableOptions.addHashPartitions(getPrimaryKeyColumns(schema).asJava, 3)
    kuduClient.createTable(tableName, schema, createTableOptions)
  }

  private def getColumnsSchemas(file: String): List[ColumnSchema] = {
    val lines = Source.fromFile(file).getLines.toList
    val header = lines.head.split(",").zipWithIndex.toMap

    lines.drop(1).map(line => {
      val values = line.split(",")
      new ColumnSchema.ColumnSchemaBuilder(values(header("name")), getType(values(header("type"))))
        .key(values(header("primary_key")).toBoolean).nullable(values(header("nullable")).toBoolean)
        .build
    })
  }

  private def getType(kuduType: String): Type = {
    kuduType match {
      case "string" => Type.STRING
      case "boolean" => Type.BOOL
      case "int" => Type.INT32
      case "bigint" => Type.INT64
      case "double" => Type.DOUBLE
      case "timestamp" => Type.UNIXTIME_MICROS
      case _ => throw new IllegalArgumentException("type?????? -> " + kuduType)
    }
  }

  private def getPrimaryKeyColumns(schema: Schema): List[String] = {
    schema.getPrimaryKeyColumns.asScala.map(_.getName).toList
  }

  private def loadKuduTable(tableName: String, data_file: String): Unit = {
    val session: KuduSession = kuduClient.newSession
    val table = kuduClient.openTable(tableName)

    val timestampExtractor = new TimestampExtractor

    val lines = Source.fromFile(data_file).getLines.toList
    val header = lines.head.split(",").zipWithIndex.toMap

    lines.drop(1).foreach(line => {
      val operation = table.newInsert()
      val values = line.split(",")

      for (c: ColumnSchema <- table.getSchema.getColumns.asScala) {
        val name = c.getName
        val value = values(header(name))

        value.equalsIgnoreCase("NULL") match {
          case true =>
            c.isKey || !c.isNullable match {
              case true => throw new IllegalArgumentException("null?????? -> " + name)
              case _ => operation.getRow.setNull(name)
            }
          case false =>
            c.getType match {
              case Type.STRING => operation.getRow.addString(name, value)
              case Type.INT32 => operation.getRow.addInt(name, value.toInt)
              case Type.INT64 => operation.getRow.addLong(name, value.toLong)
              case Type.DOUBLE => operation.getRow.addDouble(name, value.toDouble)
              case Type.BOOL => operation.getRow.addBoolean(name, value.toBoolean)
              case Type.UNIXTIME_MICROS => operation.getRow.addLong(name, timestampExtractor.getTimestamp(value))
              case _ => throw new IllegalArgumentException("type?????? -> " + c.getType)
            }
        }

      }
      session.apply(operation)
    })

    session.close()
  }

  def countRecords(tableName: String): Int = {
    val table = kuduClient.openTable(tableName)
    val scanner = kuduClient.newScannerBuilder(table).build()
    var count = 0
    while (scanner.hasMoreRows) {
      val results = scanner.nextRows()
      while (results.hasNext) {
        results.next()
      }
      count += results.getNumRows
    }
    scanner.close()
    count
  }

}
