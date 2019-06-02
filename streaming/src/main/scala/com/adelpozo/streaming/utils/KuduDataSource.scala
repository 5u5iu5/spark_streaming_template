package com.adelpozo.streaming.utils

import org.apache.kudu.Type
import org.apache.kudu.client.KuduPredicate.ComparisonOp.EQUAL
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder
import org.apache.kudu.client.{KuduPredicate, KuduScanner, KuduTable, RowResult}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try


case class KuduDataSource(spark: SparkSession, host: String) extends Serializable {

  private val kuduContext = new KuduContext(host, spark.sparkContext)


  def insertKuduRows(df: DataFrame, table: String) = kuduContext.insertRows(df, table)

  def insertIgnoreRows(df: DataFrame, table: String) = kuduContext.insertIgnoreRows(df, table)

  def upsertRows(df: DataFrame, table: String) = kuduContext.upsertRows(df, table)

  def deleteRows(df: DataFrame, table: String) = kuduContext.deleteRows(df, table)


  def fromKuduTable(table: String, columns: List[String] = Nil): Try[DataFrame] = {
    val kuduRDD = kuduContext.kuduRDD(spark.sparkContext, table, columns)
    Try(spark.createDataFrame(kuduRDD, sparkSchema(table, Option(columns))))
  }

  def query(tableName: String, columns: List[String], predicates: Map[String, String]): List[scala.collection.mutable.Map[String, Any]] = {
    val table: KuduTable = kuduContext.syncClient.openTable(tableName)
    val schema = table.getSchema
    val scannerBuilder: KuduScannerBuilder = kuduContext.syncClient.newScannerBuilder(table)
    scannerBuilder.setProjectedColumnNames(columns.asJava)
    predicates.foreach(value => scannerBuilder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(value._1), EQUAL, value._2)))
    val scanner: KuduScanner = scannerBuilder.build()

    var records = List[scala.collection.mutable.Map[String, Any]]()

    while (scanner.hasMoreRows) {
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        val row = rows.next()
        val result = scala.collection.mutable.Map[String, Any]()
        for (column: String <- columns) {
          row.isNull(column) match {
            case true => result += (column -> None)
            case false =>
              schema.getColumn(column).getType match {
                case Type.STRING => result += (column -> row.getString(column))
                case Type.INT8 => result += (column -> row.getByte(column))
                case Type.INT16 => result += (column -> row.getShort(column))
                case Type.INT32 => result += (column -> row.getInt(column))
                case Type.INT64 => result += (column -> row.getLong(column))
                case Type.DOUBLE => result += (column -> row.getDouble(column))
                case Type.BOOL => result += (column -> row.getBoolean(column))
                case Type.UNIXTIME_MICROS => result += (column -> row.getLong(column))
                case _ => throw new IllegalArgumentException("Type not implemented: " + schema.getColumn(column).getType)
              }
          }
        }
        records = result :: records
      }
    }

    scanner.close()
    records
  }

  def query[T](tableName: String, columns: Option[List[String]], predicates: Map[String, String], mapper: RowResult => T): List[T] = {
    val table: KuduTable = kuduContext.syncClient.openTable(tableName)
    val schema = table.getSchema
    val scannerBuilder: KuduScannerBuilder = kuduContext.syncClient.newScannerBuilder(table)
    columns match {
      case Some(cols) => scannerBuilder.setProjectedColumnNames(cols.asJava)
      case _ =>
    }
    predicates.foreach(value => scannerBuilder.addPredicate(KuduPredicate.newComparisonPredicate(schema.getColumn(value._1), EQUAL, value._2)))
    val scanner: KuduScanner = scannerBuilder.build()

    var result = List[T]()

    while(scanner.hasMoreRows) {
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        val row = rows.next()
        result = mapper(row) :: result
      }
    }

    scanner.close()
    result
  }

  private def sparkSchema(table: String, fields: Option[Seq[String]] = None): StructType = {
    val schema = kuduContext.syncClient.openTable(table).getSchema
    val kuduColumns = fields match {
      case Some(fieldNames) => fieldNames.map(schema.getColumn)
      case None => schema.getColumns.asScala
    }
    val sparkColumns = kuduColumns.map { col =>
      val sparkType = kuduTypeToSparkType(col.getType)
      StructField(col.getName, sparkType, col.isNullable)
    }
    StructType(sparkColumns)
  }

  private def kuduTypeToSparkType(t: Type): DataType = t match {
    case Type.BOOL => BooleanType
    case Type.INT8 => ByteType
    case Type.INT16 => ShortType
    case Type.INT32 => IntegerType
    case Type.INT64 => LongType
    case Type.UNIXTIME_MICROS => TimestampType
    case Type.FLOAT => FloatType
    case Type.DOUBLE => DoubleType
    case Type.STRING => StringType
    case Type.BINARY => BinaryType
    case _ => throw new IllegalArgumentException(s"No support for Kudu type $t")
  }

}