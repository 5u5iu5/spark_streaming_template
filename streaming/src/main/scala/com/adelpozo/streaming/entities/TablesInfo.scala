package com.adelpozo.streaming.entities

import org.apache.kudu.client.RowResult

object KuduUtils {

  trait Rows[T] {
    def ifNullThen(row: RowResult, name: String, value: T): T
  }

  object RowsCommons {
    implicit val RowsStringChecker: Rows[String] = new Rows[String] {
      def ifNullThen(row: RowResult, name: String, value: String): String =
        if (row.isNull(name.toString)) value else row.getString(name.toString)
    }
  }

  implicit class RowsUtils(rowsResult: RowResult) {
    def handleNulls[T](name: String, default: T)(implicit rows: Rows[T]): T = rows.ifNullThen(rowsResult, name, default)
  }

}

object TablesInfo {

  object T_Data {

    val TABLENAME: String = "impala::data"

    val COLUMNS: List[String] = List("cod", "fecha", "value1", "value2", "value3")

    sealed trait Entity extends Serializable

    case class InfoTable(cod: String, fecha: String, value1: String, value2: String, value3: String) extends Entity

    def dataMapper(row: RowResult): InfoTable = {
      import com.adelpozo.streaming.utils.KuduUtils.RowsCommons.RowsStringChecker
      import com.adelpozo.streaming.utils.KuduUtils.RowsUtils
      InfoTable(
        row.getString("cod"),
        row.getString("fecha"),
        row.handleNulls[String]("value1", ""),
        row.handleNulls[String]("value2", ""),
        row.handleNulls[String]("value3", "")
      )
    }

  }

}
