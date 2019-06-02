package com.adelpozo.streaming.utils

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

    implicit val RowsDoubleChecker: Rows[Double] = new Rows[Double] {
      def ifNullThen(row: RowResult, name: String, value: Double): Double =
        if (row.isNull(name.toString)) value else row.getDouble(name.toString)
    }
  }

  implicit class RowsUtils(rowsResult: RowResult) {
    def handleNulls[T](name: String, default: T)(implicit rows: Rows[T]): T = rows.ifNullThen(rowsResult, name, default)
  }
}
