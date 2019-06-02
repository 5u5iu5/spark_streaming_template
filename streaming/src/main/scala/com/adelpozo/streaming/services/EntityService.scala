package com.adelpozo.streaming.services

import com.adelpozo.streaming.entities.TablesInfo.T_Data._
import com.adelpozo.streaming.events.InputEvent
import com.adelpozo.streaming.utils.KuduDataSource

object EntityService extends Serializable {

  def getEntity(kuduDataSource: KuduDataSource, inputPK: InputEvent): List[InfoTable] = {
    val predicates = Map("cod" -> inputPK.cod, "fecha" -> inputPK.date)
    val infoTables = kuduDataSource.query[InfoTable](TABLENAME, Some(COLUMNS), predicates, dataMapper)

    if (infoTables.isEmpty) throw new RuntimeException(s"Empty Info Table searching for ${predicates.toString()}") else
      infoTables
  }
}
