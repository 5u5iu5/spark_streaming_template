/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package com.adelpozo.streaming.events.model

import scala.annotation.switch

case class Info(var name: String, var secondName: String, var surname: String) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("", "", "")
  def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => {
        name
      }.asInstanceOf[AnyRef]
      case 1 => {
        secondName
      }.asInstanceOf[AnyRef]
      case 2 => {
        surname
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }
  def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.name = {
        value.toString
      }.asInstanceOf[String]
      case 1 => this.secondName = {
        value.toString
      }.asInstanceOf[String]
      case 2 => this.surname = {
        value.toString
      }.asInstanceOf[String]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = Info.SCHEMA$
}

object Info {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Info\",\"namespace\":\"com.adelpozo.streaming.events.model\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"secondName\",\"type\":\"string\"},{\"name\":\"surname\",\"type\":\"string\"}]}")
}