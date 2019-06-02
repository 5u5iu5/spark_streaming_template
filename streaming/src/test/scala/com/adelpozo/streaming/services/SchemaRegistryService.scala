package com.adelpozo.streaming.services

import java.io.File

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, get, post, stubFor}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig

object SchemaRegistryService {
  private val wireMockServer = new WireMockServer(wireMockConfig().port(8081))

  val configPath: String = getClass.getClassLoader.getResource(".").getPath

  val schema = new org.apache.avro.Schema.Parser().parse(new File(s"${configPath}/schema/avro.avsc"))

  def closeSchemaRegistry: Unit = wireMockServer.shutdown()

  def initSchemaRegistry: Unit = {
    wireMockServer.start()
    WireMock.configureFor("localhost", 8081)

    val escapedSchema = schema.toString().replaceAll("\"", "\\\\\\\"")

    println(s"SCHEMA -> ${schema.toString(false)}")

    stubFor(post("/subjects/topicToPublish-value?deleted=false")
      .willReturn(aResponse()
        .withStatus(200)
        .withHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
        .withBody(
          "{" +
            "\"subject\":\"topic-value\"," +
            "\"version\":1," +
            "\"id\":111," +
            "\"schema\":\"" + escapedSchema + "\"" +
            "}"
        )
      )
    )

    stubFor(get("/schemas/ids/111")
      .willReturn(aResponse()
        .withStatus(200)
        .withBody("{\"schema\":\"" + escapedSchema + "\"}")
      )
    )
  }
}
