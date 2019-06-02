package com.adelpozo.streaming.utils.docker

import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker, LogLineReceiver}

import scala.concurrent.duration._


trait DockerKuduService extends DockerKit {

  val kuduContainer = DockerContainer(image = "usuresearch/kudu-docker-slim", name = Some("apache-kudu"))
    .withPorts(8050 -> Some(8050), 8051 -> Some(8051), 7050 -> Some(7050), 7051 -> Some(7051))
    .withEnv("KUDU_MASTER_EXTRA_OPTS=--webserver_advertised_addresses localhost:8051 --rpc_advertised_addresses localhost:7051",
             "KUDU_TSERVER_EXTRA_OPTS=--webserver_advertised_addresses localhost:8050 --rpc_advertised_addresses localhost:7050")
    //.withLogLineReceiver(new LogLineReceiver(true, println))
    .withReadyChecker(
      DockerReadyChecker.HttpResponseCode(port = 8050, code = 200).within(100.millis).looped(120, 500.millis)
    )

  abstract override def dockerContainers: List[DockerContainer] = kuduContainer :: super.dockerContainers

}
