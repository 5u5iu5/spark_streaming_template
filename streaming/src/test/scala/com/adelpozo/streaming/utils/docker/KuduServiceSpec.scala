package com.adelpozo.streaming.utils.docker

import com.adelpozo.streaming.tags.NarrowTestsSuite
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import org.apache.kudu.client.KuduClient
import org.scalatest._
import org.scalatest.time._

@NarrowTestsSuite
class KuduServiceSpec extends FlatSpec with Matchers with DockerTestKit with DockerKitDockerJava
  with DockerKuduService {

  implicit val pc = PatienceConfig(Span(20, Seconds), Span(1, Second))

  "kudu container" should "be ready" in {
    isContainerReady(kuduContainer).futureValue shouldBe true
    kuduContainer.isRunning().futureValue shouldBe true
    kuduContainer.bindPorts.get(7051) should not be empty
    kuduContainer.getIpAddresses().futureValue should not be (Seq.empty)
  }

  "kudu" should "be ready" in {
    val kuduClient = new KuduClient.KuduClientBuilder("localhost").build()
    kuduClient.getTablesList.getTablesList.size() shouldBe 0
    kuduClient.close()
  }
  //
}
