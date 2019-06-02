package com.adelpozo.streaming.utils.docker

import com.adelpozo.streaming.tags.NarrowTestsSuite
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import org.scalatest._
import org.scalatest.time._
//import scala.concurrent.duration._

@NarrowTestsSuite
class AllAtOnceSpec extends FlatSpec with Matchers with DockerTestKit with DockerKitDockerJava
  with DockerKuduService with DockerKafkaService {
//
//  override val PullImagesTimeout = 120.minutes
//  override val StartContainersTimeout = 120.seconds
//  override val StopContainersTimeout = 120.seconds

  implicit val pc = PatienceConfig(Span(20, Seconds), Span(1, Second))


  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }


  "all containers" should "be ready at the same time" in {
    dockerContainers.map(_.image).foreach(println)
    dockerContainers.forall(_.isReady().futureValue) shouldBe true
  }

}
