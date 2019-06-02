# Spark Streaming pipeline template (Consume and produce)

This is a simple project template about Spark Streaming Pipeline application.

## Functionality

It will be very simple:

1. I expect a simple string from Kafka as follows: __000120190501__. Where the format is:
    - NNNN: Information person code
    - YYYYMMDD: The date when it was saved
2. There is a simple parse logic to convert the simple raw string in a understandable DataSet of Info.
3. After that, I want to retrieve the full name from Kudu table by Info parsed object
4. Once I have the full name, I want to build specific avro object in order to produce self.
5. Finally, I want to produce this avro object.

## The most important thing. I want to test the streaming!!

Actually, I want to put the focus in a entire test of streaming.

But there are two kinds of tests:

-  Narrow (Mock way in order to have a integrate test with kafka, kudu, Confluent and Spark)
-  Unit (pending, I'm sorry)

By default, unit test tag is active. The profile is **default-test**.

To launch an integration test you will need specified the next profile **narrow-test**

```bash
mvn clean test -Pnarrow-test
```

Or you can to run the class com.adelpozo.streaming.NarrowITTest in your favourite IDE.

### Info needed before launch Narrow Test.

At the moment, I'm using docker-testkit to have docker with the next containers; kudu and kafka which will be released from ScalaTest cycle.

It is highly recommended to download the following images before launching the test. Since depending on the network, it could give timeout when trying to pull in the test.

Kafka Image used: spotify/kafka
Kudu Image used: usuresearch/kudu-docker-slim

The scala classes related in order to run the containers are:
- com.adelpozo.streaming.utils.docker.DockerKuduService
- com.adelpozo.streaming.utils.docker.DockerKafkaService

I think so the test it is very simple to understand and if you want to explore, it  is very simple to tunning.



