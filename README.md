# Kinesis Stream
[![Build Status](https://travis-ci.com/500px/kinesis-stream.svg?branch=master)](https://travis-ci.com/500px/kinesis-stream)

A wrapper around KCL 2.x which exposes an Akka Streams source to consume messages off of a Kinesis Data Stream.

## Requirements
- Scala >= 2.12.x or >= 2.11.12
- Akka Streams >= 2.5.14

## Usage

**build.sbt**

```scala
libraryDependencies += "com.500px" %% "kinesis-stream_2.12" % "0.1.7"
```

**note**: Due to java package names not allowing numbers, the import path for the project is `px.kinesis.stream.consumer`.

### Simple Example (At Most Once Delivery)

In the following example, we consume from a stream named `test-stream` and name our kinesis application `test-app`.

*note: To test this example out, you will need the [required IAM policies](#required-iam-permissions).* 

```scala
import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink}
import px.kinesis.stream.consumer

import scala.concurrent.Future

object Main extends App {

  implicit val system = ActorSystem("kinesis-source")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()

  // A simple consumer that will print to the console for now
  val console = Sink.foreach[String](println)

  // In this example, commit as soon as we consume a record, giving us at-most-once delivery semantics.
  // To achieve at-least-once, we would commit after processing the record
  val runnableGraph: RunnableGraph[Future[Done]] =
    consumer
      .source("test-stream", "test-app")
      .via(consumer.commitFlow(parallelism = 2)) // convenience flow to commit records (marking records processed)
      .map(r => r.data.utf8String)
      .toMat(console)(Keep.left)

  val done = runnableGraph.run()
  done.onComplete(_ => {
    println("Shutdown completed")
    system.terminate()
  })

}

```

### Configuration using hocon

The Kinesis Consumer can be configured via HOCON configuration as is common for Akka projects

```hocon
example.consumer {
  application-name = "test-app" # name of the application (consumer group)
  stream-name = "test-stream" # name of the stream to connect to

  position {
    initial = "latest" # (latest, trim-horizon, at-timestamp). defaults to latest
    #time = "" # Only set if position is at-timestamp. Supports a valid Java Date parseable datetime string
  }

  checkpoint {
    #completion-timeout = "30s" # When a Shard End / Shutdown event occurs, this timeout determines how long to wait for all in-flight messages to be marked processed. Defaults to 30s
    #timeout = "20s" # timeout for checkpoints to complete. (Checkpoints are completed when the checkpoint actor accepts the checkpoint sequence number). defaults to 20s
    #max-buffer-size = 10000 # Maximum number of records to process before calling checkpoint (internally). All messages MUST be marked processed by client, before they can be considered for checkpointing.
    #max-duration = "60s" # Max duration to wait between checkpoint calls
  }
}
```

Configuring the consumer using `ConsumerConfig`.

```scala
//...

consumer.source(ConsumerConfig.fromConfig(system.settings.config.getConfig("example.consumer")))

```

### Configuring using native AWS SDK config

The `ConsumerConfig` class has methods for accepting raw AWS SDK clients which can be configured. If you require very
custom configuration, this option is available.

## Gotchas

### Checkpointing

You MUST ensure to mark all in-flight messages as processed. The checkpoint tracking component needs to keep track
of all in-flight messages and as such, could run out of memory if messages are not marked as processed regularly.

If you choose to do at-most-once processing, a simple solution is to mark a record as processed as soon as it arrives on
the stream.


## Required IAM Permissions

The following IAM permissions are required to use KCL 2.x

- `STREAM_ARN` - the ARN of the Kinesis stream being consumed from
- `DYNAMO_ARN` - the ARN of the DynamoDB table that the KCL will use for checkpointing
    - The name of the table will correspond to the `application name` set for the consumer
    
```json
{
    "PolicyName": "Kinesis-Consumer",
    "PolicyDocument": {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "kinesis:RegisterStreamConsumer",
            "kinesis:DescribeStreamSummary",
            "kinesis:ListShards",
            "kinesis:DescribeStream",
            "kinesis:GetShardIterator",
            "kinesis:GetRecords",
            "kinesis:PutRecord",
            "kinesis:PutRecords"
          ],
          "Resource": [
            "{STREAM_ARN}"
          ]
        },
        {
          "Effect": "Allow",
          "Action": [
            "kinesis:SubscribeToShard",
            "kinesis:ListStreams",
            "kinesis:DescribeStreamConsumer"
          ],
          "Resource": [
            "*"
          ]
        },
        {
          "Effect": "Allow",
          "Action": [
            "dynamodb:*"
          ],
          "Resource": [
            "{DYNAMO_ARN}"
          ]
        },
        {
          "Effect": "Allow",
          "Action": [
            "cloudwatch:PutMetricData"
          ],
          "Resource": [
            "*"
          ]
        }
      ]
    }
  }
```

# Improvements

- Implement a RecordProcessor which does not use a checkpoint tracker to allow for a simpler at-most-once processing.
