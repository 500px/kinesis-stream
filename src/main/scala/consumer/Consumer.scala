package consumer

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, MergeHub, Source}
import akka.stream.{ActorMaterializer, KillSwitches}
import consumer.checkpoint.CheckpointConfig
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import scala.concurrent.{ExecutionContext, Future}

object Consumer {

  def source(streamName: String, config: ConsumerConfig)(
      implicit am: ActorMaterializer,
      system: ActorSystem,
      ec: ExecutionContext): Source[Record, Future[Done]] = {

    MergeHub
      .source[Record](perProducerBufferSize = 1)
      .viaMat(KillSwitches.single)(Keep.both)
      .watchTermination()(Keep.both)
      .mapMaterializedValue {
        case ((publishSink, killSwitch), terminationFuture) => {
          val scheduler =
            StreamScheduler(streamName, config)(publishSink,
                                                killSwitch,
                                                terminationFuture)
          scheduler.start()
        }
      }
  }

  def source(streamName: String, appName: String)(
      implicit am: ActorMaterializer,
      system: ActorSystem,
      ec: ExecutionContext): Source[Record, Future[Done]] =
    source(streamName, ConsumerConfig(appName))
}

case class ConsumerConfig(name: String,
                          workerId: String,
                          checkpointConfig: CheckpointConfig,
                          kinesisClient: KinesisAsyncClient,
                          dynamoClient: DynamoDbAsyncClient,
                          cloudwatchClient: CloudWatchAsyncClient)

object ConsumerConfig {
  def apply(name: String): ConsumerConfig = {

    val kinesisClient =
      KinesisAsyncClient.builder.build()
    val dynamoClient = DynamoDbAsyncClient.builder.build()
    val cloudWatchClient =
      CloudWatchAsyncClient.builder.build()

    ConsumerConfig(name,
                   generateWorkerId(),
                   CheckpointConfig(),
                   kinesisClient,
                   dynamoClient,
                   cloudWatchClient)
  }

  private def generateWorkerId(): String = UUID.randomUUID().toString
}
