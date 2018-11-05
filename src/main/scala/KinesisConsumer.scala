import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, MergeHub, Source}
import akka.stream.{ActorMaterializer, KillSwitches}
import checkpoint.CheckpointConfig
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import scala.concurrent.{ExecutionContext, Future}

object KinesisConsumer {

  def source(streamName: String, appName: String, workerId: String)(
      implicit kinesisClient: KinesisAsyncClient,
      dynamoClient: DynamoDbAsyncClient,
      cloudwatchClient: CloudWatchAsyncClient,
      am: ActorMaterializer,
      system: ActorSystem,
      ec: ExecutionContext): Source[Record, Future[Done]] = {

    val checkpointConfig = CheckpointConfig()

    MergeHub
      .source[Record](perProducerBufferSize = 1)
      .viaMat(KillSwitches.single)(Keep.both)
      .watchTermination()(Keep.both)
      .mapMaterializedValue {
        case ((publishSink, killSwitch), terminationFuture) => {
          val scheduler =
            StreamScheduler(streamName, appName, workerId, checkpointConfig)(
              publishSink,
              killSwitch,
              terminationFuture)
          scheduler.start()
        }
      }
  }
}
