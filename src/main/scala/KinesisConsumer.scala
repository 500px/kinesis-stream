import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, MergeHub, Source}
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
      ec: ExecutionContext,
      logging: LoggingAdapter): Source[Record, Future[Done]] = {
    MergeHub
      .source[Record](perProducerBufferSize = 1)
      .watchTermination()(Keep.both)
      .mapMaterializedValue {
        case (publishSink, terminationFuture) => {
          val scheduler =
            StreamScheduler(streamName, appName, workerId)(publishSink,
                                                           terminationFuture)
          scheduler.start()
        }
      }
  }
}
