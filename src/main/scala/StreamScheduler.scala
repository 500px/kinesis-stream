import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.{Done, NotUsed}
import akka.stream.scaladsl.Sink
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import scala.concurrent.{ExecutionContext, Future}

class StreamScheduler(streamName: String,
                      appName: String,
                      kinesisClient: KinesisAsyncClient,
                      dynamoClient: DynamoDbAsyncClient,
                      cloudwatchClient: CloudWatchAsyncClient,
                      workerId: String)(publishSink: Sink[Record, NotUsed],
                                        terminationFuture: Future[Done])(
    implicit am: ActorMaterializer,
    system: ActorSystem,
    ec: ExecutionContext) {}
