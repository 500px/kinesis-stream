import java.util.UUID

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

object Main extends App {

  implicit val system = ActorSystem("kinesis-source")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()
  implicit val logging: LoggingAdapter = Logging(system, "Example")

  // A simple consumer that will print to the console for now
  val consumer = Sink.foreach[String](s => logging.info(s))

  val region = Region.of("us-east-1")
  implicit val kinesisAsyncClient =
    KinesisAsyncClient.builder.region(region).build()
  implicit val dynamoClient = DynamoDbAsyncClient.builder.region(region).build()
  implicit val cloudWatchClient =
    CloudWatchAsyncClient.builder.region(region).build()
  val streamName = "activity-test"
  val appName = "test-kcl-3"

  val workerId = UUID.randomUUID().toString()

  // Attach a MergeHub Source to the consumer. This will materialize to a
  // corresponding Sink.
  val runnableGraph =
    KinesisConsumer
      .source(streamName, appName, workerId)
      .take(15)
      .mapAsyncUnordered(1)(r => r.markProcessed().map(_ => r))
      .map(r => s"${r.sequenceNumber} /${r.subSequenceNumber} - ${r.key}")
      .to(consumer)

  val done = runnableGraph.run()
  done.onComplete(_ => logging.info("Shutdown completed"))
}
