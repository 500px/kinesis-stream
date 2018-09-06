import java.util.UUID
import java.util.concurrent.TimeUnit

import CheckpointTrackerActor.{CheckpointIfNeeded, Get, Process, Track}
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, MergeHub, Sink}
import akka.util.Timeout
import akka.{Done, NotUsed}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.Iterable
import scala.concurrent.{ExecutionContext, Future, blocking}

object Main extends App {

  implicit val system = ActorSystem("kinesis-source")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()
  implicit val logging: LoggingAdapter = Logging(system, "HomeFeedIndexer")

  // A simple consumer that will print to the console for now
  val consumer = Sink.foreach[String](s => logging.info(s))

  val region = Region.of("us-east-1")
  val kinesisAsyncClient = KinesisAsyncClient.builder.region(region).build()
  val dynamoClient = DynamoDbAsyncClient.builder.region(region).build()
  val cloudWatchClient = CloudWatchAsyncClient.builder.region(region).build()
  val streamName = "activity-test"
  val appName = "test-kcl-2"

  val workerId = UUID.randomUUID().toString()

  // Attach a MergeHub Source to the consumer. This will materialize to a
  // corresponding Sink.
  val runnableGraph =
    MergeHub
      .source[Record](perProducerBufferSize = 1)
      .watchTermination()(Keep.both)
      .mapMaterializedValue {
        case (publishSink, terminationFuture) => {
          val scheduler =
            createScheduler(streamName,
                            appName,
                            kinesisAsyncClient,
                            dynamoClient,
                            cloudWatchClient,
                            workerId)(publishSink, terminationFuture)
          startSchedulerAndRegisterShutdown(scheduler, terminationFuture)(
            SchedulerExecutionContext.Global,
            logging)
        }
      }
      .take(15)
      .mapAsyncUnordered(1)(r => r.markProcessed().map(_ => r))
      .map(r => s"${r.sequenceNumber} /${r.subSequenceNumber} - ${r.key}")
      .to(consumer)

  val done = runnableGraph.run()
  done.onComplete(_ => logging.info("Shutdown completed"))

  def createScheduler(streamName: String,
                      appName: String,
                      kinesisClient: KinesisAsyncClient,
                      dynamoClient: DynamoDbAsyncClient,
                      cloudwatchClient: CloudWatchAsyncClient,
                      workerId: String)(publishSink: Sink[Record, NotUsed],
                                        terminationFuture: Future[Done])(
      implicit am: ActorMaterializer,
      system: ActorSystem,
      ec: ExecutionContext) = {
    val configsBuilder = new ConfigsBuilder(
      streamName,
      appName,
      kinesisAsyncClient,
      dynamoClient,
      cloudWatchClient,
      workerId,
      new RecordProcessorFactoryImpl(publishSink,
                                     workerId,
                                     terminationFuture,
                                     logging)(am, system, ec)
    )
    new Scheduler(
      configsBuilder.checkpointConfig(),
      configsBuilder.coordinatorConfig(),
      configsBuilder.leaseManagementConfig(),
      configsBuilder.lifecycleConfig(),
      configsBuilder.metricsConfig(),
      configsBuilder.processorConfig(),
      configsBuilder.retrievalConfig()
    )
  }

  def startSchedulerAndRegisterShutdown(scheduler: Scheduler,
                                        terminationFuture: Future[Done])(
      implicit ec: ExecutionContext,
      logging: LoggingAdapter): Future[Done] = {
    ec.execute(scheduler)
    terminationFuture
      .recoverWith {
        case ex: Throwable =>
          logging.error(ex, "Shutting down Scheduler due to failure")
          shutdownScheduler(scheduler)
      }
      .flatMap(_ =>
        if (!scheduler.gracefuleShutdownStarted()) {
          logging.info("Shutting down Scheduler due to stream completion")
          shutdownScheduler(scheduler)
        } else Future.successful(Done))
  }

  def shutdownScheduler(scheduler: Scheduler)(
      implicit ec: ExecutionContext): Future[Done] = {
    // TODO: Use workerSTateChangeListener
    Future {
      blocking {
        scheduler.createGracefulShutdownCallable().call()
        Done
      }
    }
  }
}
