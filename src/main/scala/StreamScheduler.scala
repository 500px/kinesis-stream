import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import akka.{Done, NotUsed}
import akka.stream.scaladsl.Sink
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler

import scala.concurrent.{ExecutionContext, Future, blocking}

class StreamScheduler(streamName: String, appName: String, workerId: String)(
    publishSink: Sink[Record, NotUsed],
    terminationFuture: Future[Done])(implicit kinesisClient: KinesisAsyncClient,
                                     dynamoClient: DynamoDbAsyncClient,
                                     cloudwatchClient: CloudWatchAsyncClient,
                                     am: ActorMaterializer,
                                     system: ActorSystem,
                                     ec: ExecutionContext,
                                     logging: LoggingAdapter) {

  // TODO: incorporate kill switch - Should shut down worker,  should be triggered by record processors when failure occurs
  // TODO: Use workerstatechangelistener for shutdown future
  // TODO: Implement non actor based checkpoint tracker

  private val scheduler: Scheduler =
    createScheduler(streamName,
                    appName,
                    kinesisClient,
                    dynamoClient,
                    cloudwatchClient,
                    workerId)(publishSink, terminationFuture)

  def start(): Future[Done] =
    startSchedulerAndRegisterShutdown(SchedulerExecutionContext.Global)

  private def startSchedulerAndRegisterShutdown(
      implicit ec: ExecutionContext): Future[Done] = {
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

  private def shutdownScheduler(scheduler: Scheduler)(
      implicit ec: ExecutionContext): Future[Done] = {
    // TODO: Use workerSTateChangeListener
    Future {
      blocking {
        scheduler.createGracefulShutdownCallable().call()
        Done
      }
    }
  }

  private def createScheduler(streamName: String,
                              appName: String,
                              kinesisClient: KinesisAsyncClient,
                              dynamoClient: DynamoDbAsyncClient,
                              cloudwatchClient: CloudWatchAsyncClient,
                              workerId: String)(
      publishSink: Sink[Record, NotUsed],
      terminationFuture: Future[Done]) = {

    val configsBuilder = new ConfigsBuilder(
      streamName,
      appName,
      kinesisClient,
      dynamoClient,
      cloudwatchClient,
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
      configsBuilder
        .processorConfig()
        .callProcessRecordsEvenForEmptyRecordList(true),
      configsBuilder.retrievalConfig()
    )
  }

}

object StreamScheduler {
  def apply(streamName: String, appName: String, workerId: String)(
      publishSink: Sink[Record, NotUsed],
      terminationFuture: Future[Done])(
      implicit kinesisClient: KinesisAsyncClient,
      dynamoClient: DynamoDbAsyncClient,
      cloudwatchClient: CloudWatchAsyncClient,
      am: ActorMaterializer,
      system: ActorSystem,
      ec: ExecutionContext,
      logging: LoggingAdapter): StreamScheduler =
    new StreamScheduler(streamName, appName, workerId)(publishSink,
                                                       terminationFuture)
}
