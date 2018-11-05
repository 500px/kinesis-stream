import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, KillSwitch}
import akka.{Done, NotUsed}
import checkpoint.{CheckpointConfig, CheckpointTracker}
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.events.TaskExecutionListenerInput
import software.amazon.kinesis.lifecycle.{TaskExecutionListener, TaskType}

import scala.concurrent.{ExecutionContext, Future, blocking}

class StreamScheduler(streamName: String,
                      appName: String,
                      workerId: String,
                      checkpointConfig: CheckpointConfig)(
    publishSink: Sink[Record, NotUsed],
    killSwitch: KillSwitch,
    terminationFuture: Future[Done])(implicit kinesisClient: KinesisAsyncClient,
                                     dynamoClient: DynamoDbAsyncClient,
                                     cloudwatchClient: CloudWatchAsyncClient,
                                     am: ActorMaterializer,
                                     system: ActorSystem,
                                     ec: ExecutionContext) {

  implicit val logging = Logging(system, "Kinesis")

  private val tracker = CheckpointTracker(workerId, checkpointConfig)

  private val scheduler: Scheduler =
    createScheduler(streamName,
                    appName,
                    kinesisClient,
                    dynamoClient,
                    cloudwatchClient,
                    workerId)(publishSink, killSwitch)

  def start(): Future[Done] =
    startSchedulerAndRegisterShutdown(SchedulerExecutionContext.Global)

  private def startSchedulerAndRegisterShutdown(
      implicit ec: ExecutionContext): Future[Done] = {
    ec.execute(scheduler)

    terminationFuture
      .recoverWith {
        case ex: Throwable =>
          logging.error(ex, "Shutting down Scheduler due to failure")
          shutdown(scheduler)
      }
      .flatMap(_ =>
        if (!scheduler.gracefuleShutdownStarted()) {
          logging.info("Shutting down Scheduler due to stream completion")
          shutdown(scheduler)
        } else Future.successful(Done))
  }

  private def shutdown(scheduler: Scheduler)(
      implicit ec: ExecutionContext): Future[Done] = {
    // TODO: Use workerStateChangeListener
    val done = Future {
      blocking {
        scheduler.createGracefulShutdownCallable().call()
        Done
      }
    }

    // shutdown tracker after the graceful shutdown of worker completes
    // this ensures the tracker does not go down before shutdown based checkpointing for ShardConsumers happens
    done.foreach(_ => tracker.shutdown())
    done
  }

  private def createScheduler(streamName: String,
                              appName: String,
                              kinesisClient: KinesisAsyncClient,
                              dynamoClient: DynamoDbAsyncClient,
                              cloudwatchClient: CloudWatchAsyncClient,
                              workerId: String)(
      publishSink: Sink[Record, NotUsed],
      killSwitch: KillSwitch) = {

    val configsBuilder = new ConfigsBuilder(
      streamName,
      appName,
      kinesisClient,
      dynamoClient,
      cloudwatchClient,
      workerId,
      new RecordProcessorFactoryImpl(publishSink,
                                     workerId,
                                     tracker,
                                     killSwitch)(am, ec, logging)
    )

    new Scheduler(
      configsBuilder.checkpointConfig(),
      configsBuilder.coordinatorConfig(),
      configsBuilder.leaseManagementConfig(),
      configsBuilder
        .lifecycleConfig()
        .taskExecutionListener(new ShardShutdownListener(tracker)),
      configsBuilder.metricsConfig(),
      configsBuilder
        .processorConfig()
        .callProcessRecordsEvenForEmptyRecordList(true),
      configsBuilder.retrievalConfig()
    )
  }

}

object StreamScheduler {
  def apply(streamName: String,
            appName: String,
            workerId: String,
            checkpointConfig: CheckpointConfig)(
      publishSink: Sink[Record, NotUsed],
      killSwitch: KillSwitch,
      terminationFuture: Future[Done])(
      implicit kinesisClient: KinesisAsyncClient,
      dynamoClient: DynamoDbAsyncClient,
      cloudwatchClient: CloudWatchAsyncClient,
      am: ActorMaterializer,
      system: ActorSystem,
      ec: ExecutionContext): StreamScheduler =
    new StreamScheduler(streamName, appName, workerId, checkpointConfig)(
      publishSink,
      killSwitch,
      terminationFuture)
}

/**
  * Used to detect when ShardRecordProcessors are shut down (due to shard end, lease lost.. etc)
  * When this occurs, we can clean up the corresponding checkpoint tracker associated with the shard
  * @param tracker
  */
class ShardShutdownListener(tracker: CheckpointTracker)
    extends TaskExecutionListener {
  override def beforeTaskExecution(input: TaskExecutionListenerInput): Unit = ()

  override def afterTaskExecution(input: TaskExecutionListenerInput): Unit = {
    if (input.taskType() == TaskType.SHUTDOWN || input
          .taskType() == TaskType.SHUTDOWN_COMPLETE) {
      // note: we are just doing a fire and forget shutdown
      // there is a final cleanup as part of the scheduler shut down
      tracker.shutdown(input.shardInfo().shardId())
    }
  }
}
