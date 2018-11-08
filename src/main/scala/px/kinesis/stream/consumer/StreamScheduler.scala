package px.kinesis.stream.consumer

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, KillSwitch}
import akka.{Done, NotUsed}
import px.kinesis.stream.consumer.checkpoint.CheckpointTracker
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.events.TaskExecutionListenerInput
import software.amazon.kinesis.lifecycle.{TaskExecutionListener, TaskType}

import scala.concurrent.{ExecutionContext, Future, blocking}

class StreamScheduler(config: ConsumerConfig)(
    publishSink: Sink[Record, NotUsed],
    killSwitch: KillSwitch,
    terminationFuture: Future[Done])(implicit am: ActorMaterializer,
                                     system: ActorSystem,
                                     ec: ExecutionContext) {

  implicit val logging = Logging(system, "Kinesis")

  private val tracker =
    CheckpointTracker(config.workerId, config.checkpointConfig)

  private val scheduler: Scheduler =
    createScheduler(config)(publishSink, killSwitch)

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

  private def createScheduler(config: ConsumerConfig)(
      publishSink: Sink[Record, NotUsed],
      killSwitch: KillSwitch) = {

    val configsBuilder = new ConfigsBuilder(
      config.streamName,
      config.appName,
      config.kinesisClient,
      config.dynamoClient,
      config.cloudwatchClient,
      config.workerId,
      new RecordProcessorFactoryImpl(publishSink,
                                     config.workerId,
                                     tracker,
                                     killSwitch)(am, ec, logging)
    )

    new Scheduler(
      configsBuilder.checkpointConfig(),
      config.coordinatorConfig.getOrElse(configsBuilder.coordinatorConfig()),
      config.leaseManagementConfig.getOrElse(
        configsBuilder.leaseManagementConfig()),
      configsBuilder
        .lifecycleConfig()
        .taskExecutionListener(new ShardShutdownListener(tracker)),
      config.metricsConfig.getOrElse(configsBuilder.metricsConfig()),
      configsBuilder
        .processorConfig()
        .callProcessRecordsEvenForEmptyRecordList(true),
      configsBuilder
        .retrievalConfig()
        .initialPositionInStreamExtended(config.initialPositionInStreamExtended)
    )
  }

}

object StreamScheduler {
  def apply(config: ConsumerConfig)(publishSink: Sink[Record, NotUsed],
                                    killSwitch: KillSwitch,
                                    terminationFuture: Future[Done])(
      implicit am: ActorMaterializer,
      system: ActorSystem,
      ec: ExecutionContext): StreamScheduler =
    new StreamScheduler(config)(publishSink, killSwitch, terminationFuture)
}

/**
  * Used to detect when ShardRecordProcessors are shut down (due to shard end, lease lost.. etc)
  * When this occurs, we can clean up the corresponding consumer.checkpoint tracker associated with the shard
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
