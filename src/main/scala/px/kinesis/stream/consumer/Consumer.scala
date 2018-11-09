package px.kinesis.stream.consumer

import java.text.DateFormat
import java.util.{Date, UUID}

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, MergeHub, Source}
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.util.Timeout
import com.typesafe.config.Config
import px.kinesis.stream.consumer.checkpoint.CheckpointConfig
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{
  InitialPositionInStream,
  InitialPositionInStreamExtended
}
import software.amazon.kinesis.coordinator.CoordinatorConfig
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.metrics.MetricsConfig

import scala.concurrent.duration._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.concurrent.{ExecutionContext, Future}

object Consumer {

  /**
    * Returns a Source which emits checkpointable Records from the Kinesis stream.
    * The stream will contain messages from all shard assignments for the KCL worker
    * A KCL worker is started upon stream materialization and shut down upon stream completin/termination
    * @param config
    * @param am
    * @param system
    * @param ec
    * @return
    */
  def source(config: ConsumerConfig)(
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
            StreamScheduler(config)(publishSink, killSwitch, terminationFuture)
          scheduler.start()
        }
      }
  }

  def source(streamName: String, appName: String)(
      implicit am: ActorMaterializer,
      system: ActorSystem,
      ec: ExecutionContext): Source[Record, Future[Done]] =
    source(ConsumerConfig(streamName, appName))
}

case class ConsumerConfig(
    streamName: String,
    appName: String,
    workerId: String,
    checkpointConfig: CheckpointConfig,
    kinesisClient: KinesisAsyncClient,
    dynamoClient: DynamoDbAsyncClient,
    cloudwatchClient: CloudWatchAsyncClient,
    initialPositionInStreamExtended: InitialPositionInStreamExtended =
      InitialPositionInStreamExtended.newInitialPosition(
        InitialPositionInStream.LATEST),
    coordinatorConfig: Option[CoordinatorConfig] = None,
    leaseManagementConfig: Option[LeaseManagementConfig] = None,
    metricsConfig: Option[MetricsConfig] = None) {

  def withInitialStreamPosition(
      position: InitialPositionInStream): ConsumerConfig = {
    this.copy(
      initialPositionInStreamExtended =
        InitialPositionInStreamExtended.newInitialPosition(position))
  }

  def withInitialStreamPositionAtTimestamp(time: Date): ConsumerConfig =
    this.copy(
      initialPositionInStreamExtended =
        InitialPositionInStreamExtended.newInitialPositionAtTimestamp(time))

  def withCoordinatorConfig(config: CoordinatorConfig): ConsumerConfig =
    this.copy(coordinatorConfig = Some(config))

  def withLeaseManagementConfig(config: LeaseManagementConfig): ConsumerConfig =
    this.copy(leaseManagementConfig = Some(config))
  def withMetricsConfig(config: MetricsConfig): ConsumerConfig =
    this.copy(metricsConfig = Some(config))
}

object ConsumerConfig {
  def apply(streamName: String, appName: String): ConsumerConfig = {

    val kinesisClient =
      KinesisAsyncClient.builder.build()
    val dynamoClient = DynamoDbAsyncClient.builder.build()
    val cloudWatchClient =
      CloudWatchAsyncClient.builder.build()

    withNames(streamName, appName)(kinesisClient,
                                   dynamoClient,
                                   cloudWatchClient)
  }

  def withNames(streamName: String, appName: String)(
      implicit kinesisAsyncClient: KinesisAsyncClient,
      dynamoDbAsyncClient: DynamoDbAsyncClient,
      cloudWatchAsyncClient: CloudWatchAsyncClient): ConsumerConfig =
    ConsumerConfig(streamName,
                   appName,
                   generateWorkerId(),
                   CheckpointConfig(),
                   kinesisAsyncClient,
                   dynamoDbAsyncClient,
                   cloudWatchAsyncClient)

  def fromConfig(config: Config)(
      implicit kinesisAsyncClient: KinesisAsyncClient = null,
      dynamoDbAsyncClient: DynamoDbAsyncClient = null,
      cloudWatchAsyncClient: CloudWatchAsyncClient = null) = {
    def getOpt[A](key: String, lookup: String => A): Option[A] =
      if (config.hasPath(key)) Some(lookup(key)) else None
    def getIntOpt(key: String) = getOpt(key, config.getInt)
    def getStringOpt(key: String) = getOpt(key, config.getString)
    def getDuration(key: String): FiniteDuration =
      FiniteDuration(config.getDuration(key).toMillis, MILLISECONDS)
    def getDurationOpt(key: String) = getOpt(key, getDuration)

    val StreamPositionLatest = "latest"
    val StreamPositionHorizon = "trim-horizon"
    val StreamPositionTimestamp = "at-timestamp"

    val streamName = config.getString("stream-name")
    val name = config.getString("application-name")

    val latestPos = InitialPositionInStreamExtended.newInitialPosition(
      InitialPositionInStream.LATEST)

    val streamPosition = getStringOpt("position.initial")
      .map {
        case StreamPositionLatest => latestPos
        case StreamPositionHorizon =>
          InitialPositionInStreamExtended.newInitialPosition(
            InitialPositionInStream.TRIM_HORIZON)
        case StreamPositionTimestamp =>
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(
            DateFormat.getInstance().parse(config.getString("position.time")))
      }
      .getOrElse(latestPos)

    val completionTimeout = getDurationOpt("checkpoint.completion-timeout")
      .map(d => Timeout(d))
      .getOrElse(Timeout(30.seconds))
    val timeout = getDurationOpt("checkpoint.timeout")
      .map(d => Timeout(d))
      .getOrElse(Timeout(20.seconds))
    val maxBufferSize =
      getIntOpt("checkpoint.max-buffer-size").getOrElse(10000)
    val maxDurationInSeconds = getDurationOpt("checkpoint.max-duration")
      .map(d => d.toSeconds.toInt)
      .getOrElse(60)
    val checkpointConfig =
      CheckpointConfig(completionTimeout,
                       maxBufferSize,
                       maxDurationInSeconds,
                       timeout)

    val kinesisClient =
      if (kinesisAsyncClient == null) KinesisAsyncClient.builder.build()
      else kinesisAsyncClient
    val dynamoClient =
      if (dynamoDbAsyncClient == null) DynamoDbAsyncClient.builder.build()
      else dynamoDbAsyncClient
    val cloudWatchClient =
      if (cloudWatchAsyncClient == null) CloudWatchAsyncClient.builder.build()
      else cloudWatchAsyncClient

    ConsumerConfig(streamName,
                   name,
                   generateWorkerId(),
                   checkpointConfig,
                   kinesisClient,
                   dynamoClient,
                   cloudWatchClient,
                   streamPosition)
  }

  private def generateWorkerId(): String = UUID.randomUUID().toString
}
