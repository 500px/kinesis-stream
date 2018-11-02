import akka.Done
import akka.event.LoggingAdapter
import akka.stream.QueueOfferResult
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.Timeout
import checkpoint.CheckpointTracker
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.{
  RecordProcessorCheckpointer,
  ShardRecordProcessor
}
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
class RecordProcessorImpl(queue: SourceQueueWithComplete[Seq[Record]],
                          tracker: CheckpointTracker,
                          terminationFuture: Future[Done],
                          workerId: String,
                          logging: LoggingAdapter)
    extends ShardRecordProcessor {

  var shardId: String = _
  val shutdownTimeout = Timeout(20.seconds)

  override def initialize(initializationInput: InitializationInput): Unit = {
    logging.info("Started Record Processor {} for Worker: {}",
                 initializationInput.shardId(),
                 workerId)
    shardId = initializationInput.shardId()
  }

  override def processRecords(
      processRecordsInput: ProcessRecordsInput): Unit = {
    val records = transformRecords(processRecordsInput.records())
    trackRecords(records)
    checkpointIfNeeded(processRecordsInput.checkpointer())
    enqueueRecords(records)
  }

  def transformRecords(
      kRecords: java.util.List[KinesisClientRecord]): Seq[Record] = {
    kRecords.asScala.map(kr => Record.from(kr, shardId, tracker)).toIndexedSeq
  }

  def trackRecords(records: Seq[Record]): Unit =
    blockAndTerminateOnFailure(
      "trackRecords",
      tracker.track(shardId, records.map(_.extendedSequenceNumber)))

  def enqueueRecords(records: Seq[Record]) = {

    try {
      val offerResult = Await.result(queue.offer(records), Duration.Inf)

      offerResult match {
        case QueueOfferResult.Enqueued =>
        // Do nothing.

        case QueueOfferResult.QueueClosed =>
        // Do nothing.

        case QueueOfferResult.Dropped =>
          // terminate parent, should never be dropping messages
          logging.error("Queue result dropped!")
        case QueueOfferResult.Failure(e) =>
          // terminate parent
          logging.error(e, "enqueue failure")
      }
    } catch {
      case ex: Throwable =>
        logging.error(ex, "Queue Offer Future Failed")
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
    logging.info("Lease lost: {}", shardId)
  }

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    logging.info("Shard Ended: {}", shardId)
    checkpointForShardEnd(shardEndedInput.checkpointer())
  }

  override def shutdownRequested(
      shutdownRequestedInput: ShutdownRequestedInput): Unit = {
    logging.info("Shutdown Requested: {}", shardId)
    checkpointForShutdown(shutdownRequestedInput.checkpointer())

  }

  def checkpointIfNeeded(checkpointer: RecordProcessorCheckpointer): Unit =
    blockAndTerminateOnFailure(
      "checkpoint",
      tracker.checkpointIfNeeded(shardId, checkpointer))

  def checkpointForShardEnd(checkpointer: RecordProcessorCheckpointer): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // wait for all in flight to be marked processed
    // we then use the .checkpoint() variant to checkpoint as this is required for shard end
    // if we can't meet conditions to call .checkpoint(), then fail

    val completion = tracker
      .watchCompletion(shardId, shutdownTimeout)
      .map { _ =>
        checkpointer.checkpoint()
        Done
      }

    blockAndTerminateOnFailure("checkpointForShardEnd", completion)
  }

  def checkpointForShutdown(checkpointer: RecordProcessorCheckpointer): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    logging.info("Starting checkpoint for Shutdown {}", shardId)
    // wait for all in flight to be marked processed

    val completion = tracker
      .watchCompletion(shardId, shutdownTimeout)
      .flatMap(_ => tracker.checkpoint(shardId, checkpointer))
      .recover { case _ => Done }

    blockAndTerminateOnFailure("checkpointForShutdown", completion)
  }

  def blockAndTerminateOnFailure[A](name: String,
                                    future: Future[A]): Either[Throwable, A] = {
    Try(Await.result(future, Duration.Inf)).toEither.left.map { ex =>
      logging.error(ex, s"Failed on $name")
      ex
    }
  }

  /**
    * Races two futures against eachother and runs flatmap using the function corresponding to the winner
    * @param fut1
    * @param fut2
    * @param first - runs if future 1 finishes first
    * @param second - runs if future 2 finishes first
    * @param ec
    * @tparam A
    * @tparam B
    * @return
    */
  def race2[A, B](fut1: Future[A], fut2: Future[A])(
      first: A => Future[B],
      second: A => Future[B])(implicit ec: ExecutionContext): Future[B] = {
    Future.firstCompletedOf(Seq(fut1.map((1, _)), fut2.map((2, _)))).flatMap {
      case (1, v) => first(v)
      case (_, v) => second(v)
    }
  }

}
