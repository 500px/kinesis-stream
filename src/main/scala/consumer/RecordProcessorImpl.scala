package consumer

import akka.Done
import akka.event.LoggingAdapter
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.{KillSwitch, QueueOfferResult}
import consumer.checkpoint.CheckpointTracker
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.{
  RecordProcessorCheckpointer,
  ShardRecordProcessor
}
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
class RecordProcessorImpl(
    queue: SourceQueueWithComplete[Seq[Record]],
    tracker: CheckpointTracker,
    killSwitch: KillSwitch,
    workerId: String)(implicit ec: ExecutionContext, logging: LoggingAdapter)
    extends ShardRecordProcessor {

  var shardId: String = _

  override def initialize(initializationInput: InitializationInput): Unit = {
    logging.info("Started consumer.Record Processor {} for Worker: {}",
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
          // terminate stream, should never get into this condition
          killSwitch.abort(
            new AssertionError(
              "queue must use OverflowStrategy.Backpressure"
            ))
        case QueueOfferResult.Failure(e) =>
          // failed to enqueue, fail stream
          killSwitch.abort(e)
      }
    } catch {
      case ex: Throwable =>
        // offer failed, kill stream
        killSwitch.abort(ex)
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
    logging.debug("Lease lost: {}", shardId)
  }

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    logging.info("Shard Ended: {}", shardId)
    checkpointForShardEnd(shardEndedInput.checkpointer())
  }

  override def shutdownRequested(
      shutdownRequestedInput: ShutdownRequestedInput): Unit = {
    logging.debug("Shutdown Requested: {}", shardId)
    checkpointForShutdown(shutdownRequestedInput.checkpointer())
    // since we are shutting down this processor, lets complete the queue associated with it since this processor will
    // not emit any further records
    // the materialized stream associated with this shard will complete, freeing up resources
    queue.complete()
  }

  def checkpointIfNeeded(checkpointer: RecordProcessorCheckpointer): Unit =
    blockAndTerminateOnFailure(
      "consumer/checkpoint",
      tracker.checkpointIfNeeded(shardId, checkpointer))

  def checkpointForShardEnd(checkpointer: RecordProcessorCheckpointer): Unit = {
    // wait for all in flight to be marked processed
    // we then use the .consumer.checkpoint() variant to consumer.checkpoint as this is required for shard end
    // if we can't meet conditions to call .consumer.checkpoint(), then fail

    val completion = tracker
      .watchCompletion(shardId)
      .map { _ =>
        checkpointer.checkpoint()
        Done
      }

    blockAndTerminateOnFailure("checkpointForShardEnd", completion)
  }

  def checkpointForShutdown(checkpointer: RecordProcessorCheckpointer): Unit = {
    logging.info("Starting consumer.checkpoint for Shutdown {}", shardId)
    // wait for all in flight to be marked processed

    val completion = tracker
      .watchCompletion(shardId)
      .flatMap(_ => tracker.checkpoint(shardId, checkpointer))
      .recover { case _ => Done }

    blockAndTerminateOnFailure("checkpointForShutdown", completion)
  }

  def blockAndTerminateOnFailure[A](name: String,
                                    future: Future[A]): Either[Throwable, A] = {
    Try(Await.result(future, Duration.Inf)).toEither.left.map { ex =>
      logging.error(ex, s"Failed on $name")
      killSwitch.abort(ex)
      ex
    }
  }

}
