package px.kinesis.stream.consumer

import akka.Done
import akka.event.LoggingAdapter
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.{KillSwitch, QueueOfferResult}
import px.kinesis.stream.consumer.checkpoint.{
  CheckpointTimeoutException,
  CheckpointTracker
}
import software.amazon.kinesis.exceptions.{
  KinesisClientLibDependencyException,
  ShutdownException,
  ThrottlingException
}
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
class RecordProcessorImpl(
    queue: SourceQueueWithComplete[Seq[Record]],
    tracker: CheckpointTracker,
    killSwitch: KillSwitch,
    workerId: String)(implicit ec: ExecutionContext, logging: LoggingAdapter)
    extends ShardRecordProcessor {

  val EnqueueBatchSize = 100
  var shardId: String = _
  var queueMarkedCompleted = false

  override def initialize(initializationInput: InitializationInput): Unit = {
    logging.info("Started Record Processor {} for Worker: {}",
                 initializationInput.shardId(),
                 workerId)
    blockAndThrowOnFailure(tracker.start(initializationInput.shardId()))
    shardId = initializationInput.shardId()
  }

  override def processRecords(
      processRecordsInput: ProcessRecordsInput): Unit = {
    abortStreamOnError("processRecords") {
      val records = transformRecords(processRecordsInput.records())
      trackRecords(records)
      checkpointIfNeeded(processRecordsInput.checkpointer())

      records.grouped(EnqueueBatchSize).foreach { r =>
        enqueueRecords(r)
        checkpointIfNeeded(processRecordsInput.checkpointer())
      }
    }
  }

  def transformRecords(
      kRecords: java.util.List[KinesisClientRecord]): Seq[Record] = {
    kRecords.asScala.map(kr => Record.from(kr, shardId, tracker)).toIndexedSeq
  }

  def trackRecords(records: Seq[Record]): Unit =
    blockAndThrowOnFailure(
      tracker.track(shardId, records.map(_.extendedSequenceNumber)))

  def enqueueRecords(records: Seq[Record]) = {
    if (continueProcessing()) {
      try {
        val offerResult = Await.result(queue.offer(records), Duration.Inf)

        offerResult match {
          case QueueOfferResult.Enqueued =>
          // Do nothing.

          case QueueOfferResult.QueueClosed =>
          // Do nothing.

          case QueueOfferResult.Dropped =>
            // terminate stream, should never get into this condition
            throw new AssertionError(
              "queue must use OverflowStrategy.Backpressure"
            )
          case QueueOfferResult.Failure(e) =>
            // failed to enqueue, fail stream
            throw e
        }
      } catch {
        case ex: Throwable =>
          // offer failed, kill stream
          logging.error("Enqueue failed for: {}", shardId)
          throw ex
      }
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {

    /**
      * Note: this can be called directly, without having shutdownRequested called first.
      * This can happen when the shard consumer state is in processing and then the lease is lost, the processor will
      * not get a shutdownRequested call in this case.
      */
    completeQueue() // clean up stream resources

    logging.info("Lease lost: {}", shardId)
  }

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {

    /**
      * Note: this can be called directly, without having shutdownRequested called first.
      * This can happen when the shard consumer state is in processing and has been given all records for the shard, the processor will
      * not get a shutdownRequested call in this case.
      */
    logging.info("Shard Ended: {}", shardId)
    completeQueue() // clean up stream resources

    checkpointForShardEnd(shardEndedInput.checkpointer())
  }

  override def shutdownRequested(
      shutdownRequestedInput: ShutdownRequestedInput): Unit = {
    logging.info("Shutdown Requested: {}", shardId)
    checkpointForShutdown(shutdownRequestedInput.checkpointer())
    // since we are shutting down this processor, lets complete the queue associated with it since this processor will
    // not emit any further records
    // the materialized stream associated with this shard will complete, freeing up resources
    completeQueue()
  }

  def checkpointIfNeeded(checkpointer: RecordProcessorCheckpointer): Unit = {
    if (continueProcessing()) {
      try {
        blockAndThrowOnFailure(
          tracker.checkpointIfNeeded(shardId, checkpointer))
      } catch {
        case ex: CheckpointTimeoutException =>
          logging.error("Timed out when trying to checkpoint: {}", shardId)
          throw ex
        case ex: ThrottlingException =>
          logging.error(ex, "Throttled on checkpoint: {}", shardId)
        case ex: KinesisClientLibDependencyException =>
          logging.error(ex, "KCLDependencyException on checkpoint: {}", shardId)
        case _: ShutdownException =>
          logging.error(
            "Attempting to checkpoint but lease was lost. Stopping processing for: {}",
            shardId)
          stopProcessing()
        case ex: Throwable =>
          throw ex
      }
    }
  }

  def checkpointForShardEnd(checkpointer: RecordProcessorCheckpointer): Unit = {
    logging.info("Starting checkpoint for shard end {}", shardId)
    // wait for all in flight to be marked processed
    // we then use the .consumer.checkpoint() variant to consumer.checkpoint as this is required for shard end
    // if we can't meet conditions to call .consumer.checkpoint(), then fail

    val completion = tracker
      .watchCompletion(shardId)
      .map { _ =>
        checkpointer.checkpoint()
        Done
      }

    abortStreamOnError("checkpointForShardEnd")(
      blockAndThrowOnFailure(completion))
  }

  def checkpointForShutdown(checkpointer: RecordProcessorCheckpointer): Unit = {
    logging.info("Starting checkpoint for Shutdown {}", shardId)
    // wait for all in flight to be marked processed

    val completion = tracker
      .watchCompletion(shardId)
      .flatMap(_ => tracker.checkpoint(shardId, checkpointer))
      .recover {
        case _ =>
          logging.warning("Unable to checkpoint for shutdown {}", shardId)
          // ignore this error as we may fail to checkpoint on shutdown
          Done
      }

    // Note: with the above recover, we are not expecting any errors
    abortStreamOnError("checkpointForShutdown")(
      blockAndThrowOnFailure(completion))
  }

  /**
    * Currently we just complete the queue when we want to stop processing.
    * Once this method is invoked, other methods which check if they should continue processing, will do nothing.
    *
    * This can only occur when the ShardConsumer is in the Processing state
    */
  def stopProcessing(): Unit = completeQueue()
  def continueProcessing(): Boolean = !queueMarkedCompleted

  /**
    * Called when we know no more processing will occur. Complete the queue associated with processor since it will
    * not emit any further records.
    * The materialized stream associated with this shard will complete, freeing up resources
    *
    */
  def completeQueue(): Unit = {
    if (!queueMarkedCompleted) {
      queue.complete()
      queueMarkedCompleted = true
    }
  }

  /**
    * Trigger the stream killswitch if the given block throws an exception
    * Note: this function will swallow the exception after logging it.
    * @param name
    * @param block
    */
  def abortStreamOnError(name: String)(block: => Unit): Unit = {
    try {
      block
    } catch {
      case ex: Throwable =>
        logging.error(ex, "Aborting on {} for shard {}", name, shardId)
        killSwitch.abort(ex)
    }
  }

  /**
    * Block and throw exception on failure
    * @param t
    * @tparam A
    */
  def blockAndThrowOnFailure[A](fut: Future[A]): Unit =
    Await.result(fut, Duration.Inf)
}
