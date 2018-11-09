package px.kinesis.stream.consumer.checkpoint

import akka.Done
import akka.actor.{Actor, ActorSystem}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import px.kinesis.stream.consumer.checkpoint.CheckpointTrackerActor._
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.Iterable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class CheckpointTracker(
    workerId: String,
    maxBufferSize: Int,
    maxDurationInSeconds: Int,
    completionTimeout: Timeout,
    timeout: Timeout)(implicit system: ActorSystem, ec: ExecutionContext) {

  @volatile var isShutdown = false

  val tracker = system.actorOf(
    CheckpointTrackerActor
      .props(workerId, maxBufferSize, maxDurationInSeconds),
    s"tracker-${workerId.take(5)}")

  /**
    * Track a set of sequence numbers
    * Note: process should be called on a member of this set
    *
    * @param shardId
    * @param sequences
    * @return
    */
  def track(shardId: String, sequences: Iterable[ExtendedSequenceNumber]) = {
    tracker
      .ask(Track(shardId, sequences))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("track", shardId))
  }

  /**
    * Mark a sequence number as processed
    *
    * This sequence number is checkpointable if all sequence numbers before it are marked processed
    * If a processed sequence number has no sequence numbers before it in the tracked set, it is considered checkpointable
    * @param shardId
    * @param sequence
    * @return
    */
  def process(shardId: String, sequence: ExtendedSequenceNumber) = {
    tracker
      .ask(Process(shardId, sequence))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("process", shardId))
  }

  /**
    * Checkpoint only if conditions are met (enough time elapsed or buffer is full)
    * @param shardId
    * @param checkpointer
    * @return
    */
  def checkpointIfNeeded(shardId: String,
                         checkpointer: RecordProcessorCheckpointer) = {
    tracker
      .ask(CheckpointIfNeeded(shardId, checkpointer))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("checkpointIfNeeded", shardId))
  }

  /**
    * Forces checkpointing to occur for current highest checkpointable sequence number
    * @param shardId
    * @param checkpointer
    * @return
    */
  def checkpoint(shardId: String, checkpointer: RecordProcessorCheckpointer) = {
    tracker
      .ask(CheckpointIfNeeded(shardId, checkpointer, force = true))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("consumer/checkpoint", shardId))
  }

  /**
    * Returns a future which resolves successfully when all in flight (tracked) messages are marked processed
    * Fails if the underlying actor is shut down before the timeout elapses
    * @param shardId
    * @return
    */
  def watchCompletion(shardId: String) = {
    tracker
      .ask(WatchCompletion(shardId))(completionTimeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("watchCompletion", shardId))
  }

  /**
    * Shuts down the tracker for a particular shard
    * @param shardId
    * @return
    */
  def shutdown(shardId: String): Future[Done] = {
    if (!isShutdown) {
      tracker
        .ask(Shutdown(shardId))(timeout)
        .map(_ => Done)
        .recoverWith(mapAskTimeout("shutdown", shardId))
    } else Future.successful(Done)
  }

  /**
    * Shuts down tracker and all its shard trackers
    */
  def shutdown(): Unit = {
    if (!isShutdown) {
      isShutdown = true
      tracker.tell(Shutdown, Actor.noSender)
    }
  }

  private def mapAskTimeout[A](
      name: String,
      shardId: String): PartialFunction[Throwable, Future[A]] = {
    case _: AskTimeoutException =>
      Future.failed(
        CheckpointTimeoutException(
          s"$name took longer than: $timeout for $shardId"))
    case other => Future.failed(other)
  }
}

case class CheckpointTimeoutException(message: String)
    extends Exception(message)

object CheckpointTracker {
  def apply(
      workerId: String,
      maxBufferSize: Int,
      maxDurationInSeconds: Int,
      completionTimeout: Timeout,
      timeout: Timeout)(implicit system: ActorSystem, ec: ExecutionContext) =
    new CheckpointTracker(workerId,
                          maxBufferSize,
                          maxDurationInSeconds,
                          completionTimeout,
                          timeout)
  def apply(workerId: String, config: CheckpointConfig)(
      implicit system: ActorSystem,
      ec: ExecutionContext) =
    new CheckpointTracker(workerId,
                          config.maxBufferSize,
                          config.maxDurationInSeconds,
                          config.completionTimeout,
                          config.timeout)
}

case class CheckpointConfig(completionTimeout: Timeout = Timeout(30.seconds),
                            maxBufferSize: Int = 10000,
                            maxDurationInSeconds: Int = 60,
                            timeout: Timeout = Timeout(20.seconds))
