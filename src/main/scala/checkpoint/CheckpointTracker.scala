package checkpoint

import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{Actor, ActorSystem}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import checkpoint.CheckpointTrackerActor._
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.Iterable
import scala.concurrent.{ExecutionContext, Future}

class CheckpointTracker(workerId: String,
                        maxBufferSize: Int,
                        maxDurationInSeconds: Int)(implicit system: ActorSystem,
                                                   ec: ExecutionContext) {

  @volatile var isShutdown = false

  val tracker = system.actorOf(
    CheckpointTrackerActor.props(workerId, maxBufferSize, maxDurationInSeconds),
    s"tracker-${workerId.take(5)}")

  val timeout = Timeout(5, TimeUnit.SECONDS)

  def track(shardId: String, sequences: Iterable[ExtendedSequenceNumber]) = {
    tracker
      .ask(Track(shardId, sequences))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("track", shardId))
  }

  def process(shardId: String, sequence: ExtendedSequenceNumber) = {
    tracker
      .ask(Process(shardId, sequence))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("process", shardId))
  }

  def checkpointIfNeeded(shardId: String,
                         checkpointer: RecordProcessorCheckpointer) = {
    tracker
      .ask(CheckpointIfNeeded(shardId, checkpointer))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("checkpointIfNeeded", shardId))
  }

  def checkpoint(shardId: String, checkpointer: RecordProcessorCheckpointer) = {
    tracker
      .ask(CheckpointIfNeeded(shardId, checkpointer, force = true))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("checkpoint", shardId))
  }

  def watchCompletion(shardId: String, completionTimeout: Timeout) = {
    tracker
      .ask(WatchCompletion(shardId))(completionTimeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout("watchCompletion", shardId))
  }

  def shutdown(shardId: String): Future[Done] = {
    if (!isShutdown) {
      tracker
        .ask(Shutdown(shardId))(timeout)
        .map(_ => Done)
        .recoverWith(mapAskTimeout("shutdown", shardId))
    } else Future.successful(Done)
  }

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
  def apply(workerId: String,
            maxBufferSize: Int = 100000,
            maxDurationInSeconds: Int = 60)(implicit system: ActorSystem,
                                            ec: ExecutionContext) =
    new CheckpointTracker(workerId, maxBufferSize, maxDurationInSeconds)
}
