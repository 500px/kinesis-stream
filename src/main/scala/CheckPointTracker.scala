import java.util.concurrent.TimeUnit

import CheckpointTrackerActor._
import akka.Done
import akka.actor.ActorSystem
import akka.util.Timeout
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber
import akka.pattern.{AskTimeoutException, ask}
import software.amazon.kinesis.processor.RecordProcessorCheckpointer

import scala.collection.immutable.Iterable
import scala.concurrent.{ExecutionContext, Future}

class CheckPointTracker(workerId: String,
                        shardId: String,
                        terminationFuture: Future[Done])(
    implicit system: ActorSystem,
    ec: ExecutionContext) {

  val tracker = system.actorOf(CheckpointTrackerActor.props(shardId),
                               s"tracker-$shardId-${workerId.substring(3)}")

  terminationFuture.onComplete(_ => shutdown())

  val timeout = Timeout(5, TimeUnit.SECONDS)

  def track(sequences: Iterable[ExtendedSequenceNumber]) = {
    tracker
      .ask(Track(sequences))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout)
  }

  def process(sequence: ExtendedSequenceNumber) = {
    tracker
      .ask(Process(sequence))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout)
  }

  def checkpointIfNeeded(checkpointer: RecordProcessorCheckpointer) = {
    tracker
      .ask(CheckpointIfNeeded(checkpointer))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout)
  }

  def checkpoint(checkpointer: RecordProcessorCheckpointer) = {
    tracker
      .ask(CheckpointIfNeeded(checkpointer, force = true))(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout)
  }

  def watchCompletion(timeout: Timeout) = {
    tracker
      .ask(WatchCompletion)(timeout)
      .map(_ => Done)
      .recoverWith(mapAskTimeout)
  }

  def shutdown(): Unit = {
    system.stop(tracker)
  }

  private def mapAskTimeout[A]: PartialFunction[Throwable, Future[A]] = {
    case _: AskTimeoutException =>
      Future.failed(
        new CommitTimeoutException(
          s"Commit took longer than: $timeout for $shardId"))
    case other => Future.failed(other)
  }
}

class CommitTimeoutException(message: String) extends Exception(message)
