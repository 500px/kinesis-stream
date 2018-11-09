package px.kinesis.stream.consumer.checkpoint

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{gracefulStop, pipe}
import CheckpointTrackerActor._
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.Iterable
import scala.concurrent.Future
import scala.concurrent.duration._
import px.kinesis.stream.consumer.checkpoint.{
  ShardCheckpointTrackerActor => shard
}

class CheckpointTrackerActor(workerId: String,
                             maxBufferSize: Int,
                             maxDurationInSeconds: Int)
    extends Actor
    with ActorLogging {
  implicit val ec = context.dispatcher

  override def receive: Receive = {
    case Track(shardId, sequenceNumbers) =>
      shardTracker(shardId).forward(shard.Track(sequenceNumbers))
    case Process(shardId, sequenceNumber) =>
      shardTracker(shardId).forward(shard.Process(sequenceNumber))
    case CheckpointIfNeeded(shardId, checkpointer, force) =>
      shardTracker(shardId).forward(
        shard.CheckpointIfNeeded(checkpointer, force))
    case WatchCompletion(shardId) =>
      shardTracker(shardId).forward(shard.WatchCompletion)
    case Shutdown(shardId) =>
      shutdownShardTracker(shardId)
      sender() ! Ack
    case Shutdown =>
      shutdownChildren()
    case ChildrenShutdownComplete =>
      context.stop(self)
  }

  def shutdownChildren(): Future[ChildrenShutdownComplete.type] = {
    Future
      .sequence(
        context.children.map(r => gracefulStop(r, 5.seconds, shard.Shutdown)))
      .map(_ => ChildrenShutdownComplete)
      .recover {
        case _ => ChildrenShutdownComplete
      } pipeTo self
  }

  def shardTracker(shardId: String): ActorRef = {
    context.child(shardId).getOrElse(createShardTracker(shardId))
  }

  def createShardTracker(shardId: String): ActorRef = {
    context.actorOf(ShardCheckpointTrackerActor
                      .props(shardId, maxBufferSize, maxDurationInSeconds),
                    shardId)
  }

  def shutdownShardTracker(shardId: String): Unit = {
    context.child(shardId).foreach(ref => ref ! shard.Shutdown)
  }

  override def postStop(): Unit = {
    log.info("Shutting down tracker {}", workerId)
  }
}

object CheckpointTrackerActor {
  // commands
  case class Track(shardId: String,
                   sequenceNumbers: Iterable[ExtendedSequenceNumber])
  case class Process(shardId: String, sequenceNumber: ExtendedSequenceNumber)
  case class CheckpointIfNeeded(shardId: String,
                                checkpointer: RecordProcessorCheckpointer,
                                force: Boolean = false)
  case class Shutdown(shardId: String)
  case object Shutdown
  case object ChildrenShutdownComplete
  case class WatchCompletion(shardId: String)

  // responses
  case object Ack

  def props(workerId: String,
            maxBufferSize: Int,
            maxDurationInSeconds: Int): Props =
    Props(classOf[CheckpointTrackerActor],
          workerId,
          maxBufferSize,
          maxDurationInSeconds)
}
