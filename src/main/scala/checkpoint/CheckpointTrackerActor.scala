package checkpoint

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import checkpoint.CheckpointTrackerActor._
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber
import checkpoint.{ShardCheckpointTrackerActor => shard}

import scala.collection.immutable.Iterable

class CheckpointTrackerActor(workerId: String) extends Actor with ActorLogging {
  override def receive: Receive = {
    case Track(shardId, sequenceNumbers) => shardTracker(shardId).forward(shard.Track(sequenceNumbers))
    case Process(shardId, sequenceNumber) => shardTracker(shardId).forward(shard.Process(sequenceNumber))
    case CheckpointIfNeeded(shardId, checkpointer, force) => shardTracker(shardId).forward(shard.CheckpointIfNeeded(checkpointer, force))
    case WatchCompletion(shardId) => shardTracker(shardId).forward(shard.WatchCompletion)
    case Shutdown(shardId) =>
      shutdownShardTracker(shardId)
      sender() ! Ack
  }

  def shardTracker(shardId: String): ActorRef = {
    context.child(shardId).getOrElse(createShardTracker(shardId))
  }

  def createShardTracker(shardId: String): ActorRef = {
    context.actorOf(ShardCheckpointTrackerActor.props(shardId), shardId)
  }

  def shutdownShardTracker(shardId: String): Unit = {
    context.child(shardId).foreach(context.stop)
  }

  override def postStop(): Unit = {
    log.info("Shutting down tracker {}", workerId)
  }
}

object CheckpointTrackerActor {
  // commands
  case class Track(shardId: String, sequenceNumbers: Iterable[ExtendedSequenceNumber])
  case class Process(shardId: String, sequenceNumber: ExtendedSequenceNumber)
  case class CheckpointIfNeeded(shardId: String, checkpointer: RecordProcessorCheckpointer,
                                force: Boolean = false)
  case class Shutdown(shardId: String)
  case class WatchCompletion(shardId: String)

  // responses
  case object Ack

  def props(workerId: String): Props = Props(classOf[CheckpointTrackerActor], workerId)
}
