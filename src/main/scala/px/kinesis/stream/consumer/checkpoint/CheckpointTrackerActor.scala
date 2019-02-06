package px.kinesis.stream.consumer.checkpoint

import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  OneForOneStrategy,
  Props,
  SupervisorStrategy,
  Terminated
}
import akka.pattern.{gracefulStop, pipe}
import CheckpointTrackerActor._
import akka.actor.Status.Failure
import akka.actor.SupervisorStrategy.Escalate
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

  var trackers = Map.empty[String, TrackerState]

  override def receive: Receive = {
    case Create(shardId) =>
      startShardTracker(shardId)
      sender() ! Ack
    case Track(shardId, sequenceNumbers) =>
      forward(shardId, shard.Track(sequenceNumbers))
    case Process(shardId, sequenceNumber) if isTrackerActive(shardId) =>
      forward(shardId, shard.Process(sequenceNumber))
    case Process(shardId, sequenceNumber) =>
      // Skip out on forwarding the message to trackers, we should just respond to sender immediately
      // This case would occur if the associated tracker was shutdown gracefully due to a shutdown request (possible lease loss)
      log.warning(
        "The tracker associated with shard {} is terminating or already shut down. Since there is no lease for the given shard, check pointing for sequenceNumber: {} will not occur.",
        shardId,
        sequenceNumber.toString
      )
      sender() ! shard.Ack

    case CheckpointIfNeeded(shardId, checkpointer, force) =>
      forward(shardId, shard.CheckpointIfNeeded(checkpointer, force))
    case WatchCompletion(shardId) =>
      forward(shardId, shard.WatchCompletion)
    case Shutdown(shardId) =>
      shutdownShardTracker(shardId)
      sender() ! Ack
    case Shutdown =>
      shutdownChildren()
    case ChildrenShutdownComplete =>
      context.stop(self)
    case Terminated(child) =>
      removeShardTracker(child.path.name)
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

  /**
    * Forward messages to shard trackers
    * @param shardId
    * @param message
    */
  def forward(shardId: String, message: shard.Command): Unit =
    trackers.get(shardId) match {
      case Some(TrackerState(ref, isTerminating)) if !isTerminating =>
        log.info(s"forwarding $message")
        ref.forward(message)
      case _ =>
        log.warning("Tracker for {} is not active", shardId)
        sender() ! Failure(
          new IllegalStateException(
            s"Tracker for shard ${shardId} is not active"))
    }

  /**
    * Returns true if the tracker is active.
    * A tracker must exist and not be in a terminating state for it to be active
    * @param shardId
    * @return
    */
  def isTrackerActive(shardId: String): Boolean =
    trackers.get(shardId).exists(t => !t.isTerminating)

  /**
    * Start a shard tracker actor
    * Watches the actor for termination
    * @param shardId
    */
  def startShardTracker(shardId: String): Unit = {
    log.info("Initializing shard tracker for {}", shardId)
    val ref = context.actorOf(
      ShardCheckpointTrackerActor
        .props(shardId, maxBufferSize, maxDurationInSeconds),
      shardId)
    context.watch(ref)
    trackers = trackers + (shardId -> TrackerState(ref))
  }

  /**
    * Removes tracker for map of running trackers. Stops watching it.
    * @param shardId
    */
  def removeShardTracker(shardId: String): Unit = {
    trackers.get(shardId).map(s => context.unwatch(s.ref))
    trackers = trackers - shardId
  }

  /**
    * Mark tracker as terminating and send it a shutdown message
    * @param shardId
    */
  def shutdownShardTracker(shardId: String): Unit = {
    trackers.get(shardId).foreach { tracker =>
      trackers = trackers + (shardId -> tracker.copy(isTerminating = true))
      tracker.ref ! shard.Shutdown
    }
  }

  override def postStop(): Unit = {
    log.info("Shutting down tracker {}", workerId)
  }

  /**
    * Set the supervision strategy such that any exceptions in children should be escalated.
    * We do not expect child trackers to throw exceptions. If they do, we need to fail.
    */
  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: Exception => Escalate
  }

}

object CheckpointTrackerActor {
  // state
  private[checkpoint] case class TrackerState(ref: ActorRef,
                                              isTerminating: Boolean = false)
  // commands
  case class Track(shardId: String,
                   sequenceNumbers: Iterable[ExtendedSequenceNumber])
  case class Process(shardId: String, sequenceNumber: ExtendedSequenceNumber)
  case class CheckpointIfNeeded(shardId: String,
                                checkpointer: RecordProcessorCheckpointer,
                                force: Boolean = false)
  case class Create(shardId: String)
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
