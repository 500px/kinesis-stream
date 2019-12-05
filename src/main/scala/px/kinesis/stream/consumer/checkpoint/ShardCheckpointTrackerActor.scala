package px.kinesis.stream.consumer.checkpoint

import java.time.Instant

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import px.kinesis.stream.consumer.checkpoint.ShardCheckpointTrackerActor._
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.{Iterable, Queue}
import scala.util.Try

class ShardCheckpointTrackerActor(shardId: String, maxBufferSize: Int, maxDurationInSeconds: Int) extends Actor with ActorLogging {
  implicit val ordering: Ordering[ExtendedSequenceNumber] =
    Ordering.fromLessThan[ExtendedSequenceNumber]((a, b) => a.compareTo(b) < 0)

  var tracked: Queue[ExtendedSequenceNumber] = Queue.empty[ExtendedSequenceNumber]
  var processed: Set[ExtendedSequenceNumber] = Set.empty
  var lastCheckpoint: Option[ExtendedSequenceNumber] = None
  var timeSinceLastCheckpoint: Long = now
  var watchers: List[ActorRef] = List.empty[ActorRef]

  log.info("{} Tracker started using {}", shardId, context.dispatcher.toString)

  override def receive: Receive = {
    case Command.Track(sequenceNumbers) =>
      log.debug("Tracking: {}", sequenceNumbers.map(formatSeqNum).mkString(","))
      tracked ++= sequenceNumbers
      sender() ! Response.Ack

    case Command.Process(sequenceNumber: ExtendedSequenceNumber) =>
      log.debug("Marked: {}", formatSeqNum(sequenceNumber))

      if (lastCheckpoint.forall(c => ordering.gt(sequenceNumber, c))) {
        processed += sequenceNumber
      }

      sender() ! Response.Ack
      notifyIfCompleted()

    case Command.CheckpointIfNeeded(checkpointer, force) =>
      val checkpointable = getCheckpointable

      val checkpointableLog = checkpointable.map(formatSeqNum).mkString("[", ",", "]")
      log.debug("CheckpointIfNeeded: {}", checkpointableLog)

      checkpointable.lastOption.fold(sender() ! Response.CheckPointed()) { s =>
        if (shouldCheckpoint() || force) {
          log.debug("Checkpointing(forced={}) {}", force, shardId)
          // we absorb the exceptions so we don't lose state for this actor
          Try(checkpointer.checkpoint(s.sequenceNumber(), s.subSequenceNumber())).toEither
            .fold(
              ex => sender() ! Failure(ex),
              _ => {
                log.info("Checkpointed Successfully: {} is at {}", shardId, formatSeqNum(s))
                tracked = tracked.drop(checkpointable.size)
                processed --= checkpointable
                lastCheckpoint = Some(s)
                timeSinceLastCheckpoint = now
                sender() ! Response.CheckPointed(Some(s))
              }
            )
        } else {
          log.debug("Skipping Checkpoint: {}", shardId)
          sender() ! Response.CheckPointed()
        }
      }
      notifyIfCompleted()

    case Command.WatchCompletion =>
      log.info("WatchCompletion: {}", shardId)
      watchers = sender() :: watchers
      notifyIfCompleted()

    case Command.Get =>
      log.debug("Tracked: {}", tracked.mkString(","))
      log.debug("Processed: {}", processed.mkString(","))
      sender() ! Response.Details(tracked, getCheckpointable)

    case Command.Shutdown =>
      notifyWatchersOfShutdown()
      context.stop(self)
  }

  def getCheckpointable: Queue[ExtendedSequenceNumber] = tracked.takeWhile(processed.contains)

  override def postStop(): Unit = log.info("Shutting down tracker {}", shardId)

  def shouldCheckpoint(): Boolean = haveReachedMaxTracked || checkpointTimeElapsed

  def haveReachedMaxTracked: Boolean = tracked.size >= maxBufferSize

  def checkpointTimeElapsed: Boolean = now - timeSinceLastCheckpoint >= maxDurationInSeconds

  def notifyIfCompleted(): Unit =
    if (isCompleted && watchers.nonEmpty) {
      log.info("Notifying completion for {}", shardId)
      watchers.foreach(ref => ref ! Response.Completed)
      watchers = List.empty[ActorRef]
    }

  def notifyWatchersOfShutdown(): Unit =
    if (!isCompleted && watchers.nonEmpty) {
      log.info("Notifying failure to watchers for {}", shardId)
      watchers.foreach(_ ! Failure(new Exception("Watch failed. Reason: tracker shutdown")))
    } else {
      notifyIfCompleted()
    }

  def isCompleted: Boolean = tracked.isEmpty || tracked.forall(processed.contains)

  def now: Long = Instant.now().getEpochSecond

  def formatSeqNum(es: ExtendedSequenceNumber): String = es.sequenceNumber().takeRight(10)
}

object ShardCheckpointTrackerActor {
  sealed trait Command extends Product with Serializable
  object Command {
    final case class Track(sequenceNumbers: Iterable[ExtendedSequenceNumber]) extends Command
    final case class Process(sequenceNumber: ExtendedSequenceNumber) extends Command
    final case class CheckpointIfNeeded(checkPointer: RecordProcessorCheckpointer, force: Boolean = false) extends Command
    case object WatchCompletion extends Command
    case object Get extends Command
    case object Shutdown extends Command
  }

  sealed trait Response extends Product with Serializable
  object Response {
    final case class Details(tracked: Queue[ExtendedSequenceNumber], checkpointable: Queue[ExtendedSequenceNumber]) extends Response
    final case class CheckPointed(sequenceNumber: Option[ExtendedSequenceNumber] = None) extends Response
    case object Ack extends Response
    case object Completed extends Response
  }

  def props(shardId: String, maxBufferSize: Int, maxDurationInSeconds: Int): Props =
    Props(new ShardCheckpointTrackerActor(shardId, maxBufferSize, maxDurationInSeconds))
}
