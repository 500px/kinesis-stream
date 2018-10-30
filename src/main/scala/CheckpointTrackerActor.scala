import java.time.Instant

import CheckpointTrackerActor._
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.{Iterable, SortedSet}
import scala.util.Try

class CheckpointTrackerActor(shardId: String) extends Actor with ActorLogging {
  implicit val ordering =
    Ordering.fromLessThan[ExtendedSequenceNumber]((a, b) => a.compareTo(b) < 0)

  val CheckpointMaxSize = 100000
  val CheckpointDurationSeconds = 60
  var tracked = SortedSet.empty[ExtendedSequenceNumber]
  var processed = SortedSet.empty[ExtendedSequenceNumber]
  var timeSinceLastCheckpoint = Instant.now().getEpochSecond
  var watchers: List[ActorRef] = List.empty[ActorRef]

  // TODO handle shutdown

  override def receive: Receive = {
    case Track(sequenceNumbers) =>
      log.info("Tracking: {}", sequenceNumbers.mkString(","))
      tracked ++= sequenceNumbers
      sender() ! Ack
    case Process(sequenceNumber: ExtendedSequenceNumber)
        if tracked.contains(sequenceNumber) =>
      log.info("Marked: {}", sequenceNumber)
      processed += sequenceNumber
      sender() ! Ack
      notifyIfCompleted()
    case CheckpointIfNeeded(checkpointer, force) =>
      val checkpointable = tracked.takeWhile(processed.contains)
      log.info("CheckpointIfNeeded: {}", checkpointable.mkString("[", ",", "]"))
      checkpointable.lastOption.fold(sender() ! Ack) { s =>
        if (shouldCheckpoint() || force) {
          log.info("Checkpointing {}", shardId)
          // we absorb the exceptions so we don't lose state for this actor
          Try(
            checkpointer.checkpoint(s.sequenceNumber(), s.subSequenceNumber()))
            .fold(ex => sender() ! Failure(ex), _ => {
              tracked --= checkpointable
              processed --= checkpointable
              sender() ! Ack
            })
        }
      }
      notifyIfCompleted()
    case WatchCompletion =>
      log.info("WatchCompletion")
      watchers = sender() :: watchers
      notifyIfCompleted()

    case Get =>
      log.info("Tracked: {}", tracked.mkString(","))
      log.info("Processed: {}", processed.mkString(","))
  }

  override def postStop(): Unit = {
    log.info("Shutting down tracker {}", shardId)
  }

  def shouldCheckpoint(): Boolean = {
    haveReachedMaxTracked() || checkpointTimeElapsed()
  }

  def haveReachedMaxTracked(): Boolean = tracked.size >= CheckpointMaxSize
  def checkpointTimeElapsed(): Boolean = {
    Instant
      .now()
      .getEpochSecond - timeSinceLastCheckpoint >= CheckpointDurationSeconds
  }

  def notifyIfCompleted() = {
    if (isCompleted()) {
      watchers.foreach(ref => ref ! Completed)
      watchers = List.empty[ActorRef]
    }
  }

  def isCompleted(): Boolean = {
    tracked.isEmpty || tracked.forall(processed.contains)
  }
}

object CheckpointTrackerActor {
  case object Ack
  case class Track(sequenceNumbers: Iterable[ExtendedSequenceNumber])
  case class Process(sequenceNumber: ExtendedSequenceNumber)
  case class CheckpointIfNeeded(checkpointer: RecordProcessorCheckpointer,
                                force: Boolean = false)
  case object WatchCompletion
  case object Completed
  case object Get
  case object Tick
  def props(shardId: String): Props =
    Props(classOf[CheckpointTrackerActor], shardId)
}
