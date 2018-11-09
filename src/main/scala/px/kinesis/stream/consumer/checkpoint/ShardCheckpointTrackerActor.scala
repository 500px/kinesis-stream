package px.kinesis.stream.consumer.checkpoint

import java.time.Instant

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import px.kinesis.stream.consumer.checkpoint.ShardCheckpointTrackerActor._
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.{Iterable, Queue}
import scala.util.Try

class ShardCheckpointTrackerActor(shardId: String,
                                  maxBufferSize: Int,
                                  maxDurationInSeconds: Int)
    extends Actor
    with ActorLogging {
  implicit val ordering =
    Ordering.fromLessThan[ExtendedSequenceNumber]((a, b) => a.compareTo(b) < 0)

  var tracked = Queue.empty[ExtendedSequenceNumber]
  var processed = Set.empty[ExtendedSequenceNumber]
  var lastCheckpoint: Option[ExtendedSequenceNumber] = None
  var timeSinceLastCheckpoint = now()
  var watchers: List[ActorRef] = List.empty[ActorRef]

  log.info("{} Tracker started using {}", shardId, context.dispatcher.toString)

  override def receive: Receive = {
    case Track(sequenceNumbers) =>
      log.debug("Tracking: {}", sequenceNumbers.map(formatSeqNum).mkString(","))
      tracked ++= sequenceNumbers
      sender() ! Ack
    case Process(sequenceNumber: ExtendedSequenceNumber) =>
      log.debug("Marked: {}", formatSeqNum(sequenceNumber))

      if (lastCheckpoint.forall(c => ordering.gt(sequenceNumber, c))) {
        processed += sequenceNumber
      }

      sender() ! Ack
      notifyIfCompleted()
    case CheckpointIfNeeded(checkpointer, force) =>
      val checkpointable = getCheckpointable()
      log.debug("CheckpointIfNeeded: {}",
                checkpointable
                  .map(formatSeqNum)
                  .mkString("[", ",", "]"))
      checkpointable.lastOption.fold(sender() ! Checkpointed()) { s =>
        if (shouldCheckpoint() || force) {
          log.debug("Checkpointing(forced={}) {}", force, shardId)
          // we absorb the exceptions so we don't lose state for this actor
          Try(
            checkpointer.checkpoint(s.sequenceNumber(), s.subSequenceNumber()))
            .fold(
              ex => sender() ! Failure(ex),
              _ => {
                log.info("Checkpointed Successfully: {} is at {}",
                         shardId,
                         formatSeqNum(s))
                tracked = tracked.drop(checkpointable.size)
                processed --= checkpointable
                lastCheckpoint = Some(s)
                timeSinceLastCheckpoint = now()
                sender() ! Checkpointed(Some(s))
              }
            )
        } else {
          log.debug("Skipping Checkpoint: {}", shardId)
          sender() ! Checkpointed()
        }
      }
      notifyIfCompleted()
    case WatchCompletion =>
      log.info("WatchCompletion: {}", shardId)
      watchers = sender() :: watchers
      notifyIfCompleted()

    case Get =>
      log.debug("Tracked: {}", tracked.mkString(","))
      log.debug("Processed: {}", processed.mkString(","))
      sender() ! Details(tracked, getCheckpointable())
    case Shutdown =>
      notifyWatchersOfShutdown()
      context.stop(self)
  }

  def getCheckpointable(): Queue[ExtendedSequenceNumber] =
    tracked.takeWhile(processed.contains)

  override def postStop(): Unit = {
    log.info("Shutting down tracker {}", shardId)
  }

  def shouldCheckpoint(): Boolean = {
    haveReachedMaxTracked() || checkpointTimeElapsed()
  }

  def haveReachedMaxTracked(): Boolean = tracked.size >= maxBufferSize
  def checkpointTimeElapsed(): Boolean = {
    Instant
      .now()
      .getEpochSecond - timeSinceLastCheckpoint >= maxDurationInSeconds
  }

  def notifyIfCompleted() = {
    if (isCompleted() && watchers.nonEmpty) {
      log.info("Notifying completion for {}", shardId)
      watchers.foreach(ref => ref ! Completed)
      watchers = List.empty[ActorRef]
    }
  }

  def notifyWatchersOfShutdown() = {
    if (!isCompleted() && watchers.nonEmpty) {
      log.info("Notifying failure to watchers for {}", shardId)
      watchers.foreach(ref =>
        ref ! Failure(new Exception("Watch failed. Reason: tracker shutdown")))
    } else {
      notifyIfCompleted()
    }
  }

  def isCompleted(): Boolean = {
    tracked.isEmpty || tracked.forall(processed.contains)
  }

  def now(): Long = Instant.now().getEpochSecond
  def formatSeqNum(es: ExtendedSequenceNumber): String =
    es.sequenceNumber().takeRight(10)
}

object ShardCheckpointTrackerActor {
  case object Ack
  case class Track(sequenceNumbers: Iterable[ExtendedSequenceNumber])
  case class Process(sequenceNumber: ExtendedSequenceNumber)
  case class CheckpointIfNeeded(checkpointer: RecordProcessorCheckpointer,
                                force: Boolean = false)

  case class Details(tracked: Queue[ExtendedSequenceNumber],
                     checkpointable: Queue[ExtendedSequenceNumber])
  case class Checkpointed(sequenceNumber: Option[ExtendedSequenceNumber] = None)
  case object WatchCompletion
  case object Completed
  case object Get
  case object Shutdown

  def props(shardId: String,
            maxBufferSize: Int,
            maxDurationInSeconds: Int): Props =
    Props(classOf[ShardCheckpointTrackerActor],
          shardId,
          maxBufferSize,
          maxDurationInSeconds)
}
