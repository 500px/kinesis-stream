package px.kinesis.stream.consumer.checkpoint

import akka.actor.Status.Failure
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.must.Matchers
import px.kinesis.stream.consumer.checkpoint.ShardCheckpointTrackerActor._
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.{Queue, Seq}

class ShardCheckpointTrackerActorSpec
    extends TestKit(ActorSystem("ShardCheckpointTrackerActorSpec"))
    with ImplicitSender
    with AnyFunSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockFactory {
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val emptyQueue: Queue[ExtendedSequenceNumber] = Queue.empty[ExtendedSequenceNumber]

  def createTracker(maxBufferSize: Int = 10,
                    maxDurationInSeconds: Int = 10): ActorRef =
    system.actorOf(
      ShardCheckpointTrackerActor
        .props("shardId-01", maxBufferSize, maxDurationInSeconds))

  def toSequenceNum(i: Int): ExtendedSequenceNumber =
    new ExtendedSequenceNumber(i.toString)

  describe("track") {
    it("should track sequence numbers") {
      val tracker = createTracker()

      tracker ! Command.Track(Seq(1).map(toSequenceNum))
      expectMsg(Response.Ack)
    }

    it("should accept empty sequence list") {
      val tracker = createTracker()

      tracker ! Command.Track(Seq.empty[ExtendedSequenceNumber])
      expectMsg(Response.Ack)
    }

    it(
      "should not consider sequence number checkpointable if it is tracked without being marked processed") {
      val tracker = createTracker()

      val tracked = Seq(1, 2, 3).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      tracker ! Command.Get
      expectMsg(Response.Details(Queue(tracked: _*), emptyQueue))
    }
  }

  describe("process") {
    it("should mark sequence number as processed") {
      val tracker = createTracker()

      tracker ! Command.Track(Seq(1).map(toSequenceNum))
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)
    }

    it(
      "should do nothing if sequence number is marked processed without first being tracked") {
      val tracker = createTracker()

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)
    }

    it(
      "should make sequence number checkpointable if it is lowest sequence in tracked set") {
      val tracker = createTracker()

      val tracked = Seq(1, 2, 3, 4).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)

      tracker ! Command.Get
      expectMsg(Response.Details(Queue(tracked: _*), Queue(toSequenceNum(1))))
    }

    it(
      "should not make sequence number checkpointable if sequence numbers lower than it are not marked processed") {
      val tracker = createTracker()

      val tracked = Seq(1, 2, 3, 4).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(3))
      expectMsg(Response.Ack)

      tracker ! Command.Get
      expectMsg(Response.Details(Queue(tracked: _*), emptyQueue))

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)

      tracker ! Command.Get
      expectMsgPF() {
        case Response.Details(t, set)
          if !set.contains(toSequenceNum(3)) && t.forall(tracked.contains) =>
            true
      }
    }
  }

  describe("checkpoint") {

    it(
      "should checkpoint highest checkpointable sequence number if tracking more than max buffer") {
      val tracker = createTracker(maxBufferSize = 1)

      val checkpointer = mock[RecordProcessorCheckpointer]
      expectCheckpointAt(checkpointer, toSequenceNum(3))

      val tracked = Seq(1, 2, 3, 4).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(3))
      expectMsg(Response.Ack)
      tracker ! Command.Process(toSequenceNum(2))
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)

      tracker ! Command.CheckpointIfNeeded(checkpointer)
      expectMsg(Response.CheckPointed(Some(toSequenceNum(3))))
    }

    it("should not checkpoint if there is nothing being tracked") {
      val tracker = createTracker(maxBufferSize = 0)

      val checkpointer = mock[RecordProcessorCheckpointer]
      expectNoCheckpoint(checkpointer)

      tracker ! Command.CheckpointIfNeeded(checkpointer)
      expectMsg(Response.CheckPointed(None))
    }

    it("should not checkpoint if max buffer or duration are not reached") {
      val tracker = createTracker(maxBufferSize = 5, maxDurationInSeconds = 60)

      val checkpointer = mock[RecordProcessorCheckpointer]
      expectNoCheckpoint(checkpointer)

      val tracked = Seq(1, 2, 3).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(3))
      expectMsg(Response.Ack)
      tracker ! Command.Process(toSequenceNum(2))
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)

      tracker ! Command.CheckpointIfNeeded(checkpointer)
      expectMsg(Response.CheckPointed(None))
    }

    it("should remove sequence number from tracked once it is checkpointed") {
      val tracker = createTracker(maxBufferSize = 2)

      val checkpointer = mock[RecordProcessorCheckpointer]
      expectCheckpointAt(checkpointer, toSequenceNum(3))

      val tracked = Seq(1, 2, 3).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(3))
      expectMsg(Response.Ack)
      tracker ! Command.Process(toSequenceNum(2))
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)

      tracker ! Command.CheckpointIfNeeded(checkpointer)
      expectMsg(Response.CheckPointed(Some(toSequenceNum(3))))

      tracker ! Command.Get
      expectMsg(Response.Details(emptyQueue, emptyQueue))
    }

    it("should checkpoint regardless of max buffer or duration if forced=true") {
      val tracker = createTracker(maxBufferSize = 5, maxDurationInSeconds = 60)

      val checkpointer = mock[RecordProcessorCheckpointer]
      expectCheckpointAt(checkpointer, toSequenceNum(3))

      val tracked = Seq(1, 2, 3).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(3))
      expectMsg(Response.Ack)
      tracker ! Command.Process(toSequenceNum(2))
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)

      tracker ! Command.CheckpointIfNeeded(checkpointer, force = true)
      expectMsg(Response.CheckPointed(Some(toSequenceNum(3))))

      tracker ! Command.Get
      expectMsg(Response.Details(emptyQueue, emptyQueue))
    }

    it("should not checkpoint if there is no sequence number checkpointable") {
      val tracker = createTracker(maxBufferSize = 2)

      val checkpointer = mock[RecordProcessorCheckpointer]
      expectNoCheckpoint(checkpointer)

      val tracked = Seq(1, 2, 3).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      tracker ! Command.Process(toSequenceNum(3))
      expectMsg(Response.Ack)
      tracker ! Command.Process(toSequenceNum(2))
      expectMsg(Response.Ack)

      tracker ! Command.CheckpointIfNeeded(checkpointer, force = true)
      expectMsg(Response.CheckPointed(None))
    }

  }

  describe("watch") {
    it(
      "should send completed message to watcher once all tracked messages are marked processed") {
      val tracker = createTracker()

      val tracked = Seq(1, 2, 3).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      val watcher = TestProbe("watcher")
      watcher.send(tracker, Command.WatchCompletion)

      tracker ! Command.Process(toSequenceNum(3))
      expectMsg(Response.Ack)

      watcher.expectNoMessage()
      tracker ! Command.Process(toSequenceNum(2))
      expectMsg(Response.Ack)

      watcher.expectNoMessage()

      tracker ! Command.Process(toSequenceNum(1))
      expectMsg(Response.Ack)

      watcher.expectMsg(Response.Completed)
    }

    it(
      "should send failure to watchers if tracker is requested to shutdown before detecting completion") {
      val tracker = createTracker()

      val tracked = Seq(1, 2, 3).map(toSequenceNum)
      tracker ! Command.Track(tracked)
      expectMsg(Response.Ack)

      val watcher = TestProbe("watcher")
      watcher.send(tracker, Command.WatchCompletion)

      tracker ! Command.Process(toSequenceNum(3))
      expectMsg(Response.Ack)

      watcher.expectNoMessage()
      tracker ! Command.Process(toSequenceNum(2))
      expectMsg(Response.Ack)

      tracker ! Command.Shutdown
      watcher.expectMsgPF() {
        case Failure(_) => true
      }
    }
  }

  private def expectCheckpointAt(checkpointer: RecordProcessorCheckpointer,
                                 seqNum: ExtendedSequenceNumber) = {
    (checkpointer
      .checkpoint(_: String, _: Long))
      .expects(seqNum.sequenceNumber(), seqNum.subSequenceNumber())
      .once()
  }

  private def expectNoCheckpoint(checkpointer: RecordProcessorCheckpointer) = {
    (checkpointer.checkpoint(_: String, _: Long)).expects(*, *).never()
  }
}
