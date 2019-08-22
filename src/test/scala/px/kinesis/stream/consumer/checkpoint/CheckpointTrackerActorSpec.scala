package px.kinesis.stream.consumer.checkpoint

import akka.actor.Status.Failure
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import px.kinesis.stream.consumer.checkpoint.CheckpointTrackerActor._
import px.kinesis.stream.consumer.checkpoint.{
  ShardCheckpointTrackerActor => shard
}
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.immutable.Seq

class CheckpointTrackerActorSpec
    extends TestKit(ActorSystem("CheckpointTrackerActorSpec"))
    with ImplicitSender
    with FunSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockFactory {
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val workerId = "123"

  def toSequenceNum(i: Int): ExtendedSequenceNumber =
    new ExtendedSequenceNumber(i.toString)

  def createTracker(): ActorRef =
    system.actorOf(
      CheckpointTrackerActor
        .props(workerId, 10, 10))

  describe("track") {
    it("should track successfully after creation of tracker") {
      val tracker = createTracker()
      val shardId = "01"
      tracker ! Command.Create(shardId)
      expectMsg(Response.Ack)

      tracker ! Command.Track(shardId, Seq(1).map(toSequenceNum))
      expectMsg(shard.Response.Ack)
    }

    it("should fail if tracker is not active") {
      val tracker = createTracker()
      val shardId = "01"
      // shard tracker for 01 does not exist
      tracker ! Command.Track(shardId, Seq(1).map(toSequenceNum))
      expectMsgPF() {
        case Failure(_) => true
      }
    }
  }

  describe("process") {
    it("should process successfully after creation of tracker") {
      val tracker = createTracker()
      val shardId = "01"
      tracker ! Command.Create(shardId)
      expectMsg(Response.Ack)

      tracker ! Command.Process(shardId, toSequenceNum(1))
      expectMsg(shard.Response.Ack)
    }

    it("should process successfully even after shard tracker is shutdown") {
      val tracker = createTracker()
      val shardId = "01"
      tracker ! Command.Create(shardId)
      expectMsg(Response.Ack)

      tracker ! Command.Process(shardId, toSequenceNum(1))
      expectMsg(shard.Response.Ack)

      tracker ! Command.ShutdownShard(shardId)
      expectMsg(Response.Ack)

      tracker ! Command.Process(shardId, toSequenceNum(2))
      expectMsg(shard.Response.Ack)

    }

  }
}
