import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Keep, Sink, Source}
import checkpoint.CheckpointTracker
import software.amazon.kinesis.processor.{
  ShardRecordProcessor,
  ShardRecordProcessorFactory
}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

class RecordProcessorFactoryImpl(sink: Sink[Record, NotUsed],
                                 workerId: String,
                                 checkpointTracker: CheckpointTracker,
                                 terminationFuture: Future[Done],
                                 logging: LoggingAdapter)(
    implicit am: ActorMaterializer,
    system: ActorSystem,
    ec: ExecutionContext)
    extends ShardRecordProcessorFactory {
  override def shardRecordProcessor(): ShardRecordProcessor = {
    val queue = Source
      .queue[Seq[Record]](0, OverflowStrategy.backpressure)
      .mapConcat(identity)
      .toMat(sink)(Keep.left)
      .run()
    new RecordProcessorImpl(queue,
                            checkpointTracker,
                            terminationFuture,
                            workerId,
                            logging)
  }
}
