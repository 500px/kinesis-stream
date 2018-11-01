import java.time.Instant

import akka.Done
import akka.util.ByteString
import checkpoint.CheckpointTracker
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.concurrent.Future

case class Record(key: String,
                  data: ByteString,
                  sequenceNumber: String,
                  subSequenceNumber: Long,
                  shardId: String,
                  approximateArrivalTimestamp: Instant,
                  markProcessed: () => Future[Done]) {
  def extendedSequenceNumber =
    new ExtendedSequenceNumber(sequenceNumber, subSequenceNumber)
}

object Record {
  def from(kinesisRecord: KinesisClientRecord,
           shardId: String, tracker: CheckpointTracker): Record = {

    val extendedSequenceNumber = new ExtendedSequenceNumber(
      kinesisRecord.sequenceNumber(),
      kinesisRecord.subSequenceNumber())
    val markProcessed: () => Future[Done] = () =>
      tracker.process(shardId, extendedSequenceNumber)

    Record(
      kinesisRecord.partitionKey(),
      ByteString(kinesisRecord.data()),
      kinesisRecord.sequenceNumber(),
      kinesisRecord.subSequenceNumber(),
      shardId,
      kinesisRecord.approximateArrivalTimestamp(),
      markProcessed
    )
  }

}
