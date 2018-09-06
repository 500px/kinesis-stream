import java.time.Instant

import akka.Done
import akka.util.ByteString
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.concurrent.Future

case class Record(key: String,
                  data: ByteString,
                  sequenceNumber: String,
                  subSequenceNumber: Long,
                  approximateArrivalTimestamp: Instant,
                  markProcessed: () => Future[Done]) {
  def extendedSequenceNumber =
    new ExtendedSequenceNumber(sequenceNumber, subSequenceNumber)
}

object Record {
  def from(kinesisRecord: KinesisClientRecord,
           tracker: CheckPointTracker): Record = {

    val extendedSequenceNumber = new ExtendedSequenceNumber(
      kinesisRecord.sequenceNumber(),
      kinesisRecord.subSequenceNumber())
    val markProcessed: () => Future[Done] = () =>
      tracker.process(extendedSequenceNumber)

    Record(
      kinesisRecord.partitionKey(),
      ByteString(kinesisRecord.data()),
      kinesisRecord.sequenceNumber(),
      kinesisRecord.subSequenceNumber(),
      kinesisRecord.approximateArrivalTimestamp(),
      markProcessed
    )
  }

}
