package px.kinesis.stream.consumer

import akka.stream.scaladsl.Flow

import scala.concurrent.ExecutionContext

trait CommitGraphStages {

  /**
    * A flow which transparently marks every record as processed
    * @param parallelism
    * @param ec
    * @return
    */
  def commitFlow(parallelism: Int = 1)(implicit ec: ExecutionContext) =
    Flow[Record].mapAsync(parallelism)(r => r.markProcessed().map(_ => r))
}
