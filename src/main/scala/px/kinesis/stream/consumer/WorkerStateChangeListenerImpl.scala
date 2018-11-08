package px.kinesis.stream.consumer

import akka.Done
import akka.event.LoggingAdapter
import software.amazon.kinesis.coordinator.WorkerStateChangeListener
import software.amazon.kinesis.coordinator.WorkerStateChangeListener.WorkerState

import scala.concurrent.Promise

class WorkerStateChangeListenerImpl(shutdownPromise: Promise[Done],
                                    logging: LoggingAdapter)
    extends WorkerStateChangeListener {
  override def onWorkerStateChange(
      newState: WorkerStateChangeListener.WorkerState): Unit = {
    if (newState == WorkerState.SHUT_DOWN) {
      logging.info("Worker State Changed to shut down")
      shutdownPromise.trySuccess(Done)
    }
  }
}
