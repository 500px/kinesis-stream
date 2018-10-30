import akka.Done
import software.amazon.kinesis.coordinator.WorkerStateChangeListener.WorkerState
import software.amazon.kinesis.coordinator.{
  SchedulerCoordinatorFactory,
  WorkerStateChangeListener
}

import scala.concurrent.Promise

class CoordinatorFactoryImpl(shutdownPromise: Promise[Done])
    extends SchedulerCoordinatorFactory {
  override def createWorkerStateChangeListener(): WorkerStateChangeListener =
    new WorkerStateChangeListenerImpl(shutdownPromise)
}

class WorkerStateChangeListenerImpl(shutdownPromise: Promise[Done])
    extends WorkerStateChangeListener {
  override def onWorkerStateChange(
      newState: WorkerStateChangeListener.WorkerState): Unit = {
    if (newState == WorkerState.SHUT_DOWN) {
      shutdownPromise.trySuccess(Done)
    }
  }
}
