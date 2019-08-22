package px.kinesis.stream.consumer

import java.util.concurrent.Executors

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.ExecutionContext

object SchedulerExecutionContext {

  lazy val Global = SchedulerExecutionContext("KinesisScheduler")

  def apply(name: String): ExecutionContext =
    ExecutionContext.fromExecutor(
      Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
          .setNameFormat(s"$name-%04d")
          .setDaemon(true)
          .build
      )
    )
}
