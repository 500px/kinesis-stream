import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink}
import px.kinesis.stream.consumer

import scala.concurrent.Future

object Main extends App {

  implicit val system = ActorSystem("kinesis-source")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()

  // A simple consumer that will print to the console for now
  val console = Sink.foreach[String](println)

  val runnableGraph: RunnableGraph[Future[Done]] =
    consumer
      .source("test-stream", "test-app")
      .via(consumer.commitFlow(parallelism = 2))
      .map(r => r.data.utf8String)
      .toMat(console)(Keep.left)

  val done = runnableGraph.run()
  done.onComplete(_ => {
    println("Shutdown completed")
    system.terminate()
  })

}
