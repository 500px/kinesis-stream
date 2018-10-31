import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.contxt.kinesis.ScalaKinesisProducer

object Producer extends App {

  implicit val system = ActorSystem("kinesis-producer")
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()

  val producer = ScalaKinesisProducer("activity-test", new KinesisProducerConfiguration().setRegion("us-east-1").setCredentialsProvider(new DefaultAWSCredentialsProviderChain))

  producer.send("1", ByteString("test").toByteBuffer)

  Source(1 to 3).map(i => (i.toString, ByteString(s"Data Test: $i"))).mapAsync(1) {
    case (key, data) => producer.send(key, data.toByteBuffer)
  }.runWith(Sink.foreach(println)).onComplete {
    case _ => system.terminate()
  }
}
