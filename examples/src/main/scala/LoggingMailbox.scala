import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import com.typesafe.config.Config
import akka.actor.{ActorContext, ActorRef, ActorSystem, ExtendedActorSystem}
import akka.dispatch.{
  Envelope,
  MailboxType,
  MessageQueue,
  UnboundedMailbox,
  UnboundedQueueBasedMessageQueue,
  ProducesMessageQueue
}
import akka.event.Logging

/**
  * Logs the mailbox size when exceeding the configured limit. It logs at most once per second
  * when the messages are enqueued or dequeued.
  *
  * Configuration:
  * <pre>
  * akka.actor.default-mailbox {
  *   mailbox-type = akka.contrib.mailbox.LoggingMailboxType
  *   size-limit = 20
  * }
  * </pre>
  */
class LoggingMailboxType(settings: ActorSystem.Settings, config: Config)
    extends MailboxType
    with ProducesMessageQueue[UnboundedMailbox.MessageQueue] {
  override def create(owner: Option[ActorRef], system: Option[ActorSystem]) =
    (owner, system) match {
      case (Some(o), Some(s)) =>
        val sizeLimit = config.getInt("size-limit")
        val mailbox = new LoggingMailbox(o, s, sizeLimit)
        mailbox
      case _ =>
        throw new IllegalArgumentException("no mailbox owner or system given")
    }
}

class LoggingMailbox(owner: ActorRef, system: ActorSystem, sizeLimit: Int)
    extends UnboundedMailbox.MessageQueue {

  private val interval = 1000000000L // 1 s, in nanoseconds
  private lazy val log = Logging(system, classOf[LoggingMailbox])
  private val path = owner.path.toString
  @volatile private var logTime: Long = System.nanoTime()
  private val queueSize = new AtomicInteger
  private val dequeueCount = new AtomicInteger

  override def dequeue(): Envelope = {
    val x = super.dequeue()
    if (x ne null) {
      val size = queueSize.decrementAndGet()
      dequeueCount.incrementAndGet()
      logSize(size)
    }
    x
  }

  override def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
    super.enqueue(receiver, handle)
    val size = queueSize.incrementAndGet()
    logSize(size)
  }

  def logSize(size: Int): Unit =
    if (size >= sizeLimit) {
      val now = System.nanoTime()
      if (now - logTime > interval) {
        val msgPerSecond = dequeueCount.get.toDouble / ((now - logTime).toDouble / 1000000000L)
        logTime = now
        dequeueCount.set(0)
        log.info("Mailbox size for [{}] is [{}], processing [{}] msg/s",
                 path,
                 size,
                 f"$msgPerSecond%2.2f")
      }
    }

  override def numberOfMessages: Int = queueSize.get

  override def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
    super.cleanUp(owner, deadLetters)
  }
}
