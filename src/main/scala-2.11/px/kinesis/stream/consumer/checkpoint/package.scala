package px.kinesis.stream.consumer

import scala.util.Try
import scala.util.Success
import scala.util.Failure

package object checkpoint {

  implicit class TryOps[A](t1: Try[A]) {
    def toEither: Either[Throwable, A] = t1 match {
      case Success(value) => Right(value)
      case Failure(exception) => Left(exception)
    }
  }
}
