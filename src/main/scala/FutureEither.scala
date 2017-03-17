package querysidepoc

import scala.concurrent.{Future, Promise, ExecutionContext => ExecCtx}

object FutureEither {

  def convertToFuture[T]( f: FutureEither[T] )(implicit ec: ExecCtx) : Future[Either[Error,T]] = {

    val prom = Promise[Either[Error, T]]()

    f map {
      s: T =>
        prom.success(new Right[Error, T](s))
        s
    } recover {
      s =>
        prom.success(new Left[Error, T](s))
        s

    }

    prom.future

  }


  def successfulWith[T](future: Future[T])(implicit ec: ExecCtx): FutureEither[T] = {

    val futureE : Future[Either[Error,T]] = future map {
      s : T => Right[Error,T](s)
    } recover {
      case e: Throwable => Left( ErrorGenerico(e) )
    }

    new FutureEither( futureE )

  }

  def failureWith[T](future: Future[Error])(implicit ec: ExecCtx): FutureEither[T] = ???

  def successful[T](value: T)(implicit ec: ExecCtx) = new FutureEither[T](Future.apply(Right(value)))

  def failure[T](value: Error)(implicit ec: ExecCtx) = new FutureEither[T](Future.apply(Left(value)))

  def sequence[T](list: List[FutureEither[T]])(implicit ec: ExecCtx): FutureEither[List[T]] = {

    def _sequence[T](from: List[FutureEither[T]], result: FutureEither[List[T]]): FutureEither[List[T]] = {

      from match {

        case h :: t => {

          for {

            reg <- h

            list <- _sequence(t, result)

          } yield reg +: list

        }

        case e => result

      }
    }

    _sequence(list, successful(List[T]()))

  }

}

class FutureEither[B](private val future: Future[Either[Error ,B]]) {

  def toFutureEither: FutureEither[B] = this

  def map[BB](f: B => BB)(implicit ec: ExecCtx) =
    new FutureEither[BB](
      future map {
        case Left(a)  => Left(a)
        case Right(b) => Right(f(b))
      } recover {
        case e: Throwable => Left( ErrorGenerico(e) )
      }
    )

  def recover[B](f: Error => B)(implicit ec: ExecCtx) =
    new FutureEither[B](
      future map {
        case Left(a: Error)  => Right[Error, B](f(a))
        case Right(b: B) =>  Right[Error, B]( b )
      } recover {
        case e: Throwable => Left( ErrorGenerico(e) )
      }
    )

  def flatMap[BB](f: B => FutureEither[BB])(implicit ec: ExecCtx) =
    new FutureEither[BB](
      future  flatMap {
        case Left(a)  => Future successful Left(a)
        case Right(b) => f(b).future
      } recover {
        case e: Throwable => Left( ErrorGenerico(e) )
      }
    )

}

