package querysideproc

import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures
import querysidepoc.{CuentaNoEncontrada, _}

import scala.concurrent.{ExecutionContext, Promise}

class QuickTest extends FunSuite with ScalaFutures {



  test("Test ServicioModernizado con plan A y plan B cuando plan A falla") {


    class ServicioCuenta extends ServicioModernizado {

      override def planA(productoId: String)(implicit ec: ExecutionContext): FutureEither[Option[ProductoDTO]] = {
        FutureEither.failure(CuentaNoEncontrada(productoId))
      }

      override def planB(productoId: String)(implicit ec: ExecutionContext): FutureEither[ProductoDTO] = {
        FutureEither.successful(CuentaDTO(id = productoId, titulares = List("A", "B")))
      }
    }

    import scala.concurrent.ExecutionContext.Implicits.global

    val prom = Promise[Either[Error, ProductoDTO]]()

    val servicio = new ServicioCuenta
    val res = convertToFuture(servicio.obtenerInfo("1"))

    whenReady(res) {
      (result: Either[Error, ProductoDTO]) =>
        assert(result.isLeft)
        assert(result.left.get.isInstanceOf[CuentaNoEncontrada])
    }

  }


  test("Test ServicioModernizado con plan A y plan B cuando plan A no devuelve nada") {

    class ServicioCuenta extends ServicioModernizado {

      override def planA(productoId: String)(implicit ec: ExecutionContext): FutureEither[Option[ProductoDTO]] = {
        FutureEither.successful(Option.empty[ProductoDTO])
      }

      override def planB(productoId: String)(implicit ec: ExecutionContext): FutureEither[ProductoDTO] = {
        FutureEither.successful(CuentaDTO(id = productoId, titulares = List("A", "B")))
      }
    }

    import scala.concurrent.ExecutionContext.Implicits.global

    val prom = Promise[Either[Error, ProductoDTO]]()

    val servicio = new ServicioCuenta
    val res = convertToFuture(servicio.obtenerInfo("1"))

    whenReady(res) {
      (result: Either[Error, ProductoDTO]) =>
        assert(result.isRight)
        assert(result.right.get.asInstanceOf[CuentaDTO].titulares == List("A", "B"))
    }

  }


  test("Test ServicioModernizado con plan A y plan B cuando plan A de devuelve un valor") {

    class ServicioCuenta extends ServicioModernizado {

      override def planA(productoId: String)(implicit ec: ExecutionContext): FutureEither[Option[ProductoDTO]] = {
        FutureEither.successful(Some(CuentaDTO(productoId, List("Q"))))
      }

      override def planB(productoId: String)(implicit ec: ExecutionContext): FutureEither[ProductoDTO] = {
        FutureEither.successful(CuentaDTO(id = productoId, titulares = List("A", "B")))
      }
    }

    import scala.concurrent.ExecutionContext.Implicits.global

    val prom: Promise[Either[Error, ProductoDTO]] = Promise[Either[Error, ProductoDTO]]()

    val servicio = new ServicioCuenta
    val res = convertToFuture(servicio.obtenerInfo("1"))

    whenReady(res) {
      (result: Either[Error, ProductoDTO]) =>
        assert(result.isRight)
        assert(result.right.get.asInstanceOf[CuentaDTO].titulares == List("Q"))
    }

  }



  test("Test ServicioModernizado con plan A y plan B cuando plan A no devuelve nada y plan B falla") {

    class ServicioCuenta extends ServicioModernizado {

      override def planA(productoId: String)(implicit ec: ExecutionContext): FutureEither[Option[ProductoDTO]] = {
        FutureEither.successful(Option.empty[CuentaDTO])
      }

      override def planB(productoId: String)(implicit ec: ExecutionContext): FutureEither[ProductoDTO] = {
        FutureEither.failure(CuentaNoEncontrada(productoId))
      }
    }

    import scala.concurrent.ExecutionContext.Implicits.global

    val prom = Promise[Either[Error, ProductoDTO]]()

    val servicio = new ServicioCuenta
    val res = convertToFuture(servicio.obtenerInfo("1"))

    whenReady(res) {
      (result: Either[Error, ProductoDTO]) =>
        assert(result.isLeft)
        assert(result.left.get.asInstanceOf[CuentaNoEncontrada].cuentaId == "1")
    }

  }





  import scala.concurrent.Future
  import scala.concurrent.ExecutionContext.Implicits.global

  def convertToFuture[T]( f: FutureEither[T] ) : Future[Either[Error,T]] = {

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



}
