package querysidepoc

import akka.actor.ActorSystem
import akka.cluster.sharding.ClusterSharding
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Try
import scala.concurrent.duration._


class ServicioProductos {

  def obtenerListaProductos(personaId: String)(implicit ec:ExecutionContext): FutureEither[ListaProductos] = {

    val promis = Promise[ ListaProductos ]()

    new Thread {
      () =>
        Thread.sleep( (Math.random() * 1000).toInt)
      promis.complete(Try( ListaProductos(List("CUENTA-1" -> Cuenta, "2" -> Hipoteca))))
    }.run()

    // mock
    FutureEither.successfulWith(promis.future)
  }

}



trait Servicio {
  def obtenerInfo(productoId: String)(implicit ec: ExecutionContext): FutureEither[ProductoDTO]
}

trait ConPlanA {
  def planA(productoId: String)(implicit ec: ExecutionContext): FutureEither[Option[ProductoDTO]]
}

trait ConPlanB {
  def planB(productoId: String)(implicit ec: ExecutionContext): FutureEither[ProductoDTO]
}


trait ServicioModernizado extends Servicio with ConPlanA with ConPlanB {

  override def obtenerInfo(productoId: String)(implicit ec: ExecutionContext) = {
    import FutureEither.successful

    for {
      resultA <- planA(productoId)
      result <- resultA match {
        case Some( value ) => successful(  value )
        case _ => planB(productoId)
      }
    } yield result
  }
}

trait ConActores {
  self: ConPlanA =>

  val actorSystem: ActorSystem

  implicit val timeout: akka.util.Timeout

  val nombreRegionSharding: String

  def planA(productoId: String)(implicit ec: ExecutionContext): FutureEither[Option[ProductoDTO]] = {
    val result = (ClusterSharding(actorSystem).shardRegion(nombreRegionSharding) ? ObtenerInfo(productoId))
      .mapTo[ProductoDTO].map { case NoEncontrada => Option.empty[ProductoDTO]
    case q:ProductoDTO => Some(q) }

    FutureEither.successfulWith(result)
  }

}

class SerivicioCuentas(val actorSystem: ActorSystem) extends ServicioModernizado with ConActores {

  override val nombreRegionSharding: String = ActorCuenta.CuentaRegionName

  implicit val timeout: akka.util.Timeout = Timeout(7 seconds)

  override def planB(productoId: String)(implicit ec: ExecutionContext): FutureEither[ProductoDTO] = {

    val promis = Promise[CuentaDTO]()

    new Thread {
      () =>
        Thread.sleep((Math.random() * 1000).toInt)
      promis.complete(Try(CuentaDTO(productoId, titulares = List("A", s"B"))))
    }.run()

    // mock
    FutureEither.successfulWith(promis.future)
  }

}


class ServicioHipotecas extends Servicio {
  override def obtenerInfo(productoId: String)(implicit ec: ExecutionContext): FutureEither[ProductoDTO] =
  {
    val promis = Promise[HipotecaDTO]()

    new Thread {
      () =>
        Thread.sleep((Math.random() * 1000).toInt)
      promis.complete(Try(HipotecaDTO(productoId, total = 250000, restante = 125000, intereses = 5)))
    }.run()

    // mock
    FutureEither.successfulWith(promis.future)
  }
}


class ServicioPosicionesGlobales(servicioProductos: ServicioProductos, servicios: Map[TipoProducto, Servicio])
                                (implicit val ec: scala.concurrent.ExecutionContext) {

  def obtenerPosicionGlobal(personaId: String) : FutureEither[List[ProductoDTO]] = {

    import FutureEither._

    for {

      listaProductos <- servicioProductos.obtenerListaProductos(personaId)

      listaDetalleProducto <- successful(listaProductos.productos.map { case (id: String, tipoProducto: TipoProducto) =>  servicios(tipoProducto).obtenerInfo(id) } )

      positionGlobal <- sequence(listaDetalleProducto)

    } yield positionGlobal

  }

}