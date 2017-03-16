package querysidepoc


import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.event.{Logging, LoggingReceive}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source, Zip}
import org.json4s.{DefaultFormats, jackson}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try

case class ObtenerInfo(id: String)

case class TitularAnadidoALaCuenta(cuentaId: String,
                                   nuevaTitularId: String)

object ActorTitular {

  def props = Props(new ActorTitular)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ TitularAnadidoALaCuenta(_, nuevaTitularId) ⇒ (nuevaTitularId, msg)
    case msg @ ObtenerCuentasForTitular(personaId)  => (personaId, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case TitularAnadidoALaCuenta(cuentaId, nuevaTitularId) => nuevaTitularId.head.toString
    case ObtenerCuentasForTitular(personaId) => personaId.head.toString
  }
}

sealed trait ProductoDTO

case class ObtenerCuentasForTitular(personaId: String)

case class TitularDTO(cuentas: List[String])

class ActorTitular extends PersistentActor with ActorLogging {

  val cuentas: mutable.Set[String] = mutable.Set()

  override def persistenceId = self.path.name

  override def receiveCommand = LoggingReceive {
    case msg @ TitularAnadidoALaCuenta(cuentaId, _) =>
      persist(msg) {
        event => {
          cuentas.add(cuentaId)
          sender ! "Success"
        }
      }
    case msg @ ObtenerCuentasForTitular(personaId) =>
      sender ! TitularDTO(cuentas.toList)
  }

  override def receiveRecover = {
    case TitularAnadidoALaCuenta(cuentaId, _) =>
      cuentas.add(cuentaId)
  }

}

case class CuentaDTO(titulares: List[String]) extends ProductoDTO

case class ObtenerTitulares(cuentaId: String)

class ActorCuenta extends PersistentActor with ActorLogging {

  override def persistenceId = self.path.name

  val titulares = mutable.ArrayBuffer[String]()

  override def receiveCommand = LoggingReceive {
    case msg@TitularAnadidoALaCuenta(cuentaId, titularId) =>
      persist(msg) { event =>
        titulares.append(event.nuevaTitularId)
        sender ! "Success"
      }
    case ObtenerTitulares(_) => sender() ! CuentaDTO(titulares.toList)
  }

  def receiveRecover = {
    case TitularAnadidoALaCuenta(cuentaId, nuevoTitularId) =>
      titulares.append(nuevoTitularId)
  }

}

object ActorCuenta {


  def props = Props(new ActorCuenta)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ TitularAnadidoALaCuenta(cuentaId, nuevoTitularId) ⇒ (cuentaId, msg)
    case msg @ ObtenerTitulares(cuentaId) => (cuentaId, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case TitularAnadidoALaCuenta(cuentaId, nuevoTitularId) => cuentaId.head.toString
    case msg @ ObtenerTitulares(cuentaId) => cuentaId.head.toString
  }
}

class Main

object Main extends App {

  import scala.concurrent.duration._

  implicit val timeout = akka.util.Timeout(10 seconds)
  implicit val system = ActorSystem("query-side-poc")
  implicit val materializer = ActorMaterializer()

  val actorRefSource = Source.actorRef[TitularAnadidoALaCuenta](100, OverflowStrategy.fail)

  import de.heikoseeberger.akkahttpjson4s.Json4sSupport._

  implicit val serialization = jackson.Serialization
  implicit val formats = DefaultFormats



  val logging = Logging(system, classOf[Main])

  val shardingParaCuentas = ClusterSharding(system).start(
    typeName = "cuenta",
    entityProps = ActorCuenta.props,
    settings = ClusterShardingSettings(system),
    extractEntityId = ActorCuenta.extractEntityId,
    extractShardId = ActorCuenta.extractShardId
  )

  val shardingParaTitulares = ClusterSharding(system).start(
    typeName = "titulares",
    entityProps = ActorTitular.props,
    settings = ClusterShardingSettings(system),
    extractEntityId = ActorTitular.extractEntityId,
    extractShardId = ActorTitular.extractShardId
  )


  import akka.stream.scaladsl.GraphDSL.Implicits._

  val flow =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val Publicar =
        builder.add(Broadcast[TitularAnadidoALaCuenta](2))

      val Enriquecer = builder.add(Flow[TitularAnadidoALaCuenta].map(item => item.copy(cuentaId = "CUENTA-" + item.cuentaId)))

      val AlaCuenta = builder.add(Flow[TitularAnadidoALaCuenta].mapAsync(10)(event => (shardingParaCuentas ? event).mapTo[String]))

      val AlTitular = builder.add(Flow[TitularAnadidoALaCuenta].mapAsync(10)(event => (shardingParaTitulares ? event).mapTo[String]))

      val Unir = builder.add(Zip[String, String])

      val Comprobar
      = builder.add(Flow[(String, String)].map(tuple => {
        logging.info("received " + tuple._1 + " and " + tuple._2)
        val items = List(tuple._1, tuple._2)
        if (items.distinct.size == 1 && items.head == "Success")
          "success"
        else
          "failure"
      }
      ))

       Enriquecer ~> Publicar ~> AlaCuenta   ~> Unir.in0
                     Publicar ~> AlTitular   ~> Unir.in1
                                                Unir.out ~> Comprobar

       FlowShape(Enriquecer.in, Comprobar.out)

    })

  val sourceActorRef = flow.runWith(actorRefSource, Sink.foreach(msg => logging.info("acknowledge")))._1

  val route = post {
    path("cuenta" / Segment / "titular" / Segment) {
      (cuentaId: String, titularId: String) => {
        val event = TitularAnadidoALaCuenta(cuentaId, titularId)
        sourceActorRef ! event
        complete(OK)
      }
    }
  } ~ get {
    path("cuenta" / Segment) {
      cuentaId: String => {
        onComplete((shardingParaCuentas ? ObtenerTitulares(cuentaId)).mapTo[CuentaDTO]) {
          case scala.util.Success(dto) => complete(OK -> dto)
          case scala.util.Failure(_) => complete(InternalServerError)
        }
      }
    }
  } ~ get {
    path("titular" / Segment) {
      personaId: String => {
        onComplete((shardingParaTitulares ? ObtenerCuentasForTitular(personaId)).mapTo[TitularDTO]) {
          case scala.util.Success(dto) => complete(OK -> dto)
          case scala.util.Failure(_) => complete(InternalServerError)
        }
      }
    }
  }

  Http().bindAndHandle(route, "localhost", 8080)

}


sealed trait Error

case class PersonaNoEncontrado(personaId: String) extends Error
case class ErrorGenerico(e: Throwable) extends Error

sealed trait TipoProducto
case object Tarjeta extends TipoProducto
case object Cuenta extends TipoProducto
case object Prestamo extends TipoProducto
case object Hipoteca extends TipoProducto // Legacy backend (no events)

case class ListaProductos(productos: List[(String, TipoProducto)])

trait ServicioProductos {

  def obtenerListaProductos(personaId: String): FutureEither[ListaProductos] = {

    val promis = Promise[ Either[Error, ListaProductos ]]()

    new Thread {
      () =>
        Thread.sleep( (Math.random() * 1000).toInt)
        promis.complete(Try(Right(ListaProductos(List("1" -> Cuenta, "2" -> Hipoteca)))))
    }.run()

    // mock
    val result = new FutureEither(promis.future)
    result
  }

}

case class HipotecaDTO(id: String, saldo: Int) extends ProductoDTO

trait Servicio {

  def obtenerInfo(productoId: String): FutureEither[ProductoDTO]

}


trait ServicioDefault {

  def obtenerProdInfo(productoId: String): FutureEither[ProductoDTO]

  def obtenerInfo(productoId: String): FutureEither[ProductoDTO] = obtenerProdInfo(productoId)
}

trait ServicioEntidad {
  def obtenerDesdeEntidad(productoId: String): FutureEither[Option[ProductoDTO]]
}

trait ServicioConCache extends ServicioDefault with ServicioEntidad {

   override def obtenerInfo(productoId: String): FutureEither[ProductoDTO] = {

    import FutureEither.successful

    for {
      cache <- super.obtenerDesdeEntidad(productoId)

      result <- cache match {
                  case Some( value ) => successful(  value )
                  case _ => super.obtenerProdInfo( productoId )
                }

    } yield result

  }

}

class ServicioCuentas extends ServicioConCache {

  override def obtenerDesdeEntidad(productoId: String): FutureEither[Option[ProductoDTO]] = ???

  override def obtenerProdInfo(productoId: String): FutureEither[ProductoDTO] = ???
}






class ServicioPosicionesGlobales(servicioProductos: ServicioProductos, servicios: Map[TipoProducto, Servicio])
                                (implicit val ec: scala.concurrent.ExecutionContext) {

  def obtenerPosicionGlobal(personaId: String) : FutureEither[List[ProductoDTO]] = {

    import FutureEither._

    for {

      listaProductos <- servicioProductos.obtenerListaProductos(personaId)

      listaDetalleProducto <- successful(listaProductos.productos.map { case (id, tipoProducto) =>  servicios(tipoProducto).obtenerProdInfo(id) } )

      positionGlobal <- sequence(listaDetalleProducto)

    } yield positionGlobal

  }

}



import scala.concurrent.{ExecutionContext => ExecCtx}

object FutureEither {

  def fromFuture[T](future: Future[T])(implicit ec: ExecCtx): FutureEither[T] = {

    val futureE : Future[Either[Error,T]] = future map {
      s : T => Right[Error,T](s)
    } recover {
      case e: Throwable => Left( ErrorGenerico(e) )
    }

    new FutureEither( futureE )

  }


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

class FutureEither[B](val future: Future[Either[Error ,B]]) {

  def toFutureEither: FutureEither[B] = this

  def map[BB](f: B => BB)(implicit ec: ExecCtx) =
    new FutureEither[BB](
      future recover {
        case e: Throwable => Left( ErrorGenerico(e) )
      } map {
        case Left(a)  => Left(a)
        case Right(b) => Right(f(b))
      }
    )

  def flatMap[BB](f: B => FutureEither[BB])(implicit ec: ExecCtx) =
    new FutureEither[BB](
      future recover {
        case e: Throwable => Left( ErrorGenerico(e) )
      } flatMap {
        case Left(a)  => Future successful Left(a)
        case Right(b) => f(b).future
      }
    )

}

/*
case class TransactionEvent(transactionId: String,
                            senderAccountId: String,
                            receiverAccountId: String,
                            amount: Int
                            )

class AccountService {

  def getAccountInformation(accountId: String) = Future.successful {
    AccountInformation(accountId, holders = List("P", "Q"))
  }

}

case class AccountInformation(accountId: String,
                              holders: List[String])

*/

/*
CreditCard           |  New Backend (from events) -> no cache
Loan                 |  New Backend (from events) -> no cache
House                |  New Backend (from events) -> no cache
Checking Account     |  New Backend (from events) -> no cache
Savings Account      |  Legacy Backend (REST API call) but we can still create an actor (persistent)   ... cache (TTL ... 2 seconds)
*/


