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
import akka.util.Timeout
import org.json4s.{DefaultFormats, jackson}

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.util.Try
import FutureEither.convertToFuture

case class ObtenerInfo(id: String)

case class TitularAnadidoALaCuenta(cuentaId: String,
                                   nuevaTitularId: String)

object ActorTitular {

  val TitularRegionName = "titular"

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

case class CuentaDTO(id: String, titulares: List[String]) extends ProductoDTO

case class ObtenerTitulares(cuentaId: String)

case object NoEncontrada extends ProductoDTO

class ActorCuenta extends PersistentActor with ActorLogging {

  override def persistenceId = self.path.name

  val titulares = mutable.ArrayBuffer[String]()

  override def receiveCommand = LoggingReceive {

    case msg@TitularAnadidoALaCuenta(cuentaId, titularId) =>
      persist(msg) { event =>
        titulares.append(event.nuevaTitularId)
        sender ! "Success"
      }

    case ObtenerTitulares(cuentaId) =>
       // to keep the other demo working
       sender() ! CuentaDTO(cuentaId, titulares.toList)

    case ObtenerInfo(cuentaId) =>
      if (titulares.nonEmpty)
        sender() ! CuentaDTO(cuentaId, titulares.toList)
      else
        sender ! NoEncontrada

  }

  def receiveRecover = {
    case TitularAnadidoALaCuenta(cuentaId, nuevoTitularId) =>
      titulares.append(nuevoTitularId)
  }

}

object ActorCuenta {

  val CuentaRegionName = "cuenta"

  def props = Props(new ActorCuenta)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ TitularAnadidoALaCuenta(cuentaId, nuevoTitularId) ⇒ (cuentaId, msg)
    case msg @ ObtenerTitulares(cuentaId) => (cuentaId, msg)
    case msg @ ObtenerInfo(id) => (id, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case TitularAnadidoALaCuenta(cuentaId, nuevoTitularId) => cuentaId.head.toString
    case msg @ ObtenerTitulares(cuentaId) => cuentaId.head.toString
    case msg @ ObtenerInfo(id) => id.head.toString
  }
}

class Main

object Main extends App {

  import scala.concurrent.duration._

  implicit val timeout = akka.util.Timeout(10 seconds)
  implicit val system = ActorSystem("query-side-poc")
  implicit val materializer = ActorMaterializer()

  implicit val ec: ExecutionContext = system.dispatcher

  val servicioCuentas = new SerivicioCuentas(system)
  val servicioHipotecas = new ServicioHipotecas

  val servicios: Map[TipoProducto, Servicio] = Map(
    Cuenta -> servicioCuentas,
    Hipoteca -> servicioHipotecas
  )

  val servicioProductos = new ServicioProductos

  val servicioPosicionesGlobales = new ServicioPosicionesGlobales(servicioProductos, servicios)


  val actorRefSource = Source.actorRef[TitularAnadidoALaCuenta](100, OverflowStrategy.fail)

  import de.heikoseeberger.akkahttpjson4s.Json4sSupport._

  implicit val serialization = jackson.Serialization
  implicit val formats = DefaultFormats


  val logging = Logging(system, classOf[Main])

  val shardingParaCuentas = ClusterSharding(system).start(
    typeName = ActorCuenta.CuentaRegionName,
    entityProps = ActorCuenta.props,
    settings = ClusterShardingSettings(system),
    extractEntityId = ActorCuenta.extractEntityId,
    extractShardId = ActorCuenta.extractShardId
  )

  val shardingParaTitulares = ClusterSharding(system).start(
    typeName = ActorTitular.TitularRegionName,
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

  implicit def fromFutEToFut[T](v: FutureEither[T]): Future[Either[Error, T]] = convertToFuture(v)


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
  } ~ get {
    path("posicion" / Segment) {
      personaId: String => {
        onComplete(servicioPosicionesGlobales.obtenerPosicionGlobal(personaId)) {
          case scala.util.Success(actualResult) => actualResult match {
            case Left(error) => complete(InternalServerError -> error.description)
            case Right(value) => complete(OK -> value)
          }
          case scala.util.Failure(_) => complete(InternalServerError)
        }
      }
    }
  }

  Http().bindAndHandle(route, "localhost", 8080)

}


sealed trait Error {
  val description: String
}

case class CuentaNoEncontrada(cuentaId: String) extends Error {
  override val description = "Cuenta con id " + cuentaId + " no encontrada"
}
case class PersonaNoEncontrado(personaId: String) extends Error {
  override val description = "Persona con id " + personaId + " no encontrada"

}
case class ErrorGenerico(e: Throwable) extends Error {
  override val description = "Error generico " + e.getMessage
}

sealed trait TipoProducto
case object Tarjeta extends TipoProducto
case object Cuenta extends TipoProducto
case object Prestamo extends TipoProducto
case object Hipoteca extends TipoProducto // Legacy backend (no events)

case class ListaProductos(productos: List[(String, TipoProducto)])

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

case class HipotecaDTO(id: String, total: Int, restante: Int, intereses: Int) extends ProductoDTO

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



import scala.concurrent.{ExecutionContext => ExecCtx}

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


