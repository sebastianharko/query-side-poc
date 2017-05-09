package querysidepoc


import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.event.{Logging, LoggingReceive}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source, Zip, ZipN}
import akka.util.Timeout
import org.json4s.{DefaultFormats, jackson}

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.util.Try
import FutureEither.convertToFuture

import scala.collection.mutable.ArrayBuffer

case class ObtenerInfo(id: String)

case class TitularAnadidoALaCuenta(cuentaId: String,
                                   nuevaTitularId: String)

case class TitularAnadidoALaCuentaComplex(existingTitularId: String, cuentaId: String, nuevaTitularId: String)



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


class ActorCuentaComplex(complexTitulares: ActorRef) extends PersistentActor with ActorLogging {

  val existingTitulares = mutable.ArrayBuffer[String]()

  override def persistenceId = self.path.name

  override def receiveCommand: Receive = LoggingReceive {

    case msg @ TitularAnadidoALaCuenta(cuentaId, newTitularId) =>
      persist(msg) { event =>
        existingTitulares.append(event.nuevaTitularId)
        existingTitulares.foreach {
          existingTitularId => complexTitulares ! TitularAnadidoALaCuentaComplex(existingTitularId, cuentaId, newTitularId)
        }
        sender ! "Success"
      }
  }

  def receiveRecover = {
    case TitularAnadidoALaCuenta(cuentaId, nuevoTitularId) =>
      existingTitulares.append(nuevoTitularId)
  }

}

object ActorCuentaComplex {

  val CuentaComplexRegionName = "cuenta-complex"

  def props(titularesComplexSharding: ActorRef) = Props(new ActorCuentaComplex(titularesComplexSharding))

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ TitularAnadidoALaCuenta(cuentaId, nuevaTitularId) => (cuentaId, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case TitularAnadidoALaCuenta(cuentaId, nuevoTitularId) => cuentaId.head.toString
  }

}

object ActorTitularComplex {

  val TitularComplexRegionName = "complex"

  def props = Props(new ActorTitularComplex)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ TitularAnadidoALaCuentaComplex(existingTitularId, cuentaId, nuevaTitularId) ⇒ (existingTitularId, TitularAnadidoALaCuenta(cuentaId, nuevaTitularId))
    case msg @ ObtenerCuentasForTitular(id) => (id, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case TitularAnadidoALaCuentaComplex(existingTitularId, cuentaId, nuevaTitularId) => existingTitularId.head.toString
    case msg @ ObtenerCuentasForTitular(id) => id.head.toString
  }
}


class ActorTitularComplex extends PersistentActor with ActorLogging {

  import mutable.ArrayBuffer

  var cuentas: Map[String, ArrayBuffer[String]] = Map()

  override def persistenceId = self.path.name

  override def receiveCommand = LoggingReceive {

    case msg @ TitularAnadidoALaCuenta(cuentaId, nuevaTitularId) =>
      persist(msg) {
        event => {
          onTitularesAnadidoALaCuenta(event)
        }
      }
    case msg @ ObtenerCuentasForTitular(_) =>
      sender() ! cuentas
  }

  def onTitularesAnadidoALaCuenta(event: TitularAnadidoALaCuenta) = {
    val cuentaId = event.cuentaId
    val nuevaTitularId = event.nuevaTitularId

    cuentas.get(cuentaId) match {
        case None => cuentas = cuentas.+(cuentaId -> ArrayBuffer(nuevaTitularId))
        case Some(titulares) => titulares.append(nuevaTitularId)
    }
  }


  override def receiveRecover = {
    case event @ TitularAnadidoALaCuenta(cuentaId, nuevaTitularId) =>
      onTitularesAnadidoALaCuenta(event)
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

  val shardingParaTitularesComplex: ActorRef = ClusterSharding(system).start(
    typeName = ActorTitularComplex.TitularComplexRegionName,
    entityProps = ActorTitularComplex.props,
    settings = ClusterShardingSettings(system),
    extractEntityId = ActorTitularComplex.extractEntityId,
    extractShardId = ActorTitularComplex.extractShardId
  )

  val shardingParaCuentasComplex = ClusterSharding(system).start(
    typeName = ActorCuentaComplex.CuentaComplexRegionName,
    entityProps = ActorCuentaComplex.props(shardingParaTitularesComplex),
    settings = ClusterShardingSettings(system),
    extractEntityId = ActorCuentaComplex.extractEntityId,
    extractShardId = ActorCuentaComplex.extractShardId
  )

  import akka.stream.scaladsl.GraphDSL.Implicits._

  val flow =
    Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val Publicar =
        builder.add(Broadcast[TitularAnadidoALaCuenta](3))

      val Enriquecer = builder.add(Flow[TitularAnadidoALaCuenta].map(item => item.copy(cuentaId = item.cuentaId)))

      val AlaCuenta = builder.add(Flow[TitularAnadidoALaCuenta].mapAsync(10)(event => (shardingParaCuentas ? event).mapTo[String]))

      val AlTitular = builder.add(Flow[TitularAnadidoALaCuenta].mapAsync(10)(event => (shardingParaTitulares ? event).mapTo[String]))

      val AlaCuentaComplex = builder.add(Flow[TitularAnadidoALaCuenta].mapAsync(10)(event => (shardingParaCuentasComplex ? event).mapTo[String]))

      val Unir = builder.add(ZipN[String](3))

      val Comprobar
      = builder.add(Flow[Seq[String]].map(items => {
        if (items.distinct.size == 1 && items.head == "Success")
          "success"
        else
          "failure"
      }
      ))

       Enriquecer ~> Publicar ~> AlaCuentaComplex   ~> Unir.in(0)
                     Publicar ~> AlTitular          ~> Unir.in(1)
                     Publicar ~> AlaCuenta          ~> Unir.in(2)
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
  } ~ get {
    path("complex" / Segment) {
      personaId: String => {
        onComplete((shardingParaTitularesComplex ? ObtenerCuentasForTitular(personaId)).mapTo[Map[String, ArrayBuffer[String]]]) {
          case scala.util.Success(dto: Map[String, ArrayBuffer[String]]) => complete(OK -> dto)
          case scala.util.Failure(ex) =>
            logging.error(ex, "error in titular complex query")
            complete(InternalServerError)
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
case class PersonaNoEncontrada(personaId: String) extends Error {
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

case class HipotecaDTO(id: String, total: Int, restante: Int, intereses: Int) extends ProductoDTO


/*
Account              |  New Backend ... but can fallback
Hipoteca             |  Legacy Backend (REST API call)
*/


