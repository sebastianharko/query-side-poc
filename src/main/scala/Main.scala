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

case class TitularesDTO(titulares: List[String])

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
    case ObtenerTitulares(_) => sender() ! TitularesDTO(titulares.toList)
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

  val shardingParaPosicionGlobal = ClusterSharding(system).start(
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

      val AlaCuenta
      = builder.add(Flow[TitularAnadidoALaCuenta].mapAsync(10)(event =>
        (shardingParaCuentas ? event).mapTo[String]))

      val AlTitular
      = builder.add(Flow[TitularAnadidoALaCuenta].mapAsync(10)(event =>
        (shardingParaPosicionGlobal ? event).mapTo[String]))

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
        onComplete((shardingParaCuentas ? ObtenerTitulares(cuentaId)).mapTo[TitularesDTO]) {
          case scala.util.Success(dto) => complete(OK -> dto)
          case scala.util.Failure(_) => complete(InternalServerError)
        }
      }
    }
  } ~ get {
    path("titular" / Segment) {
      personaId: String => {
        onComplete((shardingParaPosicionGlobal ? ObtenerCuentasForTitular(personaId)).mapTo[TitularDTO]) {
          case scala.util.Success(dto) => complete(OK -> dto)
          case scala.util.Failure(_) => complete(InternalServerError)
        }
      }
    }
  }

  Http().bindAndHandle(route, "localhost", 8080)



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


