package querysidepoc


import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.event.{Logging, LoggingReceive}
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}

import scala.collection.mutable


case class AccountInformation(accountId: String,
                              holders: List[String])

case class HolderAddedToAccount(accountId: String,
                                newHolderId: String)


object GlobalPositionActor {

  def props = Props(new GlobalPositionActor)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ HolderAddedToAccount(_, newHolderId) ⇒ (newHolderId, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case HolderAddedToAccount(accountId, newHolderId) => newHolderId.head.toString

  }

}


class GlobalPositionActor extends PersistentActor with ActorLogging {

  val myAccounts: mutable.Set[String] = mutable.Set()

  override def persistenceId = self.path.name

  override def receiveCommand = LoggingReceive {
    case msg@HolderAddedToAccount(accountId, _) =>
      persist(msg) {
        event => {
          myAccounts.add(accountId)
          sender ! "Success"
        }
      }
  }

  override def receiveRecover = {
    case HolderAddedToAccount(accountId, _) =>
      myAccounts.add(accountId)
  }

}

class AccountActor extends PersistentActor with ActorLogging {

  override def persistenceId = self.path.name

  val holdersList = mutable.ArrayBuffer[String]()

  override def receiveCommand =  LoggingReceive {
    case msg@HolderAddedToAccount(accountId, newHolderId) =>
      persist(msg) { event =>
        holdersList.append(event.newHolderId)
        sender ! "Success"
      }
  }

  def receiveRecover = {
    case HolderAddedToAccount(accountId, newHolderId) =>
      holdersList.append(newHolderId)
  }

}

object AccountActor {


  def props = Props(new AccountActor)

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg @ HolderAddedToAccount(accountId, newHolderId) ⇒ (accountId, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case HolderAddedToAccount(accountId, newHolderId) => accountId.head.toString

  }
}


class Main

object Main extends App {

  implicit val system = ActorSystem("query-side-poc")

  implicit val materializer = ActorMaterializer()

  val logging = Logging(system, classOf[Main])

  val shardingForAccounts = ClusterSharding(system).start(
      typeName = "account",
      entityProps = AccountActor.props,
      settings = ClusterShardingSettings(system),
      extractEntityId = AccountActor.extractEntityId,
      extractShardId = AccountActor.extractShardId
  )

  val shardingForPosicionGlobal = ClusterSharding(system).start(
    typeName = "posicion global",
    entityProps = GlobalPositionActor.props,
    settings = ClusterShardingSettings(system),
    extractEntityId = GlobalPositionActor.extractEntityId,
    extractShardId = GlobalPositionActor.extractShardId
  )



  val exampleEvent = HolderAddedToAccount(accountId = "12710", newHolderId = "74")


  val source = Source.single(exampleEvent)

  import scala.concurrent.duration._
  implicit val timeout = akka.util.Timeout(10 seconds)

  import akka.stream.scaladsl.GraphDSL.Implicits._

  val graph =
  RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>

    val Src: Outlet[HolderAddedToAccount] = builder.add(source).out

    val Publicar =
      builder.add(Broadcast[HolderAddedToAccount](2))

    val Cambio
      = builder.add(Flow[HolderAddedToAccount].map(item => item.copy(accountId = "ING-" + item.accountId)))

    val AlaCuenta
      = builder.add(Flow[HolderAddedToAccount].mapAsync(10)(event =>
          (shardingForAccounts ? event).mapTo[String]))

    val AlaPosicionGlobal
      = builder.add(Flow[HolderAddedToAccount].mapAsync(10)(event =>
         (shardingForPosicionGlobal ? event).mapTo[String]))

    val Unir = builder.add(Zip[String, String])

    val Comprobar: FlowShape[(String, String), String]
      = builder.add(Flow[(String, String)].map(tuple => {
          logging.info("received " + tuple._1 + " and " + tuple._2)
          val items = List(tuple._1, tuple._2)
          if (items.distinct.size == 1 && items.head == "Success")
             "success"
           else
            "failure"
         }
    ))

    val Sumidero = builder.add(Sink.foreach(println)).in

    /*
    val C: UniformFanInShape[Int, Int]    = builder.add(Merge[Int](2))
    val D: FlowShape[Int, Int]            = builder.add(Flow[Int].map(_ + 1))
    val E: UniformFanOutShape[Int, Int]   = builder.add(Balance[Int](2))
    val F: UniformFanInShape[Int, Int]    = builder.add(Merge[Int](2))
    val G: Inlet[Any]                     = builder.add(Sink.foreach(println)).in

                     C     <~      F
    A  ~>     B  ~>  C     ~>      F
              B  ~>  D  ~>  E  ~>  F
                            E  ~>  G

    */

    Src ~> Cambio ~> Publicar ~> AlaCuenta         ~> Unir.in0
                     Publicar ~> AlaPosicionGlobal ~> Unir.in1
                                                      Unir.out ~> Comprobar ~> Sumidero

    ClosedShape
  })

  graph.run()

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
*/


