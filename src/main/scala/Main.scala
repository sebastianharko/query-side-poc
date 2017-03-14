package querysidepoc


import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Source}

import scala.collection.mutable


case class AccountInformation(accountId: String,
                              holders: List[String])

case class HolderAddedToAccount(accountId: String,
                                newHolderId: String)

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





object Main {

  implicit val system = ActorSystem("query-side-poc")

  implicit val materializer = ActorMaterializer()

  val shardingForAccounts = ClusterSharding(system).start(
      typeName = "account",
      entityProps = AccountActor.props,
      settings = ClusterShardingSettings(system),
      extractEntityId = AccountActor.extractEntityId,
      extractShardId = AccountActor.extractShardId
  )


  val exampleEvent = HolderAddedToAccount(accountId = "12710", newHolderId = "74")


  val source = Source.single(exampleEvent)

  import akka.stream.scaladsl.GraphDSL.Implicits._
  RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>

    val A: Outlet[HolderAddedToAccount] = builder.add(source).out

    val B: UniformFanOutShape[HolderAddedToAccount, HolderAddedToAccount] =
      builder.add(Broadcast[HolderAddedToAccount](2))

    val C: FlowShape[HolderAddedToAccount, HolderAddedToAccount]
      = builder.add(Flow[HolderAddedToAccount].map(item => item.copy(accountId = "ING-" + item.accountId)))

    val D: FlowShape[HolderAddedToAccount, String]
      = builder.add(Flow[HolderAddedToAccount].mapAsync(10)(
        event => (shardingForAccounts ? event).mapTo[String]
    ))


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

    A ~>  C ~> B ~> D
               B

    ClosedShape
  })





}
