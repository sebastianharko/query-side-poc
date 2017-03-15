name := "query-side-poc"

version := "1.0"

scalaVersion := "2.12.1"


// the library is available in Bintray's JCenter
resolvers += Resolver.jcenterRepo

libraryDependencies += "com.github.dnvriend" %% "akka-persistence-inmemory" % "2.4.17.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.17",
  "com.typesafe.akka" %% "akka-persistence" % "2.4.17",
  "com.typesafe.akka" %% "akka-cluster" % "2.4.17",
  "com.typesafe.akka" %% "akka-cluster-sharding" % "2.4.17",
  "com.typesafe.akka" %% "akka-distributed-data-experimental" % "2.4.17",
  "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.21",
  "com.typesafe.akka" %% "akka-stream" % "2.4.17",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.17",
  "org.json4s" %% "json4s-jackson" % "3.5.0",
  "de.heikoseeberger" %% "akka-http-json4s" % "1.11.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)


