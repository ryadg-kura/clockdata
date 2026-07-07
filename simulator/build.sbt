name := "simulator"
version := "0.1"
scalaVersion := "2.13.14"

// Composant 1 : pas de Spark, uniquement le client Kafka.
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.7.0"

fork := true
