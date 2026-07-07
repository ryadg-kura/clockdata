name := "analysis-service"
version := "0.1"
scalaVersion := "2.12.18"

// Composant 5 : analyse distribuée (Spark) de la couche Silver,
// répond à 4 questions sur les données stockées.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-avro" % "3.5.0"
)

javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)

fork := true
