name := "Analytics"
version := "1.6"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0"
resolvers ++=Seq(
"Akka Repository" at "http://repo.akka.io/releases/",
"Spray Repository" at "http://repo.spray.cc/"
)
