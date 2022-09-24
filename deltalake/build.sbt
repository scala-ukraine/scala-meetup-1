ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "deltalake",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % "3.3.0",
      "io.delta"         %% "delta-core" % "2.1.0"
    )
  )
