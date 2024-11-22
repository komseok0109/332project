ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "distributedSortingProject",
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "io.grpc" % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion,
      "com.google.protobuf" % "protobuf-java" % "3.21.12",
      "com.github.scopt" %% "scopt" % "4.1.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",
      "org.typelevel" %% "cats-effect" % "3.5.0",
      "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
    ),
    Compile / PB.targets := Seq(
      scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "scalapb"
    )
  )
