import sbtassembly.AssemblyPlugin.defaultShellScript

name := """banktivity-stock-quote-sync"""
organization := "my.edu.clhs"
version := "1.0-SNAPSHOT"

scalaVersion := "2.13.1"

resolvers += Resolver.mavenLocal

libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % "10.1.11"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.4"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "1.1.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3" % Runtime
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.30.1" % Runtime

assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultShellScript))

assemblyJarName in assembly := s"ibdq"
