name := """scala-misc"""

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.json4s" %% "json4s-native" % "3.2.10",
"org.apache.jena" % "jena-core" % "3.0.1" exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j") exclude("commons-logging", "commons-logging"),
"org.apache.jena" % "jena-arq" % "3.0.1" exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j") exclude("commons-logging", "commons-logging"),
"com.github.nscala-time" %% "nscala-time" % "1.2.0",
"com.bizo" %% "mighty-csv" % "0.2",
"org.scalanlp" %% "breeze" % "0.10",
"com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
"ch.qos.logback" % "logback-classic" % "1.1.3",
"org.slf4j" % "log4j-over-slf4j" % "1.7.12",
"cc.mallet" % "mallet" % "2.0.8-RC2",
"info.freelibrary" % "freelib-marc4j" % "2.6.6"
)

resolvers ++= Seq(
    Resolver.mavenLocal
)

fork in run := true
