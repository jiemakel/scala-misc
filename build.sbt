name := """scala-misc"""

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Oracle" at "http://download.oracle.com/maven/"

libraryDependencies ++= Seq(
"org.json4s" %% "json4s-native" % "3.2.10",
"com.fasterxml" % "aalto-xml" % "1.0.0",
"org.apache.jena" % "jena-core" % "3.0.1" exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j") exclude("commons-logging", "commons-logging"),
"org.apache.jena" % "jena-arq" % "3.0.1" exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j") exclude("commons-logging", "commons-logging"),
"com.github.nscala-time" %% "nscala-time" % "1.2.0",
"com.bizo" %% "mighty-csv" % "0.2",
"org.scalanlp" %% "breeze" % "0.10",
"com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
"ch.qos.logback" % "logback-classic" % "1.1.3",
"org.slf4j" % "log4j-over-slf4j" % "1.7.12",
"cc.mallet" % "mallet" % "2.0.8-RC3",
"info.freelibrary" % "freelib-marc4j" % "2.6.6",
"it.geosolutions.imageio-ext" % "imageio-ext-kakadu" % "1.1.13",
"it.geosolutions.imageio-ext" % "imageio-ext-kakadujni" % "6.3.1",
"it.geosolutions.imageio-ext" % "imageio-ext-tiff" % "1.1.13",
"org.apache.lucene" % "lucene-analyzers-common" % "6.2.1",
"org.apache.lucene" % "lucene-queryparser" % "6.2.1",
"org.apache.lucene" % "lucene-codecs" % "6.2.1",
"com.sleepycat" % "je" % "7.0.6",
"org.apache.commons" % "commons-math3" % "3.6.1"
)

resolvers ++= Seq(
    Resolver.mavenLocal,
    "imageio-ext" at "http://maven.geo-solutions.it/",
    "geotoolkit" at "http://maven.geotoolkit.org/"
)

fork in run := true

assemblyMergeStrategy in assembly := {
  case "checkstyle/checkstyle.xml" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

