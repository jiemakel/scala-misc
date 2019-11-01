name := """scala-misc"""

version := "1.2"

scalaVersion := "2.13.1"

javacOptions ++= Seq("-source","9","-target","9")
scalacOptions += "-target:9"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.7.4",
  "fi.seco" %% "lucene-morphologicalanalyzer" % "1.2.1" exclude("commons-logging", "commons-logging"),
  "fi.seco" % "lexicalanalysis-resources-fi-complete" % "1.5.16",
  "fi.hsci" %% "lucene-normalisinganalyzer" % "1.0.2",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M1",
  "org.json4s" %% "json4s-native" % "3.7.0-M1",
  "com.fasterxml" % "aalto-xml" % "1.0.0",
  "org.apache.jena" % "jena-core" % "3.11.0" exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j") exclude("commons-logging", "commons-logging"),
  "org.apache.jena" % "jena-arq" % "3.11.0" exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j") exclude("commons-logging", "commons-logging"),
  "com.github.nscala-time" %% "nscala-time" % "2.22.0",
  "org.scalanlp" %% "breeze" % "1.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
  "cc.mallet" % "mallet" % "2.0.8" exclude("commons-logging", "commons-logging"),
  "info.freelibrary" % "freelib-marc4j" % "2.6.6",
  "it.geosolutions.imageio-ext" % "imageio-ext-kakadu" % "1.1.19",
  "it.geosolutions.imageio-ext" % "imageio-ext-kakadujni" % "6.3.1",
  "it.geosolutions.imageio-ext" % "imageio-ext-tiff" % "1.1.19",
  "org.apache.lucene" % "lucene-core" % "8.0.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "8.0.0",
  "org.apache.lucene" % "lucene-queryparser" % "8.0.0",
  "org.apache.lucene" % "lucene-highlighter" % "8.0.0",
  "fi.seco" %% "lucene-perfieldpostingsformatordtermvectorscodec" % "1.1.5",
  "com.sleepycat" % "je" % "7.5.11",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "com.google.guava" % "guava" % "21.0",
  "com.breinify" % "brein-time-utilities" % "1.6.4" exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j") exclude("commons-logging", "commons-logging"),
  "org.rogach" %% "scallop" % "3.3.1",
  "org.jsoup" % "jsoup" % "1.10.2",
  "com.optimaize.languagedetector" % "language-detector" % "0.6" exclude("com.intellij","annotations"),
  "org.jetbrains.xodus" % "xodus-environment" % "1.3.0" exclude("org.jetbrains","annotations"),
  "com.github.tototoshi" %% "scala-csv" % "1.3.6",
  "junit" % "junit" % "4.12" % "test"
)

resolvers ++= Seq(
  Resolver.mavenLocal,
  "Oracle" at "https://download.oracle.com/maven/",
  "boundless" at "https://repo.boundlessgeo.com/main/",
  "geotoolkit" at "https://maven.geotoolkit.org/"
)

fork in run := true

assemblyMergeStrategy in assembly := {
  case "checkstyle/checkstyle.xml" => MergeStrategy.first
  case "is2/util/DB.class" => MergeStrategy.first
  case "fi/seco/lexical/hfst/resources.lst" => MergeStrategy.filterDistinctLines
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

