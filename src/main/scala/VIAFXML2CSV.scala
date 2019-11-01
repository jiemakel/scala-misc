
import java.io.FileInputStream
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.zip.GZIPInputStream

import XMLEventReaderSupport._
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.LazyLogging
import javax.xml.stream.XMLEventReader

import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success}

object VIAFXML2CSV extends LazyLogging {
  
  def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvText(text) => content.append(text)
      case er: EvEntityRef =>
        content.append('&')
        content.append(er.entity)
        content.append(';')
      case EvComment(_) => 
      case EvElemEnd(_,_) => break = true 
    }
    return content.toString
  }
  
  def readAggregate(endTag: String, values: HashMap[String,String])(implicit xml: XMLEventReader): Unit = {
    var break = false
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,"data",attrs) =>
        xml.next
        val value = readContents
        values.put(value,if (attrs("count") != null) attrs("count") else "1")
      case EvElemEnd(_,endTag) => break = true       
      case _ => 
    }
  }
  
  def readAlternate(endTag: String)(implicit xml: XMLEventReader): Option[String] = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,"subfield",attrs) if (attrs("code") != null) => attrs("code") match {
        case "e" | "9" => 
        case _ => 
          content.append(readContents)
          content.append(" ")
      }
      case EvElemStart(_,"subfield",_) =>
        content.append(readContents)
        content.append(" ")
      case EvElemEnd(_,endTag) => break = true       
      case _ => 
    }
    if (content.length != 0) {
      content.setLength(content.length - 1)
      return Some(content.toString)
    }
    return None
  }

  def process(record: String)(implicit output: CSVWriter): Future[Unit] = Future {
    implicit val xml = getXMLEventReader(record.substring(record.indexOf("\t")+1))
    var id: String = null
    var nameType: String = ""
    val prefLabels: HashSet[String] = new HashSet[String]()
    val altLabels: HashSet[String] = new HashSet[String]()
    val relLabels: HashSet[String] = new HashSet[String]()
    var birthDate: String = ""
    var deathDate: String = ""
    var dateType: String = ""
    var gender: String = ""
    val nationalities: HashMap[String,String] = new HashMap[String,String]()
    val countries: HashMap[String,String] = new HashMap[String,String]()
    val relatorCodes: HashMap[String,String] = new HashMap[String,String]()
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"viafID",_) => id = readContents
      case EvElemStart(_,"nameType",_) => nameType = readContents
      case EvElemStart(_,"mainHeadings",_) =>
        var break = false
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"text",_) => prefLabels.add(readContents)
          case EvElemEnd(_,"mainHeadings") => break = true       
          case _ => 
        }
      case EvElemStart(_,"gender",_) => gender = readContents
      case EvElemStart(_,"birthDate",_) => birthDate = readContents
      case EvElemStart(_,"deathDate",_) => deathDate = readContents
      case EvElemStart(_,"dateType",_) => dateType = readContents
      case EvElemStart(_,"x400",_) => readAlternate("x400").foreach(altLabels.add(_))
      case EvElemStart(_,"x500",_) => readAlternate("x500").foreach(relLabels.add(_))
      case EvElemStart(_,"nationalityOfEntity",_) => readAggregate("nationalityOfEntity", nationalities)
      case EvElemStart(_,"countries",_) => readAggregate("countries", countries)
      case EvElemStart(_,"RelatorCodes",_) => readAggregate("RelatorCodes", relatorCodes)
      case _ => 
    }
    output.synchronized { output.writeRow(Seq(id,nameType,birthDate,deathDate,dateType,gender,countries.toSeq.map(p => p._1+":"+p._2).mkString(";"),nationalities.toSeq.map(p => p._1+":"+p._2).mkString(";"),relatorCodes.toSeq.map(p => p._1+":"+p._2).mkString(";"),prefLabels.map(_.replace(";","\\;")).mkString(";"),altLabels.map(_.replace(";","\\;")).mkString(";"),relLabels.map(_.replace(";","\\;")).mkString(";"))) }
  }
  
  val numWorkers = sys.runtime.availableProcessors
  val queueCapacity = 2

  implicit val ec = ExecutionContext.fromExecutorService(
   new ThreadPoolExecutor(
     numWorkers, numWorkers,
     0L, TimeUnit.SECONDS,
     new ArrayBlockingQueue[Runnable](queueCapacity) {
       override def offer(e: Runnable) = {
         put(e); // may block if waiting for empty room
         true
       }
     }
   )
  )

  def main(args: Array[String]): Unit = {
    val s = Source.fromInputStream(new GZIPInputStream(new FileInputStream("viaf.xml.gz")), "UTF-8")
    implicit val output: CSVWriter = CSVWriter.open("output.csv")
    output.writeRow(Seq("id","nameType","birthDate","deathDate","dateType","gender","countries","nationalities","relatorCodes","prefLabels","altLabels","relLabels"))
    val f = Future.sequence(for (record <- s.getLines) yield process(record))
    f.onComplete {
      case Failure(t) => logger.error("Processing of at least one linr resulted in an error:" + t.getMessage + ": " + t.printStackTrace)
      case Success(_) => logger.info("Successfully processed all lines.")
    }
    Await.result(f, Duration.Inf)
    output.close()
  }
}
