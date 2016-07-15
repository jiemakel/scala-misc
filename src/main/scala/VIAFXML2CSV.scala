import com.bizo.mighty.csv.CSVReader
import java.net.URLEncoder
import org.apache.jena.riot.RDFFormat
import org.apache.jena.riot.RDFDataMgr
import java.io.FileOutputStream
import com.bizo.mighty.csv.CSVDictReader
import com.bizo.mighty.csv.CSVReaderSettings
import scala.io.Source
import scala.xml.pull.XMLEventReader
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvText
import scala.xml.pull.EvElemEnd
import scala.xml.MetaData
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import java.io.PrintWriter
import java.io.File
import javax.imageio.ImageIO
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.pull.EvEntityRef
import scala.xml.parsing.XhtmlEntities
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import com.typesafe.scalalogging.LazyLogging

import scala.xml.Elem
import scala.xml.factory.XMLLoader
import javax.xml.parsers.SAXParser
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.FileReader
import scala.io.BufferedSource
import scala.xml.pull.EvComment
import java.io.PushbackInputStream
import java.util.zip.GZIPInputStream
import com.bizo.mighty.csv.CSVWriter
import scala.concurrent.Promise
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

object VIAFXML2CSV extends LazyLogging {
  
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)

  /** helper function to turn XML attrs back into text */
  def attrsToString(attrs:MetaData) = {
    attrs.length match {
      case 0 => ""
      case _ => attrs.map( (m:MetaData) => " " + m.key + "='" + m.value +"'" ).reduceLeft(_+_)
    }
  }
  
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
      case EvElemStart(_,"data",attrs,_) =>
        xml.next
        val value = readContents
        values.put(value,if (attrs("count") != null) attrs("count")(0).text else "1")
      case EvElemEnd(_,endTag) => break = true       
      case _ => 
    }
  }
  
  def readAlternate(endTag: String)(implicit xml: XMLEventReader): Option[String] = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,"subfield",attrs,_) if (attrs("code") != null) => attrs("code")(0).text match {
        case "e" | "9" => 
        case _ => 
          content.append(readContents)
          content.append(" ")
      }
      case EvElemStart(_,"subfield",_,_) =>
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
    implicit val xml = new XMLEventReader(Source.fromString(record.substring(record.indexOf("\t")+1)))
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
      case EvElemStart(_,"viafID",_,_) => id = readContents
      case EvElemStart(_,"nameType",_,_) => nameType = readContents
      case EvElemStart(_,"mainHeadings",_,_) =>
        var break = false
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"text",_,_) => prefLabels.add(readContents)
          case EvElemEnd(_,"mainHeadings") => break = true       
          case _ => 
        }
      case EvElemStart(_,"gender",_,_) => gender = readContents
      case EvElemStart(_,"birthDate",_,_) => birthDate = readContents
      case EvElemStart(_,"deathDate",_,_) => deathDate = readContents
      case EvElemStart(_,"x400",_,_) => readAlternate("x400").foreach(altLabels.add(_)) 
      case EvElemStart(_,"x500",_,_) => readAlternate("x500").foreach(relLabels.add(_))
      case EvElemStart(_,"nationalityOfEntity",_,_) => readAggregate("nationalityOfEntity", nationalities)  
      case EvElemStart(_,"countries",_,_) => readAggregate("countries", countries)
      case EvElemStart(_,"RelatorCodes",_,_) => readAggregate("RelatorCodes", relatorCodes)
      case _ => 
    }
    output.synchronized { output.write(Seq(id,nameType,birthDate,deathDate,gender,countries.map(p => p._1+":"+p._2).mkString(";"),nationalities.map(p => p._1+":"+p._2).mkString(";"),relatorCodes.map(p => p._1+":"+p._2).mkString(";"),prefLabels.map(_.replace(";","\\;")).mkString(";"),altLabels.map(_.replace(";","\\;")).mkString(";"),relLabels.map(_.replace(";","\\;")).mkString(";"))) }
    println("done")
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
    implicit val output: CSVWriter = CSVWriter("output.csv")    
    output.write(Seq("id","nameType","birthDate","deathDate","gender","countries","nationalities","relatorCodes","prefLabels","altLabels","relLabels"))
    val f = Future.sequence(for (record <- s.getLines) yield process(record))
    f.onFailure { case t => logger.error("Processing of at least one linr resulted in an error:" + t.getMessage+": " + t.printStackTrace) }
    f.onSuccess { case _ => logger.info("Successfully processed all lines.") }
    Await.result(f, Duration.Inf)
    output.close()
  }
}
