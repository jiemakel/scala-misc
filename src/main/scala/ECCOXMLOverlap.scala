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
import scala.concurrent.ExecutionContext.Implicits.global
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

object ECCOXMLOverlap extends LazyLogging {
  
  def process(file1: File, file2: File): Future[(String,Int,Int)] = Future {
    var matched = 0
    var unmatched = 0
    if (!file1.exists())
      logger.warn(s"$file1 doesn't exist!")
    else if (!file2.exists())
      logger.warn(s"$file2 doesn't exist!")
    else {
      logger.info(s"Processing: ${file1} vs ${file2}")
      var fis = new FileInputStream(file1)
      while (fis.read()!='\n') {}
      while (fis.read()!='\n') {}
      val originalFrequencyMap = new HashMap[String,Int]
      var xml = new XMLEventReader(Source.fromInputStream(fis,"ISO-8859-1"))
      val content = new StringBuilder()
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"sectionHeader",_,_) | EvElemStart(_,"wd",_,_) =>
          var break2 = false
          while (xml.hasNext && !break2) xml.next match {
            case EvText(text) => content.append(text)
            case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
              case Some(chr) => content.append(chr)
              case _ => content.append(er.entity)
            }
            case EvElemEnd(_,"sectionHeader") | EvElemEnd(_,"wd") => break2 = true 
          }
          content.split(' ').map(_.replaceAll("\\W","")).filter(!_.isEmpty()).foreach(s => originalFrequencyMap.put(s,originalFrequencyMap.get(s).getOrElse(0)+1))
          content.setLength(0)
        case _ => 
      }
      val tcpFrequencyMap = new HashMap[String,Int]
      fis = new FileInputStream(file2)
      xml = new XMLEventReader(Source.fromInputStream(fis,"ISO-8859-1"))
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"TEXT",_,_) =>
          while (xml.hasNext) xml.next match {
            case EvText(text) => content.append(text)
            case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
              case Some(chr) => content.append(chr)
              case _ => content.append(er.entity)
            }
            case EvElemEnd(_,_) => 
              content.split(' ').map(_.replaceAll("\\W","")).filter(!_.isEmpty()).foreach(s => tcpFrequencyMap.put(s,originalFrequencyMap.get(s).getOrElse(0)+1))
              content.setLength(0)
            case _ =>
          }
        case _ => 
      }
      tcpFrequencyMap.foreach{case (word,freq) => 
        val ocrfreq = originalFrequencyMap.get(word).getOrElse(0)
        matched += ocrfreq
        unmatched += freq - ocrfreq
        // if (freq!=ocrfreq) println(s"diff: $word, $freq vs $ocrfreq.") 
      }
    }
    (file1.getName,matched,matched+unmatched)
  }

  def main(args: Array[String]): Unit = {
    val pairs = Source.fromFile("ecco-tcp/ecco-file-matches.txt").getLines().map{line => 
      val pair = line.split(',')
      (new File(pair(0)),new File(pair(1)))
    }
    val f = Future.sequence(for ((file1,file2) <- pairs) yield {
      val f = process(file1,file2)
      f.onFailure { case t => logger.error("An error has occured processing "+file1+"/"+file2+": " + t.printStackTrace()) }
      f.onSuccess { case _ => logger.info("Processed: "+file1+"/"+file2) }
      f
    })
    f.onFailure { case t => logger.error("Processing of at least one file resulted in an error.") }
    f.onSuccess { case _ => logger.info("Successfully processed all files.") }
    var totalMatched = 0l
    var total = 0l
    for (f <- Await.result(f, Duration.Inf)) {
      totalMatched+=f._2
      total+=f._3
    }
    logger.info(s"Total recognition rate: ${100*totalMatched/total}% ($totalMatched/$total)") 
  }
}
