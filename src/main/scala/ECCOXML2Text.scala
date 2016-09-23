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
import scala.xml.pull.EvComment
import java.io.PushbackInputStream

object ECCOXML2Text extends LazyLogging {
  
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

  def process(file: File): Future[Unit] = Future {
    logger.info("Processing: "+file)
    val fis = new PushbackInputStream(new FileInputStream(file),2)
    var second = 0
    var first = 0
    do {
      first = fis.read()
      second = fis.read()
      if (first == '<' && (second=='?' || second=='!')) while (fis.read()!='\n') {}
    } while (first == '<' && (second=='?' || second=='!'))
    fis.unread(second)
    fis.unread(first)
    val xml = new XMLEventReader(Source.fromInputStream(fis,"ISO-8859-1"))
    var currentSection: String = null
    val content = new StringBuilder()
    val dir = file.getAbsoluteFile.getParent+"/extracted/"
    new File(dir).mkdir()
    val prefix = dir+file.getName.replace(".xml","")+"_"
    var page = 1
    var sw: PrintWriter = null
    var indent = ""
    var metadata = new StringBuilder()
    var break = false
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"text",_,_) =>
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"page",attrs,_) =>
            if (currentSection!=attrs("type")(0).text) {
              if (sw != null) sw.close()
              currentSection = attrs("type")(0).text
              sw = new PrintWriter(new File(prefix+currentSection.replaceAllLiterally("bodyPage","body")+".txt"))
            }
          case EvElemStart(_,"sectionHeader",_,_) =>
            content.append("# ")
            var break2 = false
            while (xml.hasNext && !break2) xml.next match {
              case EvText(text) => content.append(text)
              case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
                case Some(chr) => content.append(chr)
                case _ => content.append(er.entity)
              }
              case EvComment(_) => 
              case EvElemEnd(_,"sectionHeader") => break2 = true 
            }
            content.append("\n\n")
          case EvElemStart(_,"wd",_,_) =>
            var break2 = false
            while (xml.hasNext && !break2) xml.next match {
              case EvText(text) => content.append(text)
              case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
                case Some(chr) => content.append(chr)
                case _ => content.append(er.entity)
              }
              case EvComment(_) => 
              case EvElemEnd(_,"wd") => break2 = true 
            }
            content.append(' ')
          case EvElemEnd(_,"p") => 
            content.append("\n\n")
          case EvElemEnd(_,"page") =>
            val pw = new PrintWriter(new File(prefix+"page"+page+".txt"))
            pw.append(content)
            pw.close()
            sw.append(content)
            content.setLength(0)
            page+=1
          case EvElemEnd(_,"text") => break = true
          case _ =>
        }
      case EvElemStart(pre, label, attrs, scope) =>
        metadata.append(indent + "<" + label + attrsToString(attrs) + ">\n")
        indent += "  "
      case EvText(text) => if (text.trim!="") metadata.append(indent + text.trim + "\n")
      case er: EvEntityRef => metadata.append('&'); metadata.append(er.entity); metadata.append(';') /* XhtmlEntities.entMap.get(er.entity) match {
        case Some(chr) => metadata.append(chr)
        case _ => metadata.append(er.entity)
      }*/
      case EvElemEnd(_, label) =>
        indent = indent.substring(0,indent.length-2)
        metadata.append(indent + "</"+label+">\n")
      case EvComment(_) => 
    }
    if (sw!=null) sw.close()
    sw = new PrintWriter(new File(prefix+"metadata.xml"))
    sw.append(metadata)
    sw.close()
  }

  def main(args: Array[String]): Unit = {
    val f = Future.sequence(for (dir<-args.toSeq;file <- getFileTree(new File(dir)); if (file.getName().endsWith(".xml") && !file.getName().startsWith("ECCO_tiff_manifest_") && !file.getName().endsWith("_metadata.xml"))) yield {
      val f = process(file)
      f.onFailure { case t => logger.error("An error has occured processing "+file+": " + t.printStackTrace) }
      f.onSuccess { case _ => logger.info("Processed: "+file) }
      f.recover { case cause => throw new Exception("An error has occured processing "+file, cause) }
    })
    f.onFailure { case t => logger.error("Processing of at least one file resulted in an error:" + t.getMessage+": " + t.printStackTrace) }
    f.onSuccess { case _ => logger.info("Successfully processed all files.") }
    Await.result(f, Duration.Inf)
  }
}
