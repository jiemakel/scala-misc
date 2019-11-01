
import java.io.{File, FileInputStream, PrintWriter, PushbackInputStream}

import XMLEventReaderSupport._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.xml.MetaData

object ECCOXML2PassimJSON extends LazyLogging {
  
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

  def process(file: File, series: String): Future[Unit] = Future {
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
    val xml = getXMLEventReader(fis,"ISO-8859-1")
    val dir = file.getAbsoluteFile.getParent+"/extracted/"
    new File(dir).mkdir()
    val prefix = dir+file.getName.replaceAllLiterally(".xml","")
    var page = 1
    val sw = new PrintWriter(new File(prefix+".json"))
    sw.append("{\"id\":\""+file.getName+"\",\"series\":\""+(if (series!="-") series else file.getName)+"\"")
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"pubDate",_) =>
        val date = xml.next.asInstanceOf[EvText].text
        if (date.length==8)
          sw.append(",\"date\":\""+date.substring(0,4)+"-"+date.substring(4,6)+"-"+date.substring(6)+"\"")
        else if (date.length==6)
          sw.append(",\"date\":\""+date.substring(0,4)+"-"+date.substring(4)+"-01\"")
        else if (date.length==4)
          sw.append(",\"date\":\""+date+"-01-01\"")
        else sw.append(",\"date\":\"1970-01-01\"")
      case EvElemStart(_,"text",_) =>
        sw.append(",\"text\":\"")
        while (xml.hasNext) xml.next match {
          case EvElemStart(_,"page",_) =>
            sw.append("<pb n=\\\""+page+"\\\" />")
          case EvElemStart(_,"sectionHeader",_) =>
            //sw.append("<loc n=\\\"")
            var break2 = false
            val content = new StringBuilder()
            while (xml.hasNext && !break2) xml.next match {
              case EvText(text) => content.append(text)
              case er: EvEntityRef =>
                content.append('&')
                content.append(er.entity)
                content.append(';')
              case EvComment(_) => 
              case EvElemEnd(_,"sectionHeader") => break2 = true 
            }
            //sw.append(content.replaceAllLiterally("\\","\\\\").replaceAllLiterally("\"","\\\""))
            //sw.append("\\\" />")
            sw.append(content.toString.replaceAllLiterally("\\","\\\\").replaceAllLiterally("\"","\\\"")+"\\n\\n")
          case EvElemStart(_,"wd",_) =>
            var break2 = false
            while (xml.hasNext && !break2) xml.next match {
              case EvText(text) => sw.append(text.replaceAllLiterally("\\","\\\\").replaceAllLiterally("\"","\\\""))
              case er: EvEntityRef =>
                sw.append('&')
                sw.append(er.entity)
                sw.append(';')
              case EvComment(_) => 
              case EvElemEnd(_,"wd") => break2 = true 
            }
            sw.append(' ')
          case EvElemEnd(_,"p") => 
            sw.append("\\n\\n")
          case EvElemEnd(_,"page") =>
            page+=1
          case _ =>
        }
      case _ =>
    }
    sw.append("\"}")
    sw.close()
  }

  def main(args: Array[String]): Unit = {
    val series = args(0)
    val f = Future.sequence(for (dir<-args.toSeq.drop(1);file <- getFileTree(new File(dir)); if (file.getName().endsWith(".xml") && !file.getName().startsWith("ECCO_tiff_manifest_") && !file.getName().endsWith("_metadata.xml"))) yield {
      val f = process(file,series)
      f.onComplete {
        case Failure(t) => logger.error("An error has occured processing "+file+": " + t.printStackTrace())
        case Success(_) => logger.info("Processed: "+file)
      }
      f.recover { case cause => throw new Exception("An error has occured processing "+file, cause) }
    })
    f.onComplete {
      case Failure(t) => logger.error("Processing of at least one file resulted in an error:" + t.getMessage+": " + t.printStackTrace)
      case Success(_) => logger.info("Successfully processed all files.")
    }
    Await.result(f, Duration.Inf)
  }
}
