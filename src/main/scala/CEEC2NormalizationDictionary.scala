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
import java.io.FileWriter

object CEEC2NormalizationDictionary extends LazyLogging {
  
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)

  def process(file: File): Future[HashMap[String,HashSet[String]]] = Future {
    logger.info(s"Processing: ${file}")
    var fis = new BufferedReader(new FileReader(file))
    var line = fis.readLine()
    val matcher = "<normalised orig=\"(.*?)\".*?>(.*?)</normalised>".r
    val map = new HashMap[String,HashSet[String]]
    while (line != null) {
      for (m <- matcher.findAllMatchIn(line)) {
        map.getOrElseUpdate(m.group(1),new HashSet[String]) += m.group(2)
        m.group(1)
      }
      line = fis.readLine()
    }
    fis.close()
    map   
  }

  def main(args: Array[String]): Unit = {
    val f = Future.sequence(for (dir<-args.toSeq;file <- getFileTree(new File(dir)); if (file.getAbsolutePath.contains("/tagged/"))) yield {
      val f = process(file)
      f.onFailure { case t => logger.error("An error has occured processing "+file+": " + t.printStackTrace()) }
      f.onSuccess { case _ => logger.info("Processed: "+file) }
      f.recover { case cause => throw new Exception("An error has occured processing "+file, cause) }
    })
    val map = new HashMap[String,HashSet[String]]
    for (f <- Await.result(f, Duration.Inf); (key,value) <- f)
      map.getOrElseUpdate(key, new HashSet[String]) ++= value
    val out = new FileWriter("normalized.csv")
    for ((key,value) <- map) out.append(s"${key},${value.mkString(";")}\n")
    out.close()
  }
}
