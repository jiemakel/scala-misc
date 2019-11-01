import java.io.{BufferedReader, File, FileReader, FileWriter}

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object CEEC2NormalizationDictionary extends LazyLogging {
  
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)

  def process(file: File): Future[mutable.HashMap[String,mutable.HashSet[String]]] = Future {
    logger.info(s"Processing: ${file}")
    var fis = new BufferedReader(new FileReader(file))
    var line = fis.readLine()
    val matcher = "<normalised orig=\"(.*?)\".*?>(.*?)</normalised>".r
    val map = new mutable.HashMap[String,mutable.HashSet[String]]
    while (line != null) {
      for (m <- matcher.findAllMatchIn(line)) {
        map.getOrElseUpdate(m.group(1),new mutable.HashSet[String]) += m.group(2)
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
      f.onComplete {
        case Failure(t) => logger.error("An error has occured processing "+file+": " + t.printStackTrace())
        case Success(_) => logger.info("Processed: "+file)
      }
      f.recover { case cause => throw new Exception("An error has occured processing "+file, cause) }
    })
    val map = new mutable.HashMap[String,mutable.HashSet[String]]
    for (f <- Await.result(f, Duration.Inf); (key,value) <- f)
      map.getOrElseUpdate(key, new mutable.HashSet[String]) ++= value
    val out = new FileWriter("normalized.csv")
    for ((key,value) <- map) out.append(s"${key},${value.mkString(";")}\n")
    out.close()
  }
}
