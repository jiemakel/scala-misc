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
import java.io.FileInputStream
import java.net.URL
import scala.collection.JavaConverters._
import com.google.common.collect.TreeMultimap
import com.brein.time.timeintervals.indexes.IntervalTree
import com.brein.time.timeintervals.collections.ListIntervalCollection
import com.brein.time.timeintervals.intervals.Interval
import com.brein.time.timeintervals.collections.IntervalCollection.IntervalFilters


object DetectColumnsInALTO extends LazyLogging {

  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)
      
  case class Block(hpos1: Long, vpos1: Long, hpos2: Long, vpos2: Long) extends Interval(vpos1, vpos2) {
  }
  
  def process(altoFile: File): Future[Unit] = Future {
    val s = Source.fromFile(altoFile,"UTF-8")
    val xml = new XMLEventReader(s)
    val textBlocks: IntervalTree = new IntervalTree()
    var url: String = ""
    var title: String = ""
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"title",_,_) => title = xml.next.asInstanceOf[EvText].text
      case EvElemStart(_,"browseURL",_,_) => url = xml.next.asInstanceOf[EvText].text
      case EvElemStart(_,"TextBlock",attrs,_) =>
        val hpos = Integer.parseInt(attrs("HPOS")(0).text)
        val vpos = Integer.parseInt(attrs("VPOS")(0).text)
        val width = Integer.parseInt(attrs("WIDTH")(0).text)
        val height = Integer.parseInt(attrs("HEIGHT")(0).text)
        val block = new Block(hpos, vpos, hpos+width, vpos+height)
        textBlocks.insert(block)
      case _ =>
    }
    val cols: Buffer[Int] = new ArrayBuffer[Int]
    val rowIntervals: IntervalTree = new IntervalTree()
    val seen: HashSet[Interval] = new HashSet[Interval]
    for (ablock <- textBlocks.asScala.asInstanceOf[Iterable[Block]]) {
      rowIntervals.clear()
      seen.clear()
      val blocks = textBlocks.overlap(ablock).asScala.asInstanceOf[Iterable[Block]]
      for (block <- blocks) rowIntervals.insert(new Interval(block.hpos1,block.hpos2))
      var ccols = 0
      for (interval <- rowIntervals.asScala; if seen.add(interval)) {
        ccols += 1
        for (ointerval <- rowIntervals.overlap(interval).asScala) seen += ointerval
      }
      cols += ccols
    }
    val scols = cols.sorted
    println(title, url, scols(scols.length / 2))
    s.close()
  }

  def main(args: Array[String]): Unit = {
    val f = Future.sequence(for (dir<-args.toSeq;altoFile <- getFileTree(new File(dir)); if (altoFile.getName.endsWith(".xml") && !altoFile.getName.endsWith("metadata.xml") && !altoFile.getName.endsWith("mets.xml"))) yield {
      val f = process(altoFile)
      /* f.onFailure { case t => logger.error("An error has occured processing "+altoFile+": " + t.printStackTrace()) }
      f.onSuccess { case _ => logger.info("Processed: "+altoFile) }*/
      f
    })
    f.onFailure { case t => logger.error("Processing of at least one file resulted in an error.") }
    f.onSuccess { case _ => logger.info("Successfully processed all files.") }
    Await.result(f, Duration.Inf)
  }
}
