import java.io.{File, FileWriter, PrintWriter, StringWriter}

import com.brein.time.timeintervals.indexes.IntervalTree
import com.brein.time.timeintervals.intervals.IntegerInterval
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.Try
import scala.xml.pull.{EvElemStart, EvText, XMLEventReader}

object DetectColumnsInALTO extends LazyLogging {

  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)
      
  case class Block(hpos1: Int, vpos1: Int, hpos2: Int, vpos2: Int) extends IntegerInterval(vpos1, vpos2) {
  }
  
  var pw: PrintWriter = _
  
  def process(altoFile: File): Future[Unit] = Future {
    val s = Source.fromFile(altoFile,"UTF-8")
    val xml = new XMLEventReader(s)
    val textBlocks: IntervalTree = new IntervalTree()
    var issn: String = ""
    var date: String = ""
    val page: Int = Try(Integer.parseInt(altoFile.getName.substring(altoFile.getName.lastIndexOf('_')+1,altoFile.getName.length-4))).getOrElse(0)
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"identifier",_,_) => issn = xml.next.asInstanceOf[EvText].text
      case EvElemStart(_,"published",_,_) => date = xml.next.asInstanceOf[EvText].text
      case EvElemStart(_,"TextBlock",attrs,_) => try {
        val hpos = Integer.parseInt(attrs("HPOS").head.text)
        val vpos = Integer.parseInt(attrs("VPOS").head.text)
        val width = Integer.parseInt(attrs("WIDTH").head.text)
        val height = Integer.parseInt(attrs("HEIGHT").head.text)
        val block = Block(hpos, vpos, hpos+width, vpos+height)
        if (width>=0 && height>=0) textBlocks.insert(block)
      } catch {
        case e: Exception => logger.warn("Exception processing text block "+attrs,e)
      }
      case _ =>
    }
    val cols: mutable.Buffer[(Int,Long)] = new ArrayBuffer[(Int,Long)]
    val rowIntervals: IntervalTree = new IntervalTree()
    val seen: mutable.HashSet[IntegerInterval] = new mutable.HashSet[IntegerInterval]
    var sum: Long = 0l
    for (ablock <- textBlocks.asScala.asInstanceOf[Iterable[Block]]) {
      rowIntervals.clear()
      seen.clear()
      val blocks = textBlocks.overlap(ablock).asScala.asInstanceOf[Iterable[Block]]
      for (block <- blocks) rowIntervals.insert(new IntegerInterval(block.hpos1,block.hpos2))
      var ccols = 0
      for (interval <- rowIntervals.asScala; if seen.add(interval.asInstanceOf[IntegerInterval])) {
        ccols += 1
        for (ointerval <- rowIntervals.overlap(interval).asScala) seen += ointerval.asInstanceOf[IntegerInterval]
      }
      val area = (ablock.hpos2 - ablock.hpos1) * (ablock.vpos2 - ablock.vpos1)
      sum += area
      cols += ccols -> area
    }
    val scols = cols.sorted
    sum = sum / 2
    var csum: Long = 0l
    var i = -1
    while (csum < sum) {
      i += 1
      csum += scols(i)._2
    }
    synchronized {
      if (scols.isEmpty) pw.println(issn+","+date+","+page+",0,0")
      else pw.println(issn+","+date+","+page+","+scols(i)._1+","+scols(scols.length / 2)._1)
    }
    s.close()
  }

  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  
  def main(args: Array[String]): Unit = {
    pw = new PrintWriter(new FileWriter(args(args.length-1)))
    val toProcess = for (
        dir<-args.dropRight(1).toStream;
        fd=new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!")
    ) yield fd
    val f = Future.sequence(toProcess.flatMap(fd => {
      getFileTree(fd)
        .filter(altoFile => altoFile.getName.endsWith(".xml") && !altoFile.getName.endsWith("metadata.xml") && !altoFile.getName.endsWith("mets.xml"))
        .map(altoFile => {
          val f = process(altoFile)
          f.recover { 
            case cause =>
              logger.error("An error has occured processing "+altoFile+": " + getStackTraceAsString(cause))
              throw new Exception("An error has occured processing "+altoFile, cause) 
          }
        })
    }))
    f.onFailure { case _ => logger.error("Processing of at least one file resulted in an error.") }
    f.onSuccess { case _ => logger.info("Successfully processed all files.") }
    Await.ready(f, Duration.Inf)
    pw.close()
  }
}
