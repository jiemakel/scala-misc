import java.io.{File, FileWriter, PrintWriter, StringWriter}

import com.brein.time.timeintervals.collections.ListIntervalCollection
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder.IntervalType
import com.brein.time.timeintervals.indexes.{IntervalTree, IntervalTreeBuilder}
import com.brein.time.timeintervals.intervals.IntegerInterval
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._

object DetectColumnsInALTO extends LazyLogging {

  def decodeEntity(entity: String): String = {
    XhtmlEntities.entMap.get(entity) match {
      case Some(chr) => chr.toString
      case None =>
        println("Encountered unknown entity "+entity)
        '〈' + entity + '〉'
    }
  }

  def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_,_) => return null
      case EvText(text) => content.append(text)
      case er: EvEntityRef => content.append(decodeEntity(er.entity))
      case EvComment(comment) if comment == " unknown entity apos; " => content.append('\'')
      case EvComment(comment) if comment.startsWith(" unknown entity") =>
        val entity = comment.substring(16, comment.length - 2)
        content.append(decodeEntity(entity))
      case EvComment(comment) =>
        println("Encountered comment: "+comment)
      case EvElemEnd(_,_) => break = true
    }
    content.toString
  }

  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)
      
  case class Block(hpos1: Int, vpos1: Int, hpos2: Int, vpos2: Int) extends IntegerInterval(vpos1, vpos2) {
  }

  case class Style(font: String, size: Int, bold: Boolean, italics: Boolean, underlined: Boolean) {
    var chars = 0l
    var words = 0l
  }

  var pw1: PrintWriter = _
  var pw2: PrintWriter = _
  var pw3: PrintWriter = _
  var pw4: PrintWriter = _
  var pw5: PrintWriter = _
  var pw6: PrintWriter = _

  private val treeBuilder = IntervalTreeBuilder.newBuilder()
    .usePredefinedType(IntervalType.INTEGER)
    .collectIntervals(_ => new ListIntervalCollection())

  private def tob(b: Boolean): Char = b match {
    case true => '1'
    case false => '0'
  }

  def process(altoFile: File): Future[Unit] = Future {
    val s = Source.fromFile(altoFile, "UTF-8")
    implicit val xml = new XMLEventReader(s)
    val textBlocks: IntervalTree = treeBuilder.build()
    var issn: String = "?"
    var binding: String = "?"
    var date: String = "?"
    var words = 0l
    var characters = 0l
    val page: Int = Try(Integer.parseInt(altoFile.getName.substring(altoFile.getName.lastIndexOf('_') + 1, altoFile.getName.length - 4))).getOrElse(0)
    val styleMap = new mutable.HashMap[String,Style]
    var bstyle: Style = null
    var lstyle: Style = null
    var sstyle: Style = null
    while (xml.hasNext) xml.next match {
      case EvElemStart(_, "identifier", _, _) => issn = readContents
      case EvElemStart(_, "bindingIdentifier", _, _) => binding = readContents
      case EvElemStart(_, "published", _, _) => date = readContents
      case EvElemStart(_, "TextStyle", attrs, _) => try {
        val id = attrs("ID").head.text
        val size = attrs("FONTSIZE").head.text.toInt
        val font = attrs("FONTFAMILY").head.text
        val styles: Set[String] = Option(attrs("FONTSTYLE")).map(s => immutable.HashSet(s.head.text.split(" "):_*)).getOrElse(Set.empty[String])
        styleMap.put(id, Style(font,size,styles.contains("bold"),styles.contains("italics"),styles.contains("underline")))
      } catch {
        case e: Exception => logger.warn("Exception processing style " + attrs, e)
      }
      case EvElemStart(_, "PrintSpace", attrs, _) => try {
        val height = attrs("HEIGHT").head.text.toInt
        val width = attrs("WIDTH").head.text.toInt
        pw6.synchronized {
          pw6.println(issn + "," + binding + "," + date + "," + page + "," + width + "," + height)
        }
      } catch {
        case e: Exception => logger.warn("Exception processing page attrs " + attrs, e)
      }
      case EvElemStart(_, "Page", attrs, _) => try {
        val height = attrs("HEIGHT").head.text.toInt
        val width = attrs("WIDTH").head.text.toInt
        pw5.synchronized {
          pw5.println(issn + "," + binding + "," + date + "," + page + "," + width + "," + height)
        }
      } catch {
        case e: Exception => logger.warn("Exception processing printspace attrs " + attrs, e)
      }
      case EvElemStart(_, "TextBlock", attrs, _) => try {
        val hpos = Integer.parseInt(attrs("HPOS").head.text)
        val vpos = Integer.parseInt(attrs("VPOS").head.text)
        val width = Integer.parseInt(attrs("WIDTH").head.text)
        val height = Integer.parseInt(attrs("HEIGHT").head.text)
        val block = Block(hpos, vpos, hpos + width, vpos + height)
        bstyle = null
        for (styles <- Option(attrs("STYLEREFS"));
             style <- styles.head.text.split(" ")) if (styleMap.contains(style)) bstyle = styleMap(style)
        if (width >= 0 && height >= 0)
          textBlocks.insert(block)
      } catch {
        case e: Exception => logger.warn("Exception processing text block " + attrs, e)
      }
      case EvElemStart(_, "TextLine", attrs, _) => try {
        lstyle = null
        for (styles <- Option(attrs("STYLEREFS"));
             style <- styles.head.text.split(" ")) if (styleMap.contains(style)) lstyle = styleMap(style)
      } catch {
        case e: Exception => logger.warn("Exception processing textline attrs " + attrs, e)
      }
      case EvElemStart(_, "String", attrs, _) =>
        val cchars = attrs("CONTENT").head.text.length
        characters += cchars
        words += 1
        sstyle = null
        for (styles <- Option(attrs("STYLEREFS"));
             style <- styles.head.text.split(" ")) if (styleMap.contains(style)) sstyle = styleMap(style)
        val cstyle = if (sstyle != null) sstyle else if (lstyle != null) lstyle else bstyle
        if (cstyle!=null) {
          cstyle.words += 1
          cstyle.chars += cchars
        }
      case _ =>
    }
    s.close()
    pw1.synchronized {
      pw1.println(issn + "," + binding + "," + date + "," + page + "," + words + "," + characters)
    }
    pw4.synchronized {
      for (style <- styleMap.valuesIterator)
        pw4.println(issn + "," + binding + "," + date + "," + page + "," + style.font + "," + style.size + "," + tob(style.bold) + "," + tob(style.italics) + "," + tob(style.underlined) + "," + style.words + "," + style.chars)
    }
    val cols: mutable.Buffer[(Int, Long)] = new ArrayBuffer[(Int, Long)]
    val rowIntervals: IntervalTree = treeBuilder.build()
    val seen: mutable.HashSet[IntegerInterval] = new mutable.HashSet[IntegerInterval]
    var sum: Long = 0l
    for (ablock <- textBlocks.asScala.asInstanceOf[Iterable[Block]]) {
      rowIntervals.clear()
      seen.clear()
      val blocks = textBlocks.overlap(ablock).asScala.asInstanceOf[Iterable[Block]]
      for (block <- blocks) rowIntervals.insert(new IntegerInterval(block.hpos1, block.hpos2))
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
    var wmedianindex = -1
    while (csum < sum) {
      wmedianindex += 1
      csum += scols(wmedianindex)._2
    }
    val modeMap = new mutable.HashMap[Int, (Int, Long)]
    for ((col, area) <- scols) {
      val (oldFreq,oldArea) = modeMap.getOrElse(col, (0, 0l))
      modeMap.put(col, (oldFreq + 1, oldArea + area))
    }
    var mode1,mode2,mode1Weight = 0
    var mode2Weight = 0l
    for ((col,(count,area)) <- modeMap) {
      if (mode1Weight<count) {
        mode1=col
        mode1Weight=count
      }
      if (mode2Weight<area) {
        mode2=col
        mode2Weight = area
      }
    }
    pw2.synchronized {
      if (scols.isEmpty) pw2.println(issn+","+binding+","+date+","+page+",0,0,0,0")
      else pw2.println(issn+","+binding+","+date+","+page+","+mode2+","+mode1+","+scols(wmedianindex)._1+","+scols(scols.length / 2)._1) // area weighted mode, mode of all blocks, area weighted median, median of all blocks
    }
  }

  def processMETS(f: File): Future[Unit] = Future {
    val s = Source.fromFile(f)
    val imgsizes = new mutable.HashMap[String,(Double,Double)]
    implicit val xml = new XMLEventReader(s)
    var issn = "?"
    var binding = "?"
    var date = "?"
    var page = 1
    while (xml.hasNext) xml.next match {
      case EvElemStart(_, "dmdSec",attrs,_) if attrs("ID").head.text.startsWith("MODSMD_ISSUE") =>
        var break = false
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_, "identifier",attrs,_) => if (attrs("type") != null) attrs("type").head.text.toLowerCase match {
            case "issn" => issn = readContents
            case "binding" => binding = readContents
            case _ =>
          }
          case EvElemStart(_, "dateIssued",_,_) => date = readContents
          case EvElemEnd(_, "dmdSec") => break = true
          case _ =>
        }
      case EvElemStart(_, "amdSec",attrs,_) =>
        val imgId = attrs("ID").head.text.replaceFirst("PARAM","")
        var pmultiplier = -1.0
        var xs = -1
        var ys = -1
        var break = false
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"PixelSize",_,_) =>
            pmultiplier = readContents.toFloat
          case EvElemStart(_,"ImageWidth",_,_) =>
            xs = readContents.toInt
          case EvElemStart(_,"ImageLength",_,_) =>
            ys = readContents.toInt
          case EvElemEnd(_, "amdSec") => break = true
          case _ =>
        }
        imgsizes.put(imgId,(xs*pmultiplier,ys*pmultiplier))
      case EvElemStart(_, "file",attrs,_) if attrs("ID").head.text.startsWith("IMG") =>
        val dims = imgsizes(attrs("ID").head.text)
        pw3.synchronized {
          pw3.println(issn + "," + binding + "," + date + "," + page + "," + dims._1 + "," + dims._2)
        }
        page += 1
      case _ =>
    }
    s.close()
  }

  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  
  def main(args: Array[String]): Unit = {
    val prefix = args(args.length-1)
    pw1 = new PrintWriter(new FileWriter(prefix+"npwordschars.csv"))
    pw1.println("ISSN,issueId,date,page,words,chars")
    pw2 = new PrintWriter(new FileWriter(prefix+"npcolumns.csv"))
    pw2.println("ISSN,issueId,date,page,wmodecols,modecols,wmediancols,mediancols")
    pw3 = new PrintWriter(new FileWriter(prefix+"npsizes1.csv"))
    pw3.println("ISSN,issueId,date,page,width,height")
    pw4 = new PrintWriter(new FileWriter(prefix+"npstyles.csv"))
    pw4.println("ISSN,issueId,date,page,font,size,bold,italics,underlined,words,chars")
    pw5 = new PrintWriter(new FileWriter(prefix+"npsizes2.csv"))
    pw5.println("ISSN,issueId,date,page,width,height")
    pw6 = new PrintWriter(new FileWriter(prefix+"npsizes3.csv"))
    pw6.println("ISSN,issueId,date,page,width,height")
    val toProcess: Seq[File] = for (
        dir<-args.dropRight(1);
        fd=new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!")
    ) yield fd
    val f = Future.sequence(toProcess.toStream.flatMap(fd => {
      getFileTree(fd)
        .filter(altoFile => altoFile.getName.endsWith(".xml") && !altoFile.getName.endsWith("metadata.xml") && !altoFile.getName.endsWith("mets.xml"))
        .map(altoFile => process(altoFile).map[Try[Unit]](Success(_)).recover{
          case cause =>
            logger.error("An error has occured processing "+altoFile+": " + getStackTraceAsString(cause))
            Failure(new Exception("An error has occured processing "+altoFile, cause))
        })
    }))
    val f2 = Future.sequence(toProcess.toStream.flatMap(fd => {
      getFileTree(fd)
        .filter(_.getName == "mets.xml")
        .map(metsFile => processMETS(metsFile).map[Try[Unit]](Success(_)).recover{
            case cause =>
              logger.error("An error has occured processing "+metsFile+": " + getStackTraceAsString(cause))
              Failure(new Exception("An error has occured processing "+metsFile, cause))
        })
    }))
    var results = Await.result(f, Duration.Inf)
    if (results.exists(_.isFailure)) logger.error("Processing of at least one ALTO file resulted in an error.")
    else logger.info("Successfully processed all ALTO files.")
    pw1.close()
    pw2.close()
    results = Await.result(f2, Duration.Inf)
    if (results.exists(_.isFailure)) logger.error("Processing of at least one METS file resulted in an error.")
    else logger.info("Successfully processed all METS files.")
    pw3.close()
  }
}
