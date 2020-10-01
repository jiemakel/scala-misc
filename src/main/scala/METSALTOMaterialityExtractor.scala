import java.io.{File, FileInputStream, FileWriter, PrintWriter, StringWriter}

import com.brein.time.timeintervals.collections.ListIntervalCollection
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder.IntervalType
import com.brein.time.timeintervals.indexes.{IntervalTree, IntervalTreeBuilder}
import com.brein.time.timeintervals.intervals.IntegerInterval
import com.typesafe.scalalogging.LazyLogging
import javax.xml.stream.XMLEventReader

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.xml.parsing.XhtmlEntities
import XMLEventReaderSupport._

object METSALTOMaterialityExtractor extends LazyLogging {

  def decodeEntity(entity: String): String = {
    XhtmlEntities.entMap.get(entity) match {
      case Some(chr) => chr.toString
      case None =>
        println("Encountered unknown entity "+entity)
        '〈' + entity + '〉'
    }
  }

  def readContents(implicit xml: Iterator[EvEvent]): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_) => return null
      case EvText(text,_)  => content.append(text)
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
    var area = 0l
  }

  var pw1: PrintWriter = _
  var pw2: PrintWriter = _
  var pw3: PrintWriter = _
  var pw4: PrintWriter = _
  var pw5: PrintWriter = _
  var pw6: PrintWriter = _
  var pw7: PrintWriter = _
  var pw8: PrintWriter = _
  var pw9: PrintWriter = _
  var pw10: PrintWriter = _
  var pw11: PrintWriter = _
  var pw12: PrintWriter = _

  private val treeBuilder = IntervalTreeBuilder.newBuilder()
    .usePredefinedType(IntervalType.INTEGER)
    .collectIntervals(_ => new ListIntervalCollection())

  private def tob(b: Boolean): Char = if (b) '1' else '0'

  def process(altoFile: File): Future[Unit] = Future {
    val s = new FileInputStream(altoFile)
    implicit val xml = getXMLEventReader(s)
    val textBlocks: IntervalTree = treeBuilder.build()
    var issn: String = "?"
    var date: String = "?"
    var lastModified: String = "?"
    var binding: String = "?"
    var words = 0l
    var characters = 0l
    val page: Int = Try(Integer.parseInt(altoFile.getName.substring(altoFile.getName.lastIndexOf('_') + 1, altoFile.getName.length - 4))).getOrElse(0)
    val styleMap = new mutable.HashMap[String,Style]
    var bstyle: Style = null
    var lstyle: Style = null
    var sstyle: Style = null
    var preSoftware: String = "?"
    var preVersion: String = "?"
    var ocrSoftware: String = "?"
    var ocrVersion: String = "?"
    var curBlock: Block = null
    var curBlockWords = 0l
    var curBlockChars = 0l
    while (xml.hasNext) xml.next match {
      case EvElemStart(_, "identifier", _) => issn = readContents
      case EvElemStart(_, "bindingIdentifier", _) => binding = readContents
      case EvElemStart(_, "published", _) => date = readContents
      case EvElemStart(_, "lastModified", _) => lastModified = readContents
      case EvElemStart(_, "preProcessingStep",_) =>
        var break = false
        preSoftware = "?"
        preVersion = "?"
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_, "softwareName",_) => preSoftware = readContents
          case EvElemStart(_, "softwareVersion",_) => preVersion = readContents
          case EvElemEnd(_, "preProcessingStep") => break = true
          case _ =>
        }
      case EvElemStart(_, "ocrProcessingStep",_) =>
        var break = false
        ocrSoftware = "?"
        ocrVersion = "?"
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_, "softwareName",_) => ocrSoftware = readContents
          case EvElemStart(_, "softwareVersion",_) => ocrVersion = readContents
          case EvElemEnd(_, "ocrProcessingStep") => break = true
          case _ =>
        }
      case EvElemStart(_, "TextStyle", attrs) => try {
        val id = attrs("ID")
        val size = attrs("FONTSIZE").toInt
        val font = attrs("FONTFAMILY")
        val styles: Set[String] = Option(attrs("FONTSTYLE")).map(s => immutable.HashSet(s.split(" "):_*)).getOrElse(Set.empty[String])
        styleMap.put(id, Style(font,size,styles.contains("bold"),styles.contains("italics"),styles.contains("underline")))
      } catch {
        case e: Exception => logger.warn("Exception processing style " + attrs, e)
      }
      case EvElemStart(_, "PrintSpace", attrs) => try {
        val height = attrs("HEIGHT").toInt
        val width = attrs("WIDTH").toInt
        pw6.synchronized {
          pw6.println(binding + "," + page + "," + width + "," + height)
        }
      } catch {
        case e: Exception => logger.warn("Exception processing page attrs " + attrs, e)
      }
      case EvElemStart(_, "Page", attrs) => try {
        val height = attrs("HEIGHT").toInt
        val width = attrs("WIDTH").toInt
        pw5.synchronized {
          pw5.println(binding  + "," + page + "," + width + "," + height)
        }
      } catch {
        case e: Exception => logger.warn("Exception processing printspace attrs " + attrs, e)
      }
      case EvElemStart(_, "TextBlock", attrs) => try {
        val hpos = Integer.parseInt(attrs("HPOS"))
        val vpos = Integer.parseInt(attrs("VPOS"))
        val width = Integer.parseInt(attrs("WIDTH"))
        val height = Integer.parseInt(attrs("HEIGHT"))
        curBlock = Block(hpos, vpos, hpos + width, vpos + height)
        curBlockWords = 0l
        curBlockChars = 0l
        bstyle = null
        for (styles <- attrs.get("STYLEREFS");
             style <- styles.split(' ')) if (styleMap.contains(style)) bstyle = styleMap(style)
        if (width >= 0 && height >= 0) {
          if (bstyle != null) bstyle.area += width * height
          textBlocks.insert(curBlock)
        }
      } catch {
        case e: Exception => logger.warn("Exception processing text block " + attrs, e)
      }
      case EvElemEnd(_, "TextBlock") =>
        pw8.synchronized {
          pw8.println(binding + "," + page + "," + curBlock.hpos1 + "," + curBlock.vpos1 + "," + curBlock.hpos2 + "," + curBlock.vpos2 + "," + curBlockWords + "," + curBlockChars)
        }
      case EvElemStart(_, "TextLine", attrs) => try {
        lstyle = null
        for (styles <- attrs.get("STYLEREFS");
             style <- styles.split(" ")) if (styleMap.contains(style)) lstyle = styleMap(style)
        if (lstyle != null) {
          val width = Integer.parseInt(attrs("WIDTH"))
          val height = Integer.parseInt(attrs("HEIGHT"))
          lstyle.area += width * height
          if (bstyle != null) bstyle.area -= width * height
        }
      } catch {
        case e: Exception => logger.warn("Exception processing textline attrs " + attrs, e)
      }
      case EvElemStart(_, "String", attrs) =>
        val cchars = attrs("CONTENT").length
        characters += cchars
        curBlockChars += cchars
        words += 1
        curBlockWords += 1
        sstyle = null
        for (styles <- attrs.get("STYLEREFS");
             style <- styles.split(" ")) if (styleMap.contains(style)) sstyle = styleMap(style)
        val cstyle = if (sstyle != null) sstyle else if (lstyle != null) lstyle else bstyle
        if (cstyle!=null) {
          val width = Integer.parseInt(attrs("WIDTH"))
          val height = Integer.parseInt(attrs("HEIGHT"))
          cstyle.area += width * height
          if (lstyle != null) lstyle.area -= width * height
          if (bstyle != null) bstyle.area -= width * height
          cstyle.words += 1
          cstyle.chars += cchars
        }
      case _ =>
    }
    s.close()
    pw9.synchronized {
      pw9.println(binding  + "," + issn + "," + date)
    }
    pw7.synchronized {
      pw7.println(binding  + "," + page + "," + lastModified + "," + preSoftware + "," + preVersion + "," + ocrSoftware + "," + ocrVersion)
    }
    pw1.synchronized {
      pw1.println(binding  + "," + page + "," + words + "," + characters)
    }
    pw4.synchronized {
      for (style <- styleMap.valuesIterator)
        pw4.println(binding  + "," + page + "," + style.font + "," + style.size + "," + tob(style.bold) + "," + tob(style.italics) + "," + tob(style.underlined) + "," + style.words + "," + style.chars + "," + style.area)
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
      if (scols.isEmpty) pw2.println(binding +","+page+",0,0,0,0")
      else pw2.println(binding +","+page+","+mode2+","+mode1+","+scols(wmedianindex)._1+","+scols(scols.length / 2)._1) // area weighted mode, mode of all blocks, area weighted median, median of all blocks
    }
  }

  def processMETS(f: File): Future[Unit] = Future {
    val s = new FileInputStream(f)
    val imgsizes = new mutable.HashMap[String,(Double,Double)]
    implicit val xml = getXMLEventReader(s)
    var issn = "?"
    var binding = "?"
    var date = "?"
    while (xml.hasNext) xml.next match {
      case EvElemStart(_, "dmdSec",attrs) if attrs("ID").startsWith("MODSMD_ISSUE") =>
        var break = false
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_, "identifier",attrs) => if (attrs("type") != null) attrs("type").toLowerCase match {
            case "issn" => issn = readContents
            case "binding" => binding = readContents
            case _ =>
          }
          case EvElemStart(_, "dateIssued",_) => date = readContents
          case EvElemEnd(_, "dmdSec") => break = true
          case _ =>
        }
      case EvElemStart(_, "amdSec",attrs) =>
        val imgId = attrs("ID").replaceFirst("PARAM","")
        var pmultiplier = -1.0
        var xs = -1
        var ys = -1
        var break = false
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"PixelSize",_) =>
            pmultiplier = readContents.toFloat
          case EvElemStart(_,"ImageWidth",_) =>
            xs = readContents.toInt
          case EvElemStart(_,"ImageLength",_) =>
            ys = readContents.toInt
          case EvElemEnd(_, "amdSec") => break = true
          case _ =>
        }
        imgsizes.put(imgId,(xs*pmultiplier,ys*pmultiplier))
      case EvElemStart(_, "structMap",sattrs) if sattrs("TYPE") == "PHYSICAL" =>
        var break = false
        var curImage = ""
        var curALTO = ""

        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"area",attrs) =>
            val fileID = attrs("FILEID")
            if (fileID.startsWith("IMG")) curImage = fileID
            else curALTO = fileID
          case EvElemEnd(_,"par") =>
            val dims = imgsizes(curImage)
            val page: Int = Try(Integer.parseInt(curALTO.substring(4))).getOrElse(0)
            pw3.synchronized {
              pw3.println(binding + "," + page + "," + dims._1 + "," + dims._2)
            }
          case EvElemEnd(_,"structMap") => break = true
          case _ =>
        }
      case EvElemStart(_,"structMap", attrs) if attrs("TYPE")=="LOGICAL" =>
        // process the logical structure hierarchically
        var hierarchy = Seq.empty[String]
        val pageTypes = new mutable.HashMap[Int,mutable.HashSet[String]]
        while (xml.hasNext) xml.next match {
          case EvElemStart(_,"div", sattrs) =>
            val divType =  sattrs("TYPE").toLowerCase
            hierarchy = hierarchy :+ divType
          case EvElemEnd(_,"div") =>
            hierarchy = hierarchy.dropRight(1)
          case EvElemStart(_,"area",sattrs) =>
            val page = Try(Integer.parseInt(sattrs("FILEID").substring(4))).getOrElse(0)
            pageTypes.getOrElseUpdate(page, new mutable.HashSet[String]) += hierarchy.mkString("_")
          case _ =>
        }
        pw12.synchronized {
          for (
            (page,types) <- pageTypes;
            pageType <- types
          )
            pw12.println(binding + "," + page + "," + pageType)
        }
      case _ =>
    }
    pw10.synchronized {
      pw10.println(binding + "," + issn + "," + date)
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
    pw1.println("issueId,page,words,chars")
    pw2 = new PrintWriter(new FileWriter(prefix+"npcolumns.csv"))
    pw2.println("issueId,page,wmodecols,modecols,wmediancols,mediancols")
    pw3 = new PrintWriter(new FileWriter(prefix+"npsizes1.csv"))
    pw3.println("issueId,page,width,height")
    pw4 = new PrintWriter(new FileWriter(prefix+"npstyles.csv"))
    pw4.println("issueId,page,font,size,bold,italics,underlined,words,chars,area")
    pw5 = new PrintWriter(new FileWriter(prefix+"npsizes2.csv"))
    pw5.println("issueId,page,width,height")
    pw6 = new PrintWriter(new FileWriter(prefix+"npsizes3.csv"))
    pw6.println("issueId,page,width,height")
    pw7 = new PrintWriter(new FileWriter(prefix+"npsoftware.csv"))
    pw7.println("issueId,page,lastModified,presoftware,preversion,ocrsoftware,ocrversion")
    pw8 = new PrintWriter(new FileWriter(prefix+"npboxes.csv"))
    pw8.println("issueId,page,xmin,ymin,xmax,ymax,words,chars")
    pw9 = new PrintWriter(new FileWriter(prefix+"npissues.csv"))
    pw9.println("issueId,ISSN,date")
    pw10 = new PrintWriter(new FileWriter(prefix+"npissues2.csv"))
    pw10.println("issueId,ISSN,date")
    pw11 = new PrintWriter(new FileWriter(prefix+"npcolumns-2.csv"))
    pw11.println("issueId,ISSN,columnwidth,words,chars,area")
    pw12 = new PrintWriter(new FileWriter(prefix+"nppagetypes.csv"))
    pw12.println("issueId,page,type")
    val toProcess: Seq[File] = for (
        dir<-args.dropRight(1);
        fd=new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!")
    ) yield fd
    val f2 = Future.sequence(toProcess.toStream.flatMap(fd => {
      getFileTree(fd)
        .filter(_.getName.endsWith("mets.xml"))
        .map(metsFile => processMETS(metsFile).map[Try[Unit]](Success(_)).recover{
          case cause =>
            logger.error("An error has occured processing "+metsFile+": " + getStackTraceAsString(cause))
            Failure(new Exception("An error has occured processing "+metsFile, cause))
        })
    }))
    val f = Future.sequence(toProcess.toStream.flatMap(fd => {
      getFileTree(fd)
        .filter(altoFile => altoFile.getName.endsWith(".xml") && !altoFile.getName.endsWith("metadata.xml") && !altoFile.getName.endsWith("mets.xml"))
        .map(altoFile => process(altoFile).map[Try[Unit]](Success(_)).recover{
          case cause =>
            logger.error("An error has occured processing "+altoFile+": " + getStackTraceAsString(cause))
            Failure(new Exception("An error has occured processing "+altoFile, cause))
        })
    }))
    var results = Await.result(f, Duration.Inf)
    if (results.exists(_.isFailure)) logger.error("Processing of at least one ALTO file resulted in an error.")
    else logger.info("Successfully processed all ALTO files.")
    pw1.close()
    pw2.close()
    pw4.close()
    pw5.close()
    pw6.close()
    pw7.close()
    pw8.close()
    pw9.close()
    results = Await.result(f2, Duration.Inf)
    if (results.exists(_.isFailure)) logger.error("Processing of at least one METS file resulted in an error.")
    else logger.info("Successfully processed all METS files.")
    pw3.close()
    pw10.close()
    pw11.close()
    pw12.close()
  }
}
