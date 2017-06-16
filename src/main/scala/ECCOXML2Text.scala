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
import scala.collection.Searching
import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import java.io.StringWriter
import com.bizo.mighty.csv.CSVWriter
import scala.xml.Utility
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.ArrayBlockingQueue
import scala.util.Failure
import scala.util.Success

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
  
  val numWorkers = sys.runtime.availableProcessors
  val queueCapacity = 1000
  val ec = ExecutionContext.fromExecutorService(
   new ThreadPoolExecutor(
     numWorkers, numWorkers,
     0L, TimeUnit.SECONDS,
     new ArrayBlockingQueue[Runnable](queueCapacity) {
       override def offer(e: Runnable) = {
         put(e)
         true
       }
     }
   )
  )
  
    case class Word(midX: Int, startX: Int, endX: Int, startY: Int, endY: Int, word: String) extends Ordered[Word] {
    def compare(that: Word): Int = this.midX-that.midX
  }
  
  class State {
    val content = new StringBuilder()
    var currentLine: ArrayBuffer[Word] = new ArrayBuffer //midx,startx,endx,starty,endy,content  
    var lastLine: ArrayBuffer[Word] = null
    def testParagraphBreakAtEndOfLine(guessParagraphs: Boolean): Option[String] = {
      var ret: Option[String] = None
      if (lastLine != null) { // we have two lines, check for a paragraph change between them.
        content.append(lastLine.map(_.word).mkString(" "))
        if (guessParagraphs) {
          val (lastSumEndY, lastSumHeight) = lastLine.foldLeft((0,0))((t,v) => (t._1 + v.endY, t._2 + (v.endY - v.startY)))
          val lastAvgHeight = lastSumHeight / lastLine.length
          val lastAvgEndY = lastSumEndY / lastLine.length
          val (currentSumStartY, currentSumHeight) = currentLine.foldLeft((0,0))((t,v) => (t._1 + v.startY, t._2 + (v.endY - v.startY)))
          val currentAvgHeight = currentSumHeight / currentLine.length
          val currentAvgStartY = currentSumStartY / currentLine.length
          
          val lastMaxHeight = lastLine.map(p => p.endY - p.startY).max
          val currentMaxHeight = currentLine.map(p => p.endY - p.startY).max

          val (lastSumWidth, lastSumChars) = lastLine.foldLeft((0,0))((t,v) => (t._1 + (v.endX - v.startX), t._2 + v.word.length))
          val (currentSumWidth, currentSumChars) = currentLine.foldLeft((0,0))((t,v) => (t._1 + (v.endX - v.startX), t._2 + v.word.length))
          
          val lastAvgCharWidth = lastSumWidth / lastSumChars
          val currentAvgCharWidth = currentSumWidth / currentSumChars

          val lastStartX = lastLine(0).startX
          val currentStartX = currentLine(0).startX
          if (
            (currentAvgStartY - lastAvgEndY > 0.90*math.min(lastMaxHeight, currentMaxHeight)) || // spacing 
            (currentStartX > lastStartX + 1.5*math.min(lastAvgCharWidth,currentAvgCharWidth)) || // indentation 
            (lastStartX > currentStartX + 2*math.min(lastAvgCharWidth,currentAvgCharWidth)) // catch edge cases
          ) {
            ret = Some(content.toString)
            content.clear()
          } else content.append('\n')
        } else content.append('\n')
      }
      lastLine = currentLine
      currentLine = new ArrayBuffer
      ret
    }
  }
  
  private def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_,_) => return null
      case EvText(text) => content.append(text)
      case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
        case Some(chr) => content.append(chr)
        case _ => content.append(er.entity)
      }
      case EvComment(_) => 
      case EvElemEnd(_,_) => break = true 
    }
    return content.toString.trim
  }
  
  private def readNextWordPossiblyEmittingAParagraph(attrs: MetaData, state: State, guessParagraphs: Boolean)(implicit xml: XMLEventReader): Option[String] = {
    val word = readContents
    val pos = attrs("pos")(0).text.split(",")
    val curStartX = pos(0).toInt
    val curEndX = pos(2).toInt
    val curMidX = (curStartX + curEndX) / 2
    val curStartY = pos(1).toInt
    val curEndY = pos(3).toInt
    val curHeight = curEndY - curStartY
    var ret: Option[String] = None 
    if (!state.currentLine.isEmpty) {
      var pos = Searching.search(state.currentLine).search(Word(curMidX,-1,-1,-1,-1,"")).insertionPoint - 1
      val Word(_,_,_,lastStartY, lastEndY, _) = state.currentLine(if (pos == -1) 0 else pos)
      if (curStartY > lastEndY || curMidX < state.currentLine.last.midX) // new line or new paragraph
        ret = state.testParagraphBreakAtEndOfLine(guessParagraphs)
      state.currentLine += (Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString))
    } else state.currentLine += (Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString))
    ret
  }

  def process(file: File, prefixLength: Int, dest: String, guessParagraphs: Boolean): Future[Unit] = Future({
    val dir = dest+file.getParentFile.getAbsolutePath.substring(prefixLength) + "/"
    new File(dir).mkdirs()
    val prefix = dir+file.getName.replace(".xml","")+"_"
    logger.info("Processing: "+file)
    val fis = new PushbackInputStream(new FileInputStream(file),2)
    var second = 0
    var first = fis.read()
    if (first == 0xEF) { // BOM
      fis.read()
      fis.read()
    } else fis.unread(first)
    var encoding = "ISO-8859-1"
    do {
      first = fis.read()
      second = fis.read()
      if (first == '<' && second=='!') while (fis.read()!='\n') {}
      if (first == '<' && (second=='?')) {
        for (i <- 0 until 28) fis.read()
        val es = new StringBuilder()
        var b = fis.read
        while (b!='"') {
          es.append(b.toChar)
          b = fis.read()
        }
        encoding = es.toString
        while (fis.read()!='\n') {}
      }
    } while (first == '<' && (second=='?' || second=='!'))
    fis.unread(second)
    fis.unread(first)
    implicit val xml = new XMLEventReader(Source.fromInputStream(fis,encoding))
    var currentSection: String = null
    val content = new StringBuilder()
    var page = 1
    var sw: PrintWriter = null
    var gw: CSVWriter = null
    var hw: CSVWriter = null
    var indent = ""
    var metadata = new StringBuilder()
    var break = false
    var lastLine: ArrayBuffer[(Int,Int,Int,String)] = null
    var currentLine = new ArrayBuffer[(Int,Int,Int,String)] //x,starty,endy,content
    var lastWasText = false
    var partNum = 0
    val state = new State()
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"text",_,_) =>
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"page",attrs,_) =>
            if (currentSection!=attrs("type")(0).text) {
              if (sw != null) sw.close()
              if (gw!=null) gw.close()
              if (hw!=null) hw.close()
              currentSection = attrs("type")(0).text
              partNum += 1
              sw = new PrintWriter(new File(prefix+partNum+"_"+currentSection.replaceAllLiterally("bodyPage","body")+".txt"))
              gw = null
              hw = null
              currentLine.clear()
            }
          case EvElemStart(_,"sectionHeader",attrs,_) =>
            val htype = attrs.get("type").map(_(0).text).getOrElse("")
            content.append(htype match {
              case "other" => "### "
              case "section" => "## "
              case _ =>  "# "
            })
            val heading = readContents.trim
            if (hw == null) hw = CSVWriter(prefix+partNum+"_"+currentSection.replaceAllLiterally("bodyPage","body")+"-headings.csv")
            hw.write(Seq(""+page,htype,heading))
            content.append(heading)
            content.append("\n\n")
          case EvElemStart(_,"graphicCaption",attrs,_) =>
            val htype = attrs.get("type").map(_(0).text).getOrElse("").toLowerCase
            val caption = readContents.trim
            if (gw == null) gw = CSVWriter(prefix+partNum+"_"+currentSection.replaceAllLiterally("bodyPage","body")+"-graphics.csv")
            gw.write(Seq(""+page,htype,attrs.get("colorimage").map(_(0).text).getOrElse(""),caption))
          case EvElemStart(_,"wd",attrs,_) => readNextWordPossiblyEmittingAParagraph(attrs, state, true) match {
            case Some(paragraph) => 
              content.append(paragraph)
              content.append("\n\n")
            case None => 
          }
          case EvElemEnd(_,"p") =>
            state.testParagraphBreakAtEndOfLine(true) match {
              case Some(paragraph) => 
                content.append(paragraph)
                content.append("\n\n")
              case None => 
            }
            state.content.append(state.lastLine.map(_.word).mkString(" "))
            if (state.content.length>0) {
              content.append(state.content.toString)
              content.append("\n\n")
            }
            state.content.clear()
            state.lastLine = null
            state.currentLine.clear
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
      case EvText(text) => if (!text.trim.isEmpty) {
          if (!lastWasText) metadata.append(indent)
          if (text.length==1 && Utility.Escapes.escMap.contains(text(0))) metadata.append(Utility.Escapes.escMap(text(0)))
          else metadata.append(text)
          lastWasText = !text.endsWith("\n")
        }
      case er: EvEntityRef =>
        metadata.append('&'); metadata.append(er.entity); metadata.append(';')
      case EvElemEnd(_, label) =>
        indent = indent.substring(0,indent.length-2)
        if (lastWasText) metadata.append('\n')
        metadata.append(indent + "</"+label+">\n")
        lastWasText = false
      case EvComment(_) => 
    }
    if (sw!=null) sw.close()
    if (gw!=null) gw.close()
    if (hw!=null) hw.close()
    sw = new PrintWriter(new File(prefix+"metadata.xml"))
    sw.append(metadata)
    sw.close()
    logger.info("Processed: "+file)
  })(ec)
  
  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
  
  def main(args: Array[String]): Unit = {
    val dest = new File(args.last).getAbsolutePath
    implicit val iec = ExecutionContext.Implicits.global
    val toProcess = for (
        dirp<-args.dropRight(1).toStream;
        guessParagraphs = dirp.endsWith("+");
        dir = if (guessParagraphs) dirp.dropRight(1) else dirp;
        fd=new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!");
        prefixLength = (if (!fd.isDirectory()) fd.getParentFile.getAbsolutePath else fd.getAbsolutePath).length
    ) yield (guessParagraphs,fd,prefixLength)
    val f = Future.sequence(toProcess.flatMap{ case (guessParagraphs,fd,prefixLength) => {
      getFileTree(fd)
        .filter(file => file.getName.endsWith(".xml") && !file.getName.startsWith("ECCO_tiff_manifest_") && !file.getName().endsWith("_metadata.xml") && {
          val dir = dest+file.getParentFile.getAbsolutePath.substring(prefixLength) + "/"
          val prefix = dir+file.getName.replace(".xml","")+"_"
          if (new File(prefix+"metadata.xml").exists) {
            logger.info("Already processed: "+file)
            false
          } else true
        })
        .map(file => {
          val f = process(file, prefixLength, dest, guessParagraphs)
          val path = file.getPath
          f.recover { 
            case cause =>
              logger.error("An error has occured processing "+path+": " + getStackTraceAsString(cause))
              throw new Exception("An error has occured processing "+path, cause) 
          }
        })
    }})
    f.onComplete {
      case Success(_) => logger.info("Successfully processed all sources.")
      case Failure(t) => logger.error("Processing of at least one source resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t))
    }
    Await.ready(f, Duration.Inf)
    ec.shutdown()
  }
}
