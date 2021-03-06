import java.io._
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import javax.xml.stream.XMLEventReader
import org.rogach.scallop.ScallopConf

import scala.collection.Searching
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success}
import scala.xml.parsing.XhtmlEntities
import XMLEventReaderSupport._
import com.github.tototoshi.csv.CSVWriter

import scala.xml.{MetaData, Utility}

object ECCOXML2Text extends LazyLogging {
  
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)

  /** helper function to turn XML attrs back into text */
  def attrsToString(attrs:Map[String,String]) = {
    attrs.size match {
      case 0 => ""
      case _ => attrs.toSeq.map(m => " " + m._1 + "='" + m._2 +"'").reduceLeft(_+_)
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
    var offset = 0l
    var page = 1
    var smw: PrintWriter = _
    var lmw: CSVWriter = _
    val content = new StringBuilder()
    var currentLine: ArrayBuffer[Word] = new ArrayBuffer //midx,startx,endx,starty,endy,content
    var lastLine: ArrayBuffer[Word] = _
    def testParagraphBreakAtEndOfLine(guessParagraphs: Boolean, emitLineBoxes: Boolean): Option[String] = {
      if (emitLineBoxes && currentLine.nonEmpty) {
        var lsy = Int.MaxValue
        var ley = Int.MinValue
        var lsx = Int.MaxValue
        var lex = Int.MinValue
        for (word <- currentLine) {
          if (word.startX < lsx) lsx = word.startX
          if (word.endX > lex) lex = word.endX
          if (word.startY < lsy) lsy = word.startY
          if (word.endY > ley) ley = word.endY
        }
        lmw.writeRow(Seq(""+page,""+lsx,""+lsy,""+lex,""+ley,currentLine.map(_.word).mkString(" ")))
      }
      var ret: Option[String] = None
      if (lastLine != null) { // we have two lines, check for a paragraph change between them.
        if (lastLine.nonEmpty) {
          for (word <- lastLine) {
            smw.append("" + offset + ',' + page + ',' + word.startX + ',' + word.startY + ',' + (word.endX - word.startX) + ',' + (word.endY - word.startY) + '\n')
            offset += word.word.length + 1
          }
          offset -= 1
          content.append(lastLine.map(_.word).mkString(" "))
        }
        if (guessParagraphs) {
          val (lastSumEndY, _) = lastLine.foldLeft((0,0))((t,v) => (t._1 + v.endY, t._2 + (v.endY - v.startY)))
          //val lastAvgHeight = lastSumHeight / lastLine.length
          val lastAvgEndY = lastSumEndY / lastLine.length
          val (currentSumStartY, _) = currentLine.foldLeft((0,0))((t,v) => (t._1 + v.startY, t._2 + (v.endY - v.startY)))
          //val currentAvgHeight = currentSumHeight / currentLine.length
          val currentAvgStartY = currentSumStartY / currentLine.length
          
          val lastMaxHeight = lastLine.map(p => p.endY - p.startY).max
          val currentMaxHeight = currentLine.map(p => p.endY - p.startY).max

          val (lastSumWidth, lastSumChars) = lastLine.foldLeft((0,0))((t,v) => (t._1 + (v.endX - v.startX), t._2 + v.word.length))
          val (currentSumWidth, currentSumChars) = currentLine.foldLeft((0,0))((t,v) => (t._1 + (v.endX - v.startX), t._2 + v.word.length))
          
          val lastAvgCharWidth = lastSumWidth / Math.max(lastSumChars, 1)
          val currentAvgCharWidth = currentSumWidth / Math.max(currentSumChars, 1)

          val lastStartX = lastLine(0).startX
          val currentStartX = currentLine(0).startX
          if (
            (currentAvgStartY - lastAvgEndY > 0.90*math.min(lastMaxHeight, currentMaxHeight)) || // spacing 
            (currentStartX > lastStartX + 1.5*math.min(lastAvgCharWidth,currentAvgCharWidth)) || // indentation 
            (lastStartX > currentStartX + 2*math.min(lastAvgCharWidth,currentAvgCharWidth)) // catch edge cases
          ) {
            ret = Some(content.toString)
            content.clear()
          } else {
            content.append('\n')
            offset += 1
          }
        } else {
          content.append('\n')
          offset += 1
        }
      }
      lastLine = currentLine
      currentLine = new ArrayBuffer
      ret
    }
  }

  private def decodeEntity(entity: String): String = {
    XhtmlEntities.entMap.get(entity) match {
      case Some(chr) => chr.toString
      case None =>
        logger.warn("Encountered unknown entity "+entity)
        '〈' + entity + '〉'
    }
  }
  
  private def readContents(implicit xml: Iterator[EvEvent]): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_) => return null
      case EvText(text,_) => content.append(text)
      case er: EvEntityRef => content.append(decodeEntity(er.entity))
      case EvComment(comment) if comment == " unknown entity apos; " => content.append('\'')
      case EvComment(comment) if comment.startsWith(" unknown entity") =>
        val entity = comment.substring(16, comment.length - 2)
        content.append(decodeEntity(entity))
      case EvComment(comment) =>
        logger.debug("Encountered comment: "+comment)
      case EvElemEnd(_,_) => break = true 
    }
    content.toString
  }
  
  private def readNextWordPossiblyEmittingAParagraph(pos: Seq[Int], state: State, guessParagraphs: Boolean, emitLineBoxes: Boolean)(implicit xml: Iterator[EvEvent]): Option[String] = {
    val word = readContents
    val (curStartX,curEndX) = if (pos(0)<pos(2)) (pos(0),pos(2)) else (pos(2),pos(0))
    val curMidX = (curStartX + curEndX) / 2
    val (curStartY,curEndY) = if (pos(1)<pos(3)) (pos(1),pos(3)) else (pos(3),pos(1))
    //val curHeight = curEndY - curStartY
    var ret: Option[String] = None 
    if (state.currentLine.nonEmpty) {
      val pos = state.currentLine.search(Word(curMidX,-1,-1,-1,-1,"")).insertionPoint - 1
      val Word(_,_,_,_, lastEndY, _) = state.currentLine(if (pos == -1) 0 else pos)
      if (curStartY > lastEndY || curMidX < state.currentLine.last.midX) // new line or new paragraph
        ret = state.testParagraphBreakAtEndOfLine(guessParagraphs, emitLineBoxes)
      state.currentLine += Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString)
    } else state.currentLine += Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString)
    ret
  }

  def process(file: File, prefixLength: Int, dest: String, guessParagraphs: Boolean, omitStructure: Boolean, omitHeadings: Boolean, emitLineBoxes: Boolean): Future[Unit] = Future({
    val dir = dest+file.getParentFile.getAbsolutePath.substring(prefixLength) + "/"
    new File(dir).mkdirs()
    val prefix = dir+file.getName.replace(".xml","")+"_"
    logger.info("Processing: "+file)
    val dw: PrintWriter = new PrintWriter(new File(prefix.dropRight(1)+".txt"))
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
        for (_ <- 0 until 28) fis.read()
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
    implicit val xml = getXMLEventReader(fis,encoding)
    var currentSection: String = null
    val content = new StringBuilder()
    var sw: PrintWriter = null
    var gw: CSVWriter = null
    var hw: CSVWriter = null
    var indent = ""
    val metadata = new StringBuilder()
    var break = false
    var lastWasText = false
    var partNum = 0
    val state = new State()
    state.smw = new PrintWriter(new File(prefix.substring(0,prefix.length-1)+"-payload.csv"))
    if (emitLineBoxes) state.lmw = CSVWriter.open(prefix.substring(0,prefix.length-1)+"-lineboxes.csv")
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"text",_) =>
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"page",attrs) =>
            if (currentSection!=attrs("type")) {
              if (sw != null) sw.close()
              if (gw!=null) gw.close()
              if (hw!=null) hw.close()
              currentSection = attrs("type")
              partNum += 1
              sw = new PrintWriter(new File(prefix+partNum+"_"+currentSection.replaceAllLiterally("bodyPage","body")+".txt"))
              gw = null
              hw = null
              state.currentLine.clear()
            }
          case EvElemStart(_,"sectionHeader",attrs) =>
            val htype = attrs.get("type").getOrElse("")
            if (!omitStructure) {
              val hl = htype match {
                case "other" => "### "
                case "section" => "## "
                case _ => "# "
              }
              content.append(hl)
              state.offset += hl.length
            }
            val heading = readContents
            if (hw == null) hw = CSVWriter.open(prefix + partNum + "_" + currentSection.replaceAllLiterally("bodyPage", "body") + "-headings.csv")
            hw.writeRow(Seq("" + state.page, htype, heading))
            if (!omitHeadings) {
              state.offset += heading.length + 2
              content.append(heading)
              content.append("\n\n")
            }
          case EvElemStart(_,"graphicCaption",attrs) =>
            val htype = attrs.get("type").getOrElse("").toLowerCase
            val caption = readContents
            if (gw == null) gw = CSVWriter.open(prefix+partNum+"_"+currentSection.replaceAllLiterally("bodyPage","body")+"-graphics.csv")
            gw.writeRow(Seq(""+state.page,htype,attrs.getOrElse("colorimage", ""),caption))
          case EvElemStart(_,"wd",attrs) =>
            val pos = attrs("pos").split(",")
            readNextWordPossiblyEmittingAParagraph(pos.map(_.toInt), state, guessParagraphs, emitLineBoxes) match {
            case Some(paragraph) => 
              content.append(paragraph)
              content.append("\n\n")
              state.offset += 2
            case None => 
          }
          case EvElemEnd(_,"p") =>
            state.testParagraphBreakAtEndOfLine(guessParagraphs, emitLineBoxes) match {
              case Some(paragraph) => 
                content.append(paragraph)
                content.append("\n\n")
                state.offset += 2
              case None => 
            }
            if (state.lastLine.nonEmpty) {
              for (word <- state.lastLine) {
                state.smw.append("" + state.offset + ',' + state.page + ',' + word.startX + ',' + word.startY + ',' + (word.endX - word.startX) + ',' + (word.endY - word.startY) + '\n')
                state.offset += word.word.length + 1
              }
              state.offset -= 1
              state.content.append(state.lastLine.map(_.word).mkString(" "))
            }
            if (state.currentLine.nonEmpty) {
              if (state.lastLine.nonEmpty) {
                state.content.append('\n')
                state.offset += 1
              }
              for (word <- state.currentLine) {
                state.smw.append(""+state.offset+','+state.page+','+word.startX+','+word.startY+','+(word.endX-word.startX)+','+(word.endY-word.startY)+ '\n')
                state.offset += word.word.length + 1
              }
              state.offset -= 1
              state.content.append(state.currentLine.map(_.word).mkString(" "))
            }
            if (state.content.nonEmpty) {
              content.append(state.content.toString)
              content.append("\n\n")
              state.offset += 2
            }
            state.content.clear()
            state.lastLine = null
            state.currentLine.clear
          case EvElemEnd(_,"page") =>
            val pw = new PrintWriter(new File(prefix+"page"+state.page+".txt"))
            pw.append(content)
            pw.close()
            sw.append(content)
            dw.append(content)
            content.clear()
            state.page+=1
          case EvElemEnd(_,"text") => break = true
          case _ =>
        }
      case EvElemStart(_,"documentID",attrs) =>
        metadata.append(indent + "<documentID"+ attrsToString(attrs) + ">\n")
        val documentID = readContents
        metadata.append(indent + "  " + documentID + "\n")
        metadata.append(indent + "</documentID>")
        val idw = new PrintWriter(new File(prefix.substring(0,prefix.length-1)+"-id.txt"))
        idw.append(documentID)
        idw.close()
      case EvElemStart(_,"PSMID",attrs) =>
        metadata.append(indent + "<PSMID"+ attrsToString(attrs) + ">\n")
        val documentID = readContents
        metadata.append(indent + "  " + documentID + "\n")
        metadata.append(indent + "</PSMID>")
        val idw = new PrintWriter(new File(prefix.substring(0,prefix.length-1)+"-id.txt"))
        idw.append(documentID)
        idw.close()
      case EvElemStart(_, label, attrs) =>
        metadata.append(indent + "<" + label + attrsToString(attrs) + ">\n")
        indent += "  "
      case EvText(text,_)  => if (!text.trim.isEmpty) {
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
    if (state.smw != null) state.smw.close()
    if (emitLineBoxes) state.lmw.close()
    dw.close()
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
    val opts = new ScallopConf(args) {
      val directories = trailArg[List[String]](required = true)
      val dest = opt[String](required = true)
      val omitStructure = opt[Boolean]()
      val omitHeadings = opt[Boolean]()
      val emitLineBoxes = opt[Boolean]()
      verify()
    }
    val dest = new File(opts.dest()).getAbsolutePath
    implicit val iec = ExecutionContext.Implicits.global
    val toProcess = for (
        dirp<-opts.directories().toStream;
        guessParagraphs = dirp.endsWith("+");
        dir = if (guessParagraphs) dirp.dropRight(1) else dirp;
        fd=new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!");
        prefixLength = (if (!fd.isDirectory) fd.getParentFile.getAbsolutePath else fd.getAbsolutePath).length
    ) yield (guessParagraphs,fd,prefixLength)
    val omitStructure = opts.omitStructure()
    val omitHeadings = opts.omitHeadings()
    val emitLineBoxes = opts.emitLineBoxes()
    val f = Future.sequence(toProcess.flatMap{ case (guessParagraphs,fd,prefixLength) =>
      getFileTree(fd)
        .filter(file => file.getName.endsWith(".xml") && !file.getName.startsWith("ECCO_tiff_manifest_") && !file.getName.endsWith("_metadata.xml") && {
          val dir = dest+file.getParentFile.getAbsolutePath.substring(prefixLength) + "/"
          val prefix = dir+file.getName.replace(".xml","")+"_"
          if (new File(prefix+"metadata.xml").exists) {
            logger.info("Already processed: "+file)
            false
          } else true
        })
        .map(file => {
          val f = process(file, prefixLength, dest, guessParagraphs, omitStructure, omitHeadings, emitLineBoxes)
          val path = file.getPath
          f.recover {
            case cause =>
              logger.error("An error has occured processing "+path+": " + getStackTraceAsString(cause))
              throw new Exception("An error has occured processing "+path, cause)
          }
        })
    })
    f.onComplete {
      case Success(_) => logger.info("Successfully processed all sources.")
      case Failure(t) => logger.error("Processing of at least one source resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t))
    }
    Await.ready(f, Duration.Inf)
    ec.shutdown()
  }
}
