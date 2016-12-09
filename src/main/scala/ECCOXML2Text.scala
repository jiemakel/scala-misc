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
import scala.collection.Searching
import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import java.io.StringWriter
import com.bizo.mighty.csv.CSVWriter
import scala.xml.Utility

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

  def process(file: File, prefixLength: Int, dest: String, guessParagraphs: Boolean): Future[Unit] = Future {
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
    val xml = new XMLEventReader(Source.fromInputStream(fis,encoding))
    var currentSection: String = null
    val content = new StringBuilder()
    val word = new StringBuilder()
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
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"text",_,_) =>
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"page",attrs,_) =>
            if (currentSection!=attrs("type")(0).text) {
              if (sw != null) sw.close()
              if (gw!=null) gw.close()
              if (hw!=null) hw.close()
              currentSection = attrs("type")(0).text
              sw = new PrintWriter(new File(prefix+currentSection.replaceAllLiterally("bodyPage","body")+".txt"))
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
            word.clear()
            var break2 = false
            while (xml.hasNext && !break2) xml.next match {
              case EvText(text) => word.append(text)
              case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
                case Some(chr) => word.append(chr)
                case _ => word.append(er.entity)
              }
              case EvComment(_) => 
              case EvElemEnd(_,"sectionHeader") => break2 = true 
            }
            if (hw == null) hw = CSVWriter(prefix+currentSection.replaceAllLiterally("bodyPage","body")+"-headings.csv")
            val wordS = word.toString.trim
            hw.write(Seq(""+page,htype,wordS))
            content.append(wordS)
            content.append("\n\n")
          case EvElemStart(_,"graphicCaption",attrs,_) =>
            val htype = attrs.get("type").map(_(0).text).getOrElse("").toLowerCase
            word.clear()
            var break2 = false
            while (xml.hasNext && !break2) xml.next match {
              case EvText(text) => word.append(text)
              case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
                case Some(chr) => word.append(chr)
                case _ => word.append(er.entity)
              }
              case EvComment(_) => 
              case EvElemEnd(_,"graphicCaption") => break2 = true 
            }
            if (gw == null) gw = CSVWriter(prefix+currentSection.replaceAllLiterally("bodyPage","body")+"-graphics.csv")
            gw.write(Seq(""+page,htype,attrs.get("colorimage").map(_(0).text).getOrElse(""),word.toString.trim))
          case EvElemStart(_,"wd",attrs,_) =>
            word.clear()
            var break2 = false
            while (xml.hasNext && !break2) xml.next match {
              case EvText(text) => word.append(text)
              case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
                case Some(chr) => word.append(chr)
                case _ => word.append(er.entity)
              }
              case EvComment(_) => 
              case EvElemEnd(_,"wd") => break2 = true 
            }
            val pos = attrs("pos")(0).text.split(",")
            val curX = (pos(2).toInt + pos(0).toInt) / 2
            val curStartY = pos(1).toInt
            val curEndY = pos(3).toInt
            val curHeight = curEndY - curStartY
            if (!currentLine.isEmpty) {
              var pos = Searching.search(currentLine).search((curX,-1,-1,"")).insertionPoint - 1
              val (_, lastStartY, lastEndY, _) = currentLine(if (pos == -1) 0 else pos)
              if (curStartY > lastEndY || curX < currentLine.last._1) { // new line or new paragraph
                if (lastLine != null) { // we have two lines, check for a paragraph change between them.
                  content.append(lastLine.map(_._4).mkString(" "))
                  if (guessParagraphs) {
                    val (lastSumEndY, lastSumHeight) = lastLine.foldLeft((0,0))((t,v) => (t._1 + v._3, t._2 + (v._3 - v._2)))
                    val lastAvgHeight = lastSumHeight / lastLine.length
                    val lastAvgEndY = lastSumEndY / lastLine.length
                    val (currentSumStartY, currentSumHeight) = currentLine.foldLeft((0,0))((t,v) => (t._1 + v._2, t._2 + (v._3 - v._2)))
                    val currentAvgHeight = currentSumHeight / currentLine.length
                    val currentAvgStartY = currentSumStartY / currentLine.length
                    val lastMaxHeight = lastLine.map(p => p._3 - p._2).max
                    val currentMaxHeight = currentLine.map(p => p._3 - p._2).max
                    if (currentAvgStartY - lastAvgEndY > 0.95*math.min(lastMaxHeight, currentMaxHeight))
                      content.append('\n')
                  }
                  content.append('\n')
                }
                lastLine = currentLine
                currentLine = new ArrayBuffer
                currentLine += ((curX, curStartY, curEndY, word.toString))
              } else currentLine += ((curX, curStartY, curEndY, word.toString))
            } else currentLine += ((curX, curStartY, curEndY, word.toString))
          case EvElemEnd(_,"p") =>
            if (!currentLine.isEmpty) {
              if (lastLine != null) { // we have two lines, check for a paragraph change between them.
                  content.append(lastLine.map(_._4).mkString(" "))
                  if (guessParagraphs) {
                    val (lastSumEndY, lastSumHeight) = lastLine.foldLeft((0,0))((t,v) => (t._1 + v._3, t._2 + (v._3 - v._2)))
                    val lastAvgHeight = lastSumHeight / lastLine.length
                    val lastAvgEndY = lastSumEndY / lastLine.length
                    val (currentSumStartY, currentSumHeight) = currentLine.foldLeft((0,0))((t,v) => (t._1 + v._2, t._2 + (v._3 - v._2)))
                    val currentAvgHeight = currentSumHeight / currentLine.length
                    val currentAvgStartY = currentSumStartY / currentLine.length
                    val lastMaxHeight = lastLine.map(p => p._3 - p._2).max
                    val currentMaxHeight = currentLine.map(p => p._3 - p._2).max
                    if (currentAvgStartY - lastAvgEndY > 0.95*math.min(lastMaxHeight, currentMaxHeight))
                      content.append('\n')
                  }
                  content.append('\n')
                }
              content.append(currentLine.map(_._4).mkString(" "))
              lastLine = null
              currentLine.clear()
            }
            if (content.length < 2 || content.last!='\n') content.append("\n\n")
            else if (content.substring(content.length - 2)!="\n\n") content.append("\n")
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
  }
  
  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
  
  def main(args: Array[String]): Unit = {
    val dest = new File(args.last).getAbsolutePath
    val f = Future.sequence(
      for (
        dirp<-args.dropRight(1).toSeq;
        guessParagraphs = dirp.endsWith("+");
        dir = if (guessParagraphs) dirp.dropRight(1) else dirp;
        fd=new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!");
        prefixLength = (if (!fd.isDirectory()) fd.getParentFile.getAbsolutePath else fd.getAbsolutePath).length;
        file <- getFileTree(fd); if (file.getName().endsWith(".xml") && !file.getName().startsWith("ECCO_tiff_manifest_") && !file.getName().endsWith("_metadata.xml") && {
          val dir = dest+file.getParentFile.getAbsolutePath.substring(prefixLength) + "/"
          val prefix = dir+file.getName.replace(".xml","")+"_"
          if (new File(prefix+"metadata.xml").exists) {
            logger.info("Already processed: "+file)
            false
          } else true
      })) yield {
      val f = process(file,prefixLength,dest, guessParagraphs)
      f.onFailure { case t => logger.error("An error has occured processing "+file+": " + getStackTraceAsString(t)) }
      f.onSuccess { case _ => logger.info("Processed: "+file) }
      f.recover { case cause => throw new Exception("An error has occured processing "+file, cause) }
    })
    f.onFailure { case t => logger.error("Processing of at least one file resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t)) }
    f.onSuccess { case _ => logger.info("Successfully processed all files.") }
    Await.result(f, Duration.Inf)
  }
}
