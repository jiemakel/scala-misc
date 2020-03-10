import java.io.{BufferedWriter, File, FileInputStream, FileWriter, PrintWriter}

import ECCOXML2Text.attrsToString
import com.github.tototoshi.csv.CSVWriter
import org.rogach.scallop.ScallopConf

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.Utility
import scala.xml.parsing.XhtmlEntities
import XMLEventReaderSupport._
import javax.xml.stream.XMLEventReader

object EEBOConverter extends ParallelProcessor {

  private val eeboMap = new mutable.HashMap[String,String]()

  private def decodeEntity(entity: String): String = {
    XhtmlEntities.entMap.get(entity) match {
      case Some(chr) => chr.toString
      case None => eeboMap.get(entity) match {
        case Some(chr) => chr
        case None =>
          logger.warn("Encountered unknown entity "+entity)
          '〈' + entity + '〉'
      }
    }
  }

  // see http://www.textcreationpartnership.org/docs/dox/cheat.html
  private val ignoredElems = HashSet("SUBST","CHOICE","SIC","FW","SUP","SUB","ABBR","Q", "MILESTONE","SEG","UNCLEAR","ABOVE","BELOW","DATE","BIBL","SALUTE","ADD","REF","PTR")
  private val paragraphElems = HashSet("P","AB","CLOSER","SP","STAGE","TABLE","LG","TRAILER","OPENER","FIGURE","LETTER","ARGUMENT","DATELINE","SIGNED","EPIGRAPH","GROUP","TEXT","BYLINE","POSTSCRIPT")
  private val lineElems = HashSet("L","ROW","SPEAKER","LB")

  private def append(content: String)(implicit writers: Array[StringBuilder]): Unit = {
    writers.foreach(_.append(content))
  }

  private def clean(content: StringBuilder): String = clean(content.toString)
  private def clean(content: String): String = {
    trimSpace(content.toString.replaceAllLiterally(" |  | "," | ").replaceAll("\n\n+","\n\n"))
  }

  private def trimSpace(value: String): String = {
    if (value==null) return null
    var len = value.length
    var st = 0
    while (st < len && (value(st) == ' ' || value(st) == '\n')) st += 1
    while (st < len && (value(len - 1) == ' ' || value(len -1) == '\n')) len -= 1
    if ((st > 0) || (len < value.length)) value.substring(st, len)
    else value
  }

  class ProcessOpts(
    val currentFilePrefix: String,
    val currentRootPrefix: String,
    val dest: String,
    val prefixLength: Int,
    val inlineNotes: Boolean,
    val omitEmphases: Boolean,
    val omitStructure: Boolean,
    val onlyCoreTranscription: Boolean,
    val extractsAtDocumentLevel: Boolean,
    val extractsAtPageLevel: Boolean,
    val rehyphenate: Boolean,
    val startOffset: Int,
    val currentElem: String
   ) {
    val outputFilePrefix = dest + "/" + this.currentFilePrefix.substring(prefixLength)
    val outputRootPrefix = dest + "/" + this.currentRootPrefix.substring(prefixLength)
    def child(prefix: String, startOffset: Int, elem: String): ProcessOpts = new ProcessOpts(
      this.currentFilePrefix + "_" + prefix,
      this.currentRootPrefix,
      this.dest,
      this.prefixLength,
      this.inlineNotes,
      this.omitEmphases,
      this.omitStructure,
      this.onlyCoreTranscription,
      this.extractsAtDocumentLevel,
      this.extractsAtPageLevel,
      this.rehyphenate,
      startOffset,
      elem)
  }

  private class PageTracker(val pbw: PrintWriter, val dcontent: StringBuilder) {
    var currentPage: String = "0"
    var subPage  = 1
    val pcontent = new StringBuilder()
  }

  private def process(opts: ProcessOpts)(implicit xml: Iterator[EvEvent], pt: PageTracker): String = {
    val scontent = new StringBuilder()
    val content = new StringBuilder()
    implicit val contents = Array(pt.pcontent, scontent, content)
    val divLevels = new ArrayBuffer[Int]()
    var currentDivLevel = 0
    divLevels += currentDivLevel
    var break = false
    var listDepth = 0
    var inTable = false
    var lastAddedNewLine = false
    var dpart = 1
    while (xml.hasNext && !break) {
      var addedNewLine = false
      xml.next match {
        case EvElemEnd(_, opts.currentElem) => break = true
        case EvElemStart(_, elem, _) if ignoredElems.contains(elem) =>
        case EvElemEnd(_, elem) if ignoredElems.contains(elem) =>
        case EvElemStart(_, elem, _) if elem.startsWith("DIV") => divLevels += currentDivLevel; currentDivLevel = elem.last.toString.toInt // TYPE, LANG, N
        case EvElemEnd(_, elem) if elem.startsWith("DIV") =>
          divLevels.trimEnd(1)
          currentDivLevel = divLevels.last
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
        case EvElemStart(_, "HEAD",_) => if (!opts.omitStructure) append("#" * currentDivLevel +" ")
        case EvElemEnd(_, "HEAD") =>
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
        case EvElemStart(_, "ARGUMENT",_) => if (!opts.omitStructure) append("## ")
        case EvElemStart(_, elem,_) if elem == "FRONT" || elem == "BACK" || elem == "TEXT" || elem == "BODY" =>
          val scc = clean(scontent)
          if (scc.nonEmpty) {
            val sw = new PrintWriter(new File(opts.outputFilePrefix + "_"+ dpart +"_pre"+elem.toLowerCase+".txt"))
            sw.append(scc)
            sw.close()
            dpart += 1
            content.append("\n\n")
          }
          scontent.clear()
          val cpo = opts.child(dpart+"_"+elem.toLowerCase, opts.startOffset + content.length, elem)
          val cc = process(cpo)
          if (cc.nonEmpty) {
            val sw = new PrintWriter(new File(cpo.outputFilePrefix + ".txt"))
            sw.append(cc)
            sw.close()
            content.append(cc)
            content.append("\n\n")
            dpart += 1
          }
        case EvElemStart(_, "FIGDESC", _) => if (!opts.onlyCoreTranscription) append("〈 image: ") else {
          val cpo = opts.child("image_at_"+ content.length, opts.startOffset + content.length , "FIGDESC")
          val imagedesc = process(cpo)
          val sw = new PrintWriter(if (opts.extractsAtDocumentLevel) cpo.outputRootPrefix+"_image_at_" + (opts.startOffset+content.length) + ".txt" else cpo.outputFilePrefix + ".txt")
          sw.append(imagedesc)
          sw.close()
          if (opts.extractsAtPageLevel) {
            val pcc = clean(pt.pcontent)
            val sw = new PrintWriter(cpo.outputRootPrefix+"_image_at_page"+pt.currentPage+"_at_" + pcc.length + ".txt")
            sw.append(imagedesc)
            sw.close()
          }
        }
        case EvElemEnd(_, "FIGDESC") => if (!opts.onlyCoreTranscription) append(" 〉")
        case EvElemStart(_, "HI", _) => if (!opts.omitEmphases) append("*")
        case EvElemEnd(_, "HI") => if (!opts.omitEmphases) append("*")
        case EvElemStart(_, "PB", attrs) => //MS="y" REF="48" N="90"
          val pid: String = attrs.getOrElse("REF","-1")
          val pcc = clean(pt.pcontent)
          if (pcc.nonEmpty || pt.currentPage != "0") {
            val pw = new PrintWriter(new BufferedWriter(new FileWriter(new File(opts.outputRootPrefix + "_page" + pt.currentPage + (if (pid == pt.currentPage) {
              pt.subPage += 1
              "_subpage" + pt.subPage
            } else "") + ".txt"), true)))
            pw.append(pcc)
            pw.close()
            pt.pcontent.clear()
          }
          addedNewLine = true
          if (pid!=pt.currentPage) {
            val bb = clean(pt.dcontent.toString + content.toString)
            val offset = opts.startOffset + bb.length
            pt.pbw.append("" + offset + ',' + pid + '\n')
            pt.subPage = 1
          }
          pt.currentPage = pid
        case EvElemEnd(_, "PB") =>
        case EvElemStart(_, elem, _) if elem == "NOTE" || elem == "HEADNOTE" || elem == "TAILNOTE" || elem == "DEL" || elem == "CORR" =>
          if (!opts.inlineNotes) {
            val cpo = opts.child(elem.toLowerCase+"_at_"+ content.length, opts.startOffset + content.length , elem)
            val note = process(cpo)
            val sw = new PrintWriter(if (opts.extractsAtDocumentLevel) cpo.outputRootPrefix+"_"+elem.toLowerCase+"_at_" + (opts.startOffset+content.length) + ".txt" else cpo.outputFilePrefix + ".txt")
            sw.append(note)
            sw.close()
            if (opts.extractsAtPageLevel) {
              val pcc = clean(pt.pcontent)
              val sw = new PrintWriter(cpo.outputRootPrefix+"_"+elem.toLowerCase+"_at_page"+pt.currentPage+"_at_" + pcc.length + ".txt")
              sw.append(note)
              sw.close()
            }
          }
        case EvElemEnd(_, elem) if opts.inlineNotes && (elem == "NOTE" || elem == "HEADNOTE" || elem == "TAILNOTE" || elem == "DEL" || elem == "CORR") =>
        case EvElemStart(_, "GAP", attrs) =>
          val disp = attrs.getOrElse("DISP", "〈?〉")
          gfw.synchronized(gfw.writeRow(Seq(opts.outputRootPrefix,pt.currentPage,""+pt.subPage,disp)))
          if (!opts.onlyCoreTranscription) append(disp)
        case EvElemEnd(_, "GAP") =>
        case EvElemStart(_, "LIST",_) => listDepth += 1
        case EvElemEnd(_, "LIST") =>
          listDepth -= 1
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
        case EvElemStart(_, elem,_) if paragraphElems.contains(elem) =>
          if (elem=="TABLE") inTable = true
          addedNewLine = true
          if (content.nonEmpty) {
            if (content.last != '\n') append("\n\n") else if (content.length < 2 || content.charAt(content.length - 2) != '\n') append("\n")
          }
        case EvElemEnd(_, elem) if paragraphElems.contains(elem) =>
          if (elem=="TABLE") inTable = false
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
        case EvElemStart(_, elem,_) if lineElems.contains(elem) =>
          addedNewLine = true
          if (content.nonEmpty && content.last != '\n') append("\n")
        case EvElemEnd(_, elem) if lineElems.contains(elem) =>
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n")
        case EvElemStart(_, "CELL",_) => if (!opts.omitStructure) append(" | ") else append(" ")
        case EvElemEnd(_, "CELL") => if (!opts.omitStructure) append(" | ")
        case EvElemStart(_, "ITEM",_) =>
          /* if (lastElemEnd == "LABEL") append(": ") else */ if (!opts.omitStructure) append(" * ")
        case EvElemEnd(_, "ITEM") =>
        case EvElemStart(_, "LABEL",_) => if (listDepth == 0 && !opts.omitStructure) append("#" * (currentDivLevel + 1) +" ")
        case EvElemEnd(_, "LABEL") =>
          addedNewLine = true
          if (listDepth == 0) {
            if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
          } else if (content.isEmpty || content.last != '\n') append("\n")
        case EvText(text) =>
          val text2 = if (inTable) text.replaceAllLiterally("\n","") else if (lastAddedNewLine && text.head=='\n') text.tail else text
          if (opts.rehyphenate)
            append(text2.replaceAllLiterally("|","-\n").replaceAllLiterally("∣","-\n"))
          else
            append(text2.replaceAllLiterally("|","").replaceAllLiterally("∣",""))
        case er: EvEntityRef => append(decodeEntity(er.entity))
        case EvComment(comment) if comment == " unknown entity apos; " => append("'")
        case EvComment(comment) if comment.startsWith(" unknown entity") =>
          val entity = comment.substring(16, comment.length - 2)
          append(decodeEntity(entity))
        case EvComment(comment) =>
          logger.debug("Encountered comment: "+comment)
        case ent => logger.warn("Unknown event in "+opts.currentFilePrefix+" : "+ent)
      }
      lastAddedNewLine = addedNewLine
    }
    clean(content)
  }


  def index(file: File, dest: String, prefixLength: Int, inlineNotes: Boolean, omitEmphases: Boolean, omitStructure: Boolean, onlyCoreTranscription: Boolean, extractsAtDocumentLevel: Boolean, extractsAtPageLevel: Boolean, rehyphenate: Boolean): Unit = {
    val opts = new ProcessOpts(file.getAbsolutePath.replace(".xml",""),file.getAbsolutePath.replace(".xml",""), dest, prefixLength, inlineNotes, omitEmphases, omitStructure, onlyCoreTranscription, extractsAtDocumentLevel, extractsAtPageLevel, rehyphenate, 0, null)
    val fis = new FileInputStream(file)
    implicit val xml = getXMLEventReader(fis, "UTF-8")
    val dcontent = new StringBuilder()
    new File(dest + file.getParentFile.getAbsolutePath.substring(prefixLength)).mkdirs()
    val pbw = new PrintWriter(new File(opts.outputFilePrefix+"_pbs.csv"))
    implicit val pt = new PageTracker(pbw,dcontent)
    var text = 1
    var indent = ""
    val metadata = new StringBuilder()
    var lastWasText = false
    while (xml.hasNext) xml.next match {
      case EvElemStart(_, "HEADER", _) =>
        var break = false
        while (!break) xml.next match {
          case EvElemEnd(_, "HEADER") => break = true
          case EvElemStart(_, label, attrs) =>
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
        val sw = new PrintWriter(new File(opts.outputFilePrefix+".metadata.xml"))
        sw.append(metadata)
        sw.close()
      case EvElemStart(_, "TEXT",_) =>
        val copts = opts.child(text+"_text", dcontent.length, "TEXT")
        val content = process(copts)
        if (content.nonEmpty) {
          val sw = new PrintWriter(new File(copts.outputFilePrefix + ".txt"))
          sw.append(content)
          sw.close()
          dcontent.append(content)
          dcontent.append("\n\n")
          text += 1
        }
      case _ =>
    }
    if (dcontent.nonEmpty) {
      val cw = new PrintWriter(new File(opts.outputFilePrefix + ".txt"))
      cw.append(clean(dcontent))
      cw.close()
    }
    val pcc = clean(pt.pcontent)
    if (pcc.nonEmpty || pt.currentPage != "0") {
      val pw = new PrintWriter(new BufferedWriter(new FileWriter(new File(opts.outputRootPrefix + "_page" + pt.currentPage + (if (pt.subPage!=1) {
        "_subpage" + pt.subPage
      } else "") + ".txt"), true)))
      pw.append(pcc)
      pw.close()
      pt.pcontent.clear()
    }
    pbw.close()
    fis.close()
    logger.info("Successfully processed "+file)
  }

  var gfw: CSVWriter = _

  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val directories = trailArg[List[String]](required = true)
      val dest = opt[String](required = true)
      val inlineNotes = opt[Boolean]()
      val onlyCoreTranscription = opt[Boolean]()
      val omitStructure = opt[Boolean]()
      val omitEmphases = opt[Boolean]()
      val extractsAtDocumentLevel = opt[Boolean]()
      val extractsAtPageLevel = opt[Boolean]()
      val rehyphenate = opt[Boolean]()
      verify()
    }
    val inlineNotes = opts.inlineNotes()
    val onlyCoreTranscription = opts.onlyCoreTranscription()
    val omitStructure = opts.omitStructure() || onlyCoreTranscription
    val omitEmphases = opts.omitEmphases() || omitStructure
    val extractsAtDocumentLevel = opts.extractsAtDocumentLevel()
    val extractsAtPageLevel = opts.extractsAtPageLevel()
    val rehyphenate = opts.rehyphenate()
    val dest = opts.dest()
    new File(dest).mkdirs()
    val lr = "<!ENTITY (.*?) \"(.*?)\"".r
    val lr2 = "&#h(.*?);".r
    gfw = CSVWriter.open(dest+"/gaps.csv")
    for (line <- Source.fromInputStream(getClass.getResourceAsStream("Eebochar-xml-all-view.ent")).getLines; m <- lr.findFirstMatchIn(line)) {
      val entity = m.group(1)
      val replacement = lr2.replaceAllIn(m.group(2), m => Integer.valueOf(m.group(1), 16).toChar.toString)
      eeboMap += entity -> replacement
    }
    feedAndProcessFedTasksInParallel(() => {
      for (dir <- opts.directories(); f = new File(dir); prefixLength = f.getAbsolutePath.length)
        getFileTree(f).filter(_.getName.endsWith(".xml")).foreach(file => addTask(file.getPath, () => index(file,
          dest, prefixLength, inlineNotes, omitEmphases, omitStructure, onlyCoreTranscription, extractsAtDocumentLevel, extractsAtPageLevel, rehyphenate)))
    })
    gfw.close()
  }
}