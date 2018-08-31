import java.io.{File, PrintWriter}

import ECCOXML2Text.attrsToString
import org.rogach.scallop.ScallopConf

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.Utility
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._


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
    val currentElem: String,
   ) {
    val outputFilePrefix = dest + "/" + this.currentFilePrefix.substring(prefixLength)
    val outputRootPrefix = dest + "/" + this.currentRootPrefix.substring(prefixLength)
    def child(prefix: String, elem: String): ProcessOpts = new ProcessOpts(
      this.currentFilePrefix + "_" + prefix,
      this.currentRootPrefix,
      this.dest,
      this.prefixLength,
      this.inlineNotes,
      this.omitEmphases,
      this.omitStructure,
      elem)
  }

  private class PageTracker(val pbw: PrintWriter, val dcontent: StringBuilder) {
    var lastPage: String = "-1"
  }

  private def process(opts: ProcessOpts)(implicit xml: XMLEventReader, pt: PageTracker): String = {
    val pcontent = new StringBuilder()
    val scontent = new StringBuilder()
    val content = new StringBuilder()
    implicit val contents = Array(pcontent, scontent, content)
    val divLevels = new ArrayBuffer[Int]()
    var currentDivLevel = 0
    divLevels += currentDivLevel
    var break = false
    var listDepth = 0
    var inTable = false
    var lastAddedNewLine = false
    var dpart = 1
    var curPage = 0
    while (xml.hasNext && !break) {
      var addedNewLine = false
      xml.next match {
        case EvElemEnd(_, opts.currentElem) => break = true
        case EvElemStart(_, elem, _, _) if ignoredElems.contains(elem) =>
        case EvElemEnd(_, elem) if ignoredElems.contains(elem) =>
        case EvElemStart(_, elem, _,_) if elem.startsWith("DIV") => divLevels += currentDivLevel; currentDivLevel = elem.last.toString.toInt // TYPE, LANG, N
        case EvElemEnd(_, elem) if elem.startsWith("DIV") =>
          divLevels.trimEnd(1)
          currentDivLevel = divLevels.last
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
        case EvElemStart(_, "HEAD",_,_) => if (!opts.omitStructure) append("#" * currentDivLevel +" ")
        case EvElemEnd(_, "HEAD") =>
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
        case EvElemStart(_, "ARGUMENT",_,_) => if (!opts.omitStructure) append("## ")
        case EvElemStart(_, elem,_,_) if elem == "FRONT" || elem == "BACK" || elem == "TEXT" || elem == "BODY" =>
          val scc = clean(scontent)
          if (scc.nonEmpty) {
            val sw = new PrintWriter(new File(opts.outputFilePrefix + "_"+ dpart +"_pre"+elem.toLowerCase+".txt"))
            sw.append(scc)
            sw.close()
            dpart += 1
            content.append("\n\n")
          }
          scontent.clear()
          val cpo = opts.child(dpart+"_"+elem.toLowerCase, elem)
          val cc = process(cpo)
          if (cc.nonEmpty) {
            val sw = new PrintWriter(new File(cpo.outputFilePrefix + ".txt"))
            sw.append(cc)
            sw.close()
            content.append(cc)
            content.append("\n\n")
            dpart += 1
          }
        case EvElemStart(_, "FIGDESC", _, _) => append("〈 image: ")
        case EvElemEnd(_, "FIGDESC") => append(" 〉")
        case EvElemStart(_, "HI", _, _) => if (!opts.omitEmphases) append("*")
        case EvElemEnd(_, "HI") => if (!opts.omitEmphases) append("*")
        case EvElemStart(_, "PB", attrs, _) => //MS="y" REF="48" N="90"
          val pid: String = attrs("REF").headOption.map(_.text).getOrElse("-1")
          if (pid!=pt.lastPage) {
            val bb = clean(pt.dcontent.toString + content.toString)
            println(bb.length, pid, bb)
            val offset = bb.length
            pt.pbw.append("" + offset + ',' + pid + '\n')
            pt.lastPage = pid
          }
        case EvElemEnd(_, "PB") =>
          val pcc = clean(pcontent)
          if (pcc.nonEmpty) {
            val pw = new PrintWriter(new File(opts.outputRootPrefix + "_page"+curPage+".txt"))
            pw.append(pcc)
            pw.close()
          }
          curPage += 1
          addedNewLine = true
        case EvElemStart(_, elem, _, _) if elem == "NOTE" || elem == "HEADNOTE" || elem == "TAILNOTE" || elem == "DEL" || elem == "CORR" =>
          if (!opts.inlineNotes) {
            val index = content.length - 1
            val cpo = opts.child(elem.toLowerCase+"_at_"+ index, elem)
            val note = process(cpo)
            val sw = new PrintWriter(cpo.outputFilePrefix + ".txt")
            sw.append(note)
            sw.close()
          }
        case EvElemEnd(_, elem) if opts.inlineNotes && (elem == "NOTE" || elem == "HEADNOTE" || elem == "TAILNOTE" || elem == "DEL" || elem == "CORR") =>
        case EvElemStart(_, "GAP", attrs, _) => append(attrs.get("DISP").flatMap(_.headOption).map(_.text).getOrElse("〈?〉"))
        case EvElemEnd(_, "GAP") =>
        case EvElemStart(_, "LIST",_,_) => listDepth += 1
        case EvElemEnd(_, "LIST") =>
          listDepth -= 1
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
        case EvElemStart(_, elem,_,_) if paragraphElems.contains(elem) =>
          if (elem=="TABLE") inTable = true
          addedNewLine = true
          if (content.nonEmpty) {
            if (content.last != '\n') append("\n\n") else if (content.length < 2 || content.charAt(content.length - 2) != '\n') append("\n")
          }
        case EvElemEnd(_, elem) if paragraphElems.contains(elem) =>
          if (elem=="TABLE") inTable = false
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
        case EvElemStart(_, elem,_,_) if lineElems.contains(elem) =>
          addedNewLine = true
          if (content.nonEmpty && content.last != '\n') append("\n")
        case EvElemEnd(_, elem) if lineElems.contains(elem) =>
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') append("\n")
        case EvElemStart(_, "CELL",_,_) => if (!opts.omitStructure) append(" | ")
        case EvElemEnd(_, "CELL") => if (!opts.omitStructure) append(" | ")
        case EvElemStart(_, "ITEM",_,_) =>
          /* if (lastElemEnd == "LABEL") append(": ") else */ if (!opts.omitStructure) append(" * ")
        case EvElemEnd(_, "ITEM") =>
        case EvElemStart(_, "LABEL",_,_) => if (listDepth == 0 && !opts.omitStructure) append("#" * (currentDivLevel + 1) +" ")
        case EvElemEnd(_, "LABEL") =>
          addedNewLine = true
          if (listDepth == 0) {
            if (content.isEmpty || content.last != '\n') append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') append("\n")
          } else if (content.isEmpty || content.last != '\n') append("\n")
        case EvText(text) =>
          val text2 = if (inTable) text.replaceAllLiterally("\n","") else if (lastAddedNewLine && text.head=='\n') text.tail else text
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


  def index(file: File, dest: String, prefixLength: Int, inlineNotes: Boolean, omitEmphases: Boolean, omitStructure: Boolean): Unit = {
    val opts = new ProcessOpts(file.getAbsolutePath.replace(".xml",""),file.getAbsolutePath.replace(".xml",""), dest, prefixLength, inlineNotes, omitEmphases, omitStructure, null)
    implicit val xml = new XMLEventReader(Source.fromFile(file, "UTF-8"))
    val dcontent = new StringBuilder()
    val pbw = new PrintWriter(new File(opts.outputFilePrefix+"_pbs.csv"))
    implicit val pt = new PageTracker(pbw,dcontent)
    var text = 1
    new File(dest + file.getParentFile.getAbsolutePath.substring(prefixLength)).mkdirs()
    var indent = ""
    val metadata = new StringBuilder()
    var lastWasText = false
    while (xml.hasNext) xml.next match {
      case EvElemStart(_, "HEADER", _, _) =>
        var break = false
        while (!break) xml.next match {
          case EvElemEnd(_, "HEADER") => break = true
          case EvElemStart(_, label, attrs, _) =>
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
      case EvElemStart(_, "TEXT",_,_) =>
        val copts = opts.child(text+"_text", "TEXT")
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
    pbw.close()
    logger.info("Successfully processed "+file)
  }


  
  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val directories = trailArg[List[String]](required = true)
      val dest = opt[String](required = true)
      val inlineNotes = opt[Boolean]()
      val omitStructure = opt[Boolean]()
      val omitEmphases = opt[Boolean]()
      verify()
    }
    val inlineNotes = opts.inlineNotes()
    val omitStructure = opts.omitStructure()
    val omitEmphases = opts.omitStructure() || opts.omitEmphases()
    val dest = opts.dest()
    new File(dest).mkdirs()
    val lr = "<!ENTITY (.*?) \"(.*?)\"".r
    val lr2 = "&#h(.*?);".r
    for (line <- Source.fromInputStream(getClass.getResourceAsStream("Eebochar-xml-all-view.ent")).getLines; m <- lr.findFirstMatchIn(line)) {
      val entity = m.group(1)
      val replacement = lr2.replaceAllIn(m.group(2), m => Integer.valueOf(m.group(1), 16).toChar.toString)
      eeboMap += entity -> replacement
    }
    feedAndProcessFedTasksInParallel(() => {
      for (dir <- opts.directories(); f = new File(dir); prefixLength = f.getAbsolutePath.length)
        getFileTree(f).filter(_.getName.endsWith(".xml")).foreach(file => addTask(file.getPath, () => index(file,
          dest, prefixLength, inlineNotes, omitEmphases, omitStructure)))
    })
  }
}