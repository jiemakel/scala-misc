import java.io.{File, PrintWriter}
import java.text.BreakIterator
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import ECCOIndexer._
import org.apache.lucene.document.{Document, Field, NumericDocValuesField}

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._

object EEBOIndexer extends OctavoIndexer {

  private def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_,_) => return null
      case EvText(text) => content.append(text)
      case er: EvEntityRef => content.append(decodeEntity(er.entity))
      case EvComment(comment) if (comment == " unknown entity apos; ") => content.append('\'')
      case EvComment(comment) if (comment.startsWith(" unknown entity")) =>
        val entity = content.substring(16, content.length - 2)
        content.append(decodeEntity(entity))
      case EvComment(comment) =>
        logger.debug("Encountered comment: "+comment)
      case EvElemEnd(_,_) => break = true
    }
    content.toString.trim
  }

  private val sentences = new AtomicLong
  private val paragraphs = new AtomicLong
  private val sections = new AtomicLong
  private val documentparts = new AtomicLong

  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }

  class Reuse {
    val sbi = BreakIterator.getSentenceInstance(new Locale("en_GB"))
    val dd = new Document()
    val dpd = new Document()
    val sd = new Document()
    val pd = new Document()
    val send = new Document()
    val documentIDFields = new StringSDVFieldPair("documentID", dd, dpd, sd, pd, send)
    val estcIDFields = new StringSDVFieldPair("ESTCID", dd, dpd, sd, pd, send)
    val totalPagesFields = new IntPointNDVFieldPair("totalPages", dd, dpd, sd, pd, send)
    val languageFields = new StringSDVFieldPair("language", dd, dpd, sd, pd, send)
    val fullTitleFields = new TextSDVFieldPair("fullTitle",dd,dpd,sd,pd, send)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength", dd, dpd, sd, pd, send)
    val documentLengthFields = new IntPointNDVFieldPair("documentLength", dd, dpd, sd, pd, send)
    val totalParagraphsFields = new IntPointNDVFieldPair("totalParagraphs", dd, dpd, sd, pd, send)
    def clearOptionalDocumentFields() {
      totalPagesFields.setValue(0)
      languageFields.setValue("")
      fullTitleFields.setValue("")
      dd.removeFields("containsGraphicOfType")
      dd.removeFields("containsGraphicCaption")
    }
    def clearOptionalDocumentPartFields() {
      dpd.removeFields("containsGraphicOfType")
      dpd.removeFields("containsGraphicCaption")
    }
    def clearOptionalParagraphFields() {
      pd.removeFields("sectionID")
      pd.removeFields("headingLevel")
      pd.removeFields("heading")
    }
    def clearOptionalSentenceFields() {
      send.removeFields("sectionID")
      send.removeFields("headingLevel")
      send.removeFields("heading")
    }
    val documentPartIdFields = new StringNDVFieldPair("partID", dpd, sd, pd)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID", pd, send)
    val sentenceIDField = new NumericDocValuesField("sentenceID", 0)
    send.add(sentenceIDField)
    val contentField = new Field("content","",contentFieldType)
    dd.add(contentField)
    dpd.add(contentField)
    sd.add(contentField)
    pd.add(contentField)
    send.add(contentField)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens", dd, dpd, sd, pd, send)
    val documentPartTypeFields = new StringSDVFieldPair("documentPartType", dpd, sd, pd, send)
    val sectionIDFields = new StringNDVFieldPair("sectionID", sd)
    val headingFields = new TextSDVFieldPair("heading",sd)
    val headingLevelFields = new IntPointNDVFieldPair("headingLevel", sd)
  }

  val termVectorFields = Seq("content", "containsGraphicOfType")

  class SectionInfo(val sectionID: Long, val headingLevel: Int, val heading: String) {
    val paragraphSectionIDFields = new StringSNDVFieldPair("sectionID")
    paragraphSectionIDFields.setValue(sectionID)
    val paragraphHeadingLevelFields = new IntPointSNDVFieldPair("headingLevel")
    paragraphHeadingLevelFields.setValue(headingLevel)
    val paragraphHeadingFields = new TextSSDVFieldPair("heading")
    val content = new StringBuilder()
    def addToDocument(pd: Document) {
      paragraphSectionIDFields.add(pd)
      paragraphHeadingLevelFields.add(pd)
      paragraphHeadingFields.add(pd)
    }
  }

   private val eeboMap = new mutable.HashMap[String,String]()

  private def decodeEntity(entity: String): String = {
    XhtmlEntities.entMap.get(entity) match {
      case Some(chr) => chr.toString
      case None =>
        eeboMap.get(entity) match {
          case Some(chr2) => chr2
          case None =>
            logger.warn("Encountered unknown entity "+entity)
            '['+entity+']'
        }
    }
  }

  // see http://www.textcreationpartnership.org/docs/dox/cheat.html
  private val ignoredElems = HashSet("SUBST","CHOICE","SIC","FW","SUP","SUB","ABBR","Q", "MILESTONE","SEG","UNCLEAR","ABOVE","BELOW","DATE","BIBL","SALUTE","FIGDESC","ADD","REF","PTR")
  private val paragraphElems = HashSet("P","AB","CLOSER","SP","STAGE","TABLE","LG","TRAILER","OPENER","LETTER","ARGUMENT","DATELINE","SIGNED","EPIGRAPH","GROUP","TEXT","BYLINE","POSTSCRIPT")
  private val lineElems = HashSet("L","ROW","SPEAKER","LB")

  private def process(idPrefix: String, currentElem: String, r: Reuse)(implicit xml: XMLEventReader): String = {
    val content = new StringBuilder()
    val divLevels = new ArrayBuffer[Int]()
    var currentDivLevel = 0
    divLevels += currentDivLevel
    var break = false
    var listDepth = 0
    var inTable = false
    var lastAddedNewLine = false
    while (xml.hasNext && !break) {
      var addedNewLine = false
      xml.next match {
        case EvElemEnd(_, `currentElem`) => break = true
        case EvElemStart(_, elem, _, _) if ignoredElems.contains(elem) =>
        case EvElemEnd(_, elem) if ignoredElems.contains(elem) =>
        case EvElemStart(_, elem, _,_) if elem.startsWith("DIV") => divLevels += currentDivLevel; currentDivLevel = elem.last.toString.toInt // TYPE, LANG, N
        case EvElemEnd(_, elem) if elem.startsWith("DIV") =>
          divLevels.trimEnd(1)
          currentDivLevel = divLevels.last
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append('\n')
        case EvElemStart(_, "HEAD",_,_) => content.append("#" * currentDivLevel +" ")
        case EvElemEnd(_, "HEAD") =>
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append('\n')
        case EvElemStart(_, "ARGUMENT",_,_) => content.append("## ")
        case EvElemStart(_, "FRONT",_,_) =>
        case EvElemEnd(_, "FRONT") =>
        case EvElemStart(_, "BODY",_,_) =>
        case EvElemEnd(_, "BODY") =>
        case EvElemStart(_, "BACK",_,_) =>
        case EvElemEnd(_, "BACK") =>
        case EvElemStart(_, "HI", _, _) => content.append("*")
        case EvElemEnd(_, "HI") => content.append("*")
        case EvElemStart(_, "PB", _, _) => //MS="y" REF="48" N="90"
        case EvElemEnd(_, "PB") =>
          addedNewLine = true
        case EvElemStart(_, "FIGURE", _, _) =>
          val index = content.length - 1
          val note = process(idPrefix+"-FIGURE-at-"+index, "FIGURE", r)
          val sw = new PrintWriter(new File(idPrefix+"-FIGURE-at-"+index+".txt"))
          sw.append(note)
          sw.close()
        case EvElemStart(_, elem, _, _) if elem == "NOTE" || elem == "HEADNOTE" || elem == "TAILNOTE" || elem == "DEL" || elem == "CORR" =>
          val index = content.length - 1
          val note = process(idPrefix+"-"+elem+"-at-"+index, elem, r)
          val sw = new PrintWriter(new File(idPrefix+"-"+elem+"-at-"+index+".txt"))
          sw.append(note)
          sw.close()
        case EvElemStart(_, "GAP", attrs, _) => content.append(attrs.get("DISP").flatMap(_.headOption).map(_.text).getOrElse("〈?〉"))
        case EvElemEnd(_, "GAP") =>
        case EvElemStart(_, "LIST",_,_) => listDepth += 1
        case EvElemEnd(_, "LIST") =>
          listDepth -= 1
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append('\n')
        case EvElemStart(_, elem,_,_) if paragraphElems.contains(elem) =>
          if (elem=="TABLE") inTable = true
          addedNewLine = true
          if (content.nonEmpty) {
            if (content.last != '\n') content.append("\n\n") else if (content.length < 2 || content.charAt(content.length - 2) != '\n') content.append('\n')
          }
        case EvElemEnd(_, elem) if paragraphElems.contains(elem) =>
          if (elem=="TABLE") inTable = false
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append('\n')
        case EvElemStart(_, elem,_,_) if lineElems.contains(elem) =>
          addedNewLine = true
          if (content.nonEmpty && content.last != '\n') content.append('\n')
        case EvElemEnd(_, elem) if lineElems.contains(elem) =>
          addedNewLine = true
          if (content.isEmpty || content.last != '\n') content.append('\n')
        case EvElemStart(_, "CELL",_,_) => content.append(" | ")
        case EvElemEnd(_, "CELL") => content.append(" | ")
        case EvElemStart(_, "ITEM",_,_) =>
          /* if (lastElemEnd == "LABEL") content.append(": ") else */ content.append(" * ")
        case EvElemEnd(_, "ITEM") =>
        case EvElemStart(_, "LABEL",_,_) => if (listDepth == 0) content.append("#" * (currentDivLevel + 1) +" ")
        case EvElemEnd(_, "LABEL") =>
          addedNewLine = true
          if (listDepth == 0) {
            if (content.isEmpty || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append("\n")
          } else if (content.isEmpty || content.last != '\n') content.append("\n")
        case EvText(text) =>
          val text2 = if (inTable) text.replaceAllLiterally("\n","") else if (lastAddedNewLine && text.head=='\n') text.tail else text
          content.append(text2.replaceAllLiterally("|","").replaceAllLiterally("∣",""))
        case er: EvEntityRef => content.append(decodeEntity(er.entity))
        case EvComment(_) =>
        case ent => logger.warn("Unknown event in "+idPrefix+" : "+ent)
      }
      lastAddedNewLine = addedNewLine
    }
    content.toString.replaceAllLiterally(" |  | "," | ").replaceAll("\n\n+","\n\n").trim
  }

  var metadataDirectory: String = _

  def index(file: File): Unit = {
    val r = tld.get
    r.clearOptionalDocumentFields()
    val metadataXML = new XMLEventReader(Source.fromFile(metadataDirectory+"/"+file.getName.replace(".xml",".hdr"), "UTF-8"))
    while (metadataXML.hasNext) metadataXML.next match {
      case EvElemStart(_, "TITLE",_,_) => r.fullTitleFields.setValue(readContents(metadataXML))
      case EvElemStart(_, "IDNO",attrs,_) if attrs("TYPE").head.text == "stc" =>
        val idno = readContents(metadataXML)
        if (idno.startsWith("ESTC ")) r.estcIDFields.setValue(idno.substring(5))
      case EvElemStart(_, "LANGUSAGE",attr,_) => r.languageFields.setValue(attr("ID").head.text)
      case _ =>
    }
    metadataXML.stop()
    val xml = new XMLEventReader(Source.fromFile(file, "UTF-8"))
    var break = false
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_, "TEXT",_,_) => break = true
      case _ =>
    }
    val content = process(file.getAbsolutePath, "TEXT", r)(xml)
    val cw = new PrintWriter(new File(file.getAbsolutePath+".txt"))
    cw.append(content)
    cw.close()
    logger.info("Successfully processed "+file)
  }

  def main(args: Array[String]): Unit = {
    val lr = "<!ENTITY (.*?) \"(.*?)\"".r
    val lr2 = "&#h(.*?);".r
    for (line <- Source.fromInputStream(getClass.getResourceAsStream("Eebochar-xml-all-view.ent")).getLines; m <- lr.findFirstMatchIn(line)) {
      val entity = m.group(1)
      val replacement = lr2.replaceAllIn(m.group(2), m => Integer.valueOf(m.group(1), 16).toChar.toString)
      eeboMap += entity -> replacement
    }
    val opts = new AOctavoOpts(args) {
      val dpostings = opt[String](default = Some("blocktree"))
      val dppostings = opt[String](default = Some("blocktree"))
      val spostings = opt[String](default = Some("blocktree"))
      val ppostings = opt[String](default = Some("fst"))
      val senpostings = opt[String](default = Some("fst"))
      val metadataDirectory = opt[String](required = true)
      verify()
    }
    metadataDirectory = opts.metadataDirectory()
    if (!opts.onlyMerge()) {
      // document level
      diw = iw(opts.index()+"/dindex", ds, opts.indexMemoryMb()/5)
      // document part level
      dpiw = iw(opts.index()+"/dpindex", dps, opts.indexMemoryMb()/5)
      // section level
      siw = iw(opts.index()+"/sindex", ss, opts.indexMemoryMb()/5)
      // paragraph level
      piw = iw(opts.index()+"/pindex", ps, opts.indexMemoryMb()/5)
      // sentence level
      seniw = iw(opts.index()+"/senindex", sens, opts.indexMemoryMb()/5)
      feedAndProcessFedTasksInParallel(() =>
        opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".xml")).foreach(file =>
          addTask(file.getPath, () => index(file)))
      )
    }
    waitForTasks(
      runSequenceInOtherThread(
        () => close(diw),
        () => merge(opts.index()+"/dindex", ds, opts.indexMemoryMb()/5, toCodec(opts.dpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(dpiw),
        () => merge(opts.index()+"/dpindex", dps, opts.indexMemoryMb()/5, toCodec(opts.dppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(siw),
        () => merge(opts.index()+"/sindex", ss, opts.indexMemoryMb()/5, toCodec(opts.spostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(piw),
        () => merge(opts.index()+"/pindex", ps, opts.indexMemoryMb()/5, toCodec(opts.ppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(seniw),
        () => merge(opts.index()+"/senindex", sens, opts.indexMemoryMb()/5, toCodec(opts.senpostings(), termVectorFields))
      )
    )  }
}
