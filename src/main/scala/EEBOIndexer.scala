import java.io.{File, PrintWriter}
import java.text.BreakIterator
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import org.apache.lucene.document.{Document, Field, NumericDocValuesField}

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._

object EEBOIndexer extends OctavoIndexer {

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
    val collectionIDFields = new StringSDVFieldPair("collectionID", dd, dpd, sd, pd, send)
    val documentIDFields = new StringSDVFieldPair("documentID", dd, dpd, sd, pd, send)
    val estcIDFields = new StringSDVFieldPair("ESTCID", dd, dpd, sd, pd, send)
    val dateStartFields = new IntPointNDVFieldPair("dateStart", dd, dpd, sd, pd, send)
    val dateEndFields = new IntPointNDVFieldPair("dateEnd", dd, dpd, sd, pd, send)
    val totalPagesFields = new IntPointNDVFieldPair("totalPages", dd, dpd, sd, pd, send)
    val languageFields = new StringSDVFieldPair("language", dd, dpd, sd, pd, send)
    val moduleFields = new StringSDVFieldPair("module", dd, dpd, sd, pd, send)
    val fullTitleFields = new TextSDVFieldPair("fullTitle",dd,dpd,sd,pd, send)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength", dd, dpd, sd, pd, send)
    val documentLengthFields = new IntPointNDVFieldPair("documentLength", dd, dpd, sd, pd, send)
    val totalParagraphsFields = new IntPointNDVFieldPair("totalParagraphs", dd, dpd, sd, pd, send)
    def clearOptionalDocumentFields() {
      dateStartFields.setValue(0)
      dateEndFields.setValue(Int.MaxValue)
      totalPagesFields.setValue(0)
      languageFields.setValue("")
      moduleFields.setValue("")
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
      case Some(chr) => chr.toString()
      case None => eeboMap.getOrElse(entity, entity)
    }
  }

  // see http://www.textcreationpartnership.org/docs/dox/cheat.html
  private val ignoredElems = HashSet("SUBST","CHOICE","SIC","FW","SUP","SUB","ABBR","Q", "MILESTONE","SEG","UNCLEAR","ABOVE","BELOW","DATE","BIBL","SALUTE","FIGDESC","ADD","REF","PTR")
  private val paragraphElems = HashSet("P","AB","CLOSER","SP","STAGE","TABLE","LG","TRAILER","OPENER","FIGURE","LETTER","ARGUMENT","DATELINE","SIGNED","EPIGRAPH","GROUP","TEXT","BYLINE","POSTSCRIPT")
  private val lineElems = HashSet("L","ROW","SPEAKER","LB")

  private def process(filePrefix: String, startDivLevel: Int, currentElem: String)(implicit xml: XMLEventReader): String = {
    val content = new StringBuilder()
    val divLevels = new ArrayBuffer[Int]()
    var currentDivLevel = startDivLevel
    divLevels += currentDivLevel
    var break = false
    var listDepth = 0
    var inTable = false
    var lastAddedNewLine = false
    while (xml.hasNext && !break) {
      var addedNewLine = false
      xml.next match {
        case EvElemEnd(_, `currentElem`) => break = true
        case EvElemStart(_, elem, _, _) if (ignoredElems.contains(elem)) =>
        case EvElemEnd(_, elem) if (ignoredElems.contains(elem)) =>
        case EvElemStart(_, elem, attrs,_) if (elem.startsWith("DIV")) => divLevels += currentDivLevel; currentDivLevel = elem.last.toString.toInt // TYPE, LANG, N
        case EvElemEnd(_, elem) if (elem.startsWith("DIV")) =>
          divLevels.trimEnd(1)
          currentDivLevel = divLevels.last
          addedNewLine = true
          if (content.length == 0 || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append('\n')
        case EvElemStart(_, "HEAD",_,_) => content.append("#" * currentDivLevel +" ")
        case EvElemEnd(_, "HEAD") =>
          addedNewLine = true
          if (content.length == 0 || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append('\n')
        case EvElemStart(_, "ARGUMENT",_,_) => content.append("## ")
        case EvElemStart(_, "FRONT",_,_) =>
        case EvElemEnd(_, "FRONT") =>
        case EvElemStart(_, "BODY",_,_) =>
        case EvElemEnd(_, "BODY") =>
        case EvElemStart(_, "BACK",_,_) =>
        case EvElemEnd(_, "BACK") =>
        case EvElemStart(_, "HI", _, _) => content.append("*")
        case EvElemEnd(_, "HI") => content.append("*")
        case EvElemStart(_, "PB", attrs, _) => //MS="y" REF="48" N="90"
        case EvElemEnd(_, "PB") =>
          addedNewLine = true
        case EvElemStart(_, elem, attrs, _) if (elem=="NOTE" || elem=="HEADNOTE" || elem=="TAILNOTE" || elem=="DEL" || elem=="CORR") =>
          val index = content.length - 1
          val note = process(filePrefix+"-"+elem+"-at-"+index,currentDivLevel, elem)
          val sw = new PrintWriter(new File(filePrefix+"-"+elem+"-at-"+index+".txt"))
          sw.append(note)
          sw.close()
        case EvElemStart(_, "GAP", attrs, _) => content.append(attrs.get("DISP").flatMap(_.headOption).map(_.text).getOrElse("〈?〉"))
        case EvElemEnd(_, "GAP") =>
        case EvElemStart(_, "LIST",_,_) => listDepth += 1
        case EvElemEnd(_, "LIST") =>
          listDepth -= 1
          addedNewLine = true
          if (content.length == 0 || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append('\n')
        case EvElemStart(_, elem,_,_) if (paragraphElems.contains(elem)) =>
          if (elem=="TABLE") inTable = true
          addedNewLine = true
          if (content.length > 0) {
            if (content.last != '\n') content.append("\n\n") else if (content.length < 2 || content.charAt(content.length - 2) != '\n') content.append('\n')
          }
        case EvElemEnd(_, elem) if (paragraphElems.contains(elem)) =>
          if (elem=="TABLE") inTable = false
          addedNewLine = true
          if (content.length == 0 || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append('\n')
        case EvElemStart(_, elem,_,_) if (lineElems.contains(elem)) =>
          addedNewLine = true
          if (content.length > 0 && content.last != '\n') content.append('\n')
        case EvElemEnd(_, elem) if (lineElems.contains(elem)) =>
          addedNewLine = true
          if (content.length == 0 || content.last != '\n') content.append('\n')
        case EvElemStart(_, "CELL",_,_) => content.append(" | ")
        case EvElemEnd(_, "CELL") => content.append(" | ")
        case EvElemStart(_, "ITEM",_,_) =>
          /* if (lastElemEnd == "LABEL") content.append(": ") else */ content.append(" * ")
        case EvElemEnd(_, "ITEM") =>
        case EvElemStart(_, "LABEL",_,_) => if (listDepth == 0) content.append("#" * (currentDivLevel + 1) +" ")
        case EvElemEnd(_, "LABEL") =>
          addedNewLine = true
          if (listDepth == 0) {
            if (content.length == 0 || content.last != '\n') content.append("\n\n") else if (content.length<2 || content.charAt(content.length - 2) != '\n') content.append("\n")
          } else if (content.length == 0 || content.last != '\n') content.append("\n")
        case EvText(text) => {
          val text2 = if (inTable) text.replaceAllLiterally("\n","") else if (lastAddedNewLine && text.head=='\n') text.tail else text
          content.append(text2.replaceAllLiterally("|","").replaceAllLiterally("∣",""))
        }
        case er: EvEntityRef => content.append(decodeEntity(er.entity))
        case EvComment(_) =>
        case ent => logger.warn("Unknown event in "+filePrefix+" : "+ent)
      }
      lastAddedNewLine = addedNewLine
    }
    content.toString.replaceAllLiterally(" |  | "," | ").replaceAll("\n\n+","\n\n").trim
  }


  def index(file: File): Unit = {
    implicit val xml = new XMLEventReader(Source.fromFile(file, "UTF-8"))
    var break = false
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_, "TEXT",_,_) => break = true
      case _ =>
    }
    val content = process(file.getAbsolutePath, 0, "TEXT")
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
    feedAndProcessFedTasksInParallel(() =>
      args.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".xml")).foreach(file => addTask(file.getPath, () => index(file)))
    )
  }
}
