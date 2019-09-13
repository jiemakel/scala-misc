import java.io.File
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

import com.bizo.mighty.csv.CSVReader
import fi.hsci.lucene.NormalisationFilter
import org.apache.lucene.analysis.{FilteringTokenFilter, LowerCaseFilter}
import org.apache.lucene.analysis.core.WhitespaceTokenizer
import org.apache.lucene.analysis.miscellaneous.LengthFilter
import org.apache.lucene.analysis.pattern.PatternReplaceFilter
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, OffsetAttribute}
import org.apache.lucene.analysis.util.{CharTokenizer, UnicodeProps}
import org.apache.lucene.document.{NumericDocValuesField, SortedDocValuesField}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.apache.lucene.util.BytesRef
import org.rogach.scallop._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.language.{postfixOps, reflectiveCalls}
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._

object SKVRXMLIndexer extends OctavoIndexer {

  private def readContents(elem: String)(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvText(text) => content.append(text)
      case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
        case Some(chr) => content.append(chr)
        case _ => content.append(er.entity)
      }
      case EvComment(_) =>
      case EvElemStart(_,"I",_,_) => content.append('@')
      case EvElemEnd(_,"I") => content.append('@')
      case EvElemStart(_,"H",_,_) => content.append('$')
      case EvElemEnd(_,"H") => content.append('$')
      case EvElemStart(_,"SUP",_,_) => content.append('^')
      case EvElemEnd(_,"SUP") => content.append('^')
      case EvElemStart(_,"KA",_,_) => content.append('°')
      case EvElemEnd(_,"KA") => content.append('°')
      case EvElemStart(_,"SMALLCAPS",_,_) => content.append('¨')
      case EvElemEnd(_,"SMALLCAPS") => content.append('¨')
      case EvElemStart(_,"SUB",_,_) => content.append('ˇ')
      case EvElemEnd(_,"SUB") => content.append('ˇ')
      case EvElemStart(_,"FR",_,_) => content.append('€')
      case EvElemEnd(_,"FR") => content.append('€')
      case EvElemEnd(_,elem) => break = true
      case EvElemStart(_,nelem,_,_) => logger.warn("Encountered unknown element "+nelem)
    }
    content.toString
  }

  private val sentences = new AtomicLong

  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }

  class Reuse {
    val dd = new FluidDocument()
    val send = new FluidDocument()
    val skvrIDFields = new StringSDVFieldPair("skvrID").r(dd, send)
    val urlField = new SortedDocValuesField("skvrURL", new BytesRef())
    dd.addRequired(urlField)
    send.addRequired(urlField)
    val collectorFields = new StringSDVFieldPair("collector").r(dd, send)
    val regionFields = new StringSDVFieldPair("region").r(dd, send)
    val placeFields = new StringSDVFieldPair("place").r(dd, send)
    val yearFields = new IntPointNDVFieldPair("year").r(dd, send)
    val contentField = new ContentField("content",createAnalyser((_) => new CharTokenizer() {
      override def isTokenChar(c: Int): Boolean = !UnicodeProps.WHITESPACE.get(c) && c != '_'
    },
      (_,ins) => new FilteringTokenFilter(ins) {
        val oattr = addAttribute(classOf[OffsetAttribute])
        val cattr = addAttribute(classOf[CharTermAttribute])
        override def accept(): Boolean = {
          val s = cattr.toString
          !(oattr.startOffset == 0 && "^[0-9]*$".r.findFirstIn(s).isDefined) && "^#[0-9]*$".r.findFirstIn(s).isEmpty
        }
      },
      (_,ins) => new PatternReplaceFilter(ins,Pattern.compile("^\\p{Punct}*(.*?)\\p{Punct}*$"),"$1", false),
      (_,ins) => new PatternReplaceFilter(ins,Pattern.compile("#[0-9]*$"),"",false),
      (_,ins) => NormalisationFilter.tokenTransformer(ins,(content: String) => content.filter {
        case ']' | '[' | '@' | '$' | '^' | '°' | '¨' | 'ˇ' | '€' | '*' | '\'' => false
        case _ => true
      }),
      (_,ins) => new LowerCaseFilter(ins),
      (_,ins) => new NormalisationFilter(ins,true),
      (_,ins) => new LengthFilter(ins, 1, Int.MaxValue)
    )).r(dd,send)
    val notesFields = new TextSDVFieldPair("notes").r(dd, send)
    val lineIDField = new NumericDocValuesField("lineID", 0)
    send.addRequired(lineIDField)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength").r(dd, send)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens").r(dd, send)

    def clearMultiDocumentFields() {
      dd.clearOptional()
      send.clearOptional()
      /*
      dd.removeFields("themeID")
      send.removeFields("themeID")
      dd.removeFields("theme")
      send.removeFields("theme") */
    }
  }

  val termVectorFields = Seq("content")

  private def index(file: File): Unit = {
    logger.info("Processing: " + file)
    val s = Source.fromFile(file, "UTF-8")
    implicit val xml = new XMLEventReader(s)
    val verses = new ArrayBuffer[String]
    val r = tld.get
    val c = new StringBuilder()
    while (xml.hasNext) xml.next match {
      case EvElemStart(_, "ITEM", iattrs, _) =>
        verses.clear()
        var break = false
        val id = iattrs("nro").head.text
        val year = iattrs("y").head.text.toInt
        val placeId = iattrs("p").head.text.toInt
        val collectorId = iattrs("k").head.text.toInt
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_, "V", _, _) => verses.append(readContents("V"))
          case EvElemStart(_, "REFS", _, _) => r.notesFields.setValue(readContents("REFS"))
          case EvElemEnd(_, "ITEM") => break = true
          case _ =>
        }
        r.clearMultiDocumentFields()
        r.skvrIDFields.setValue(id)
        val (region, place) = places(placeId)
        r.yearFields.setValue(year)
        r.regionFields.setValue(region)
        r.placeFields.setValue(place)
        r.collectorFields.setValue(collectors(collectorId))
        for (pthemes <- poemThemes.get(id); themeID <- pthemes) {
          new StringSNDVFieldPair("themeID").o(r.dd, r.send).setValue(themeID)
          new TextSSDVFieldPair("theme").o(r.dd, r.send).setValue(themes(themeID))
        }
        r.urlField.setBytesValue(new BytesRef("https://skvr.fi/poem/" + id))
        val dcontents = new StringBuilder
        for (line <- verses) {
          r.lineIDField.setLongValue(sentences.incrementAndGet)
          r.contentField.setValue(line)
          r.contentLengthFields.setValue(line.length)
          r.contentTokensFields.setValue(getNumberOfTokens(line))
          seniw.addDocument(r.send)
          dcontents.append(line)
          dcontents.append('\n')
        }
        val dcontentsS = dcontents.toString
        r.contentField.setValue(dcontentsS)
        r.contentLengthFields.setValue(dcontentsS.length)
        r.contentTokensFields.setValue(getNumberOfTokens(dcontentsS))
        diw.addDocument(r.dd)
      case _ =>
    }
    logger.info("Processed: " + file)
  }

  var diw, seniw = null.asInstanceOf[IndexWriter]

  val ds = new Sort(new SortField("skvrID", SortField.Type.STRING))
  val sens = new Sort(new SortField("skvrID", SortField.Type.STRING), new SortField("lineID", SortField.Type.LONG))

  val themes = new mutable.HashMap[Int, String]
  val poemThemes = new mutable.HashMap[String, ArrayBuffer[Int]]
  val places = new mutable.HashMap[Int, (String, String)]
  val collectors = new mutable.HashMap[Int, String]

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val dpostings = opt[String](default = Some("blocktree"))
      val senpostings = opt[String](default = Some("blocktree"))
      val placeCsv = opt[String](required = true)
      val collectorCsv = opt[String](required = true)
      val themeCsv = opt[String](required = true)
      val themePoemsCsv = opt[String](required = true)
      verify()
    }
    for (row <- CSVReader(opts.placeCsv()))
      places.put(row(0).toInt, (row(1), row(2)))
    for (row <- CSVReader(opts.collectorCsv()))
      collectors.put(row(0).toInt, row(1))
    for (row <- CSVReader(opts.themeCsv()))
      themes.put(row(0).toInt, row(1))
    for (row <- CSVReader(opts.themePoemsCsv()))
      poemThemes.getOrElseUpdate(row(3), new ArrayBuffer[Int]) += row(0).toInt
    diw = iw(opts.index() + "/dindex", ds, opts.indexMemoryMb() / 2)
    seniw = iw(opts.index() + "/senindex", sens, opts.indexMemoryMb() / 2)
    feedAndProcessFedTasksInParallel(() => {
      opts.directories().toStream.flatMap(p => getFileTree(new File(p)))
        .filter(f => f.getName.endsWith(".xml") && "tyyppiluettelo.xml" != f.getName).foreach(file => addTask(file.getName, () => index(file)))
    })
    waitForTasks(
      runSequenceInOtherThread(
        () => close(diw),
        () => merge(opts.index() + "/dindex", ds, opts.indexMemoryMb() / 2, toCodec(opts.dpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(seniw),
        () => merge(opts.index() + "/senindex", sens, opts.indexMemoryMb() / 2, toCodec(opts.senpostings(), termVectorFields))
      )
    )
  }
}
