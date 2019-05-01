import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.bizo.mighty.csv.CSVReader
import org.apache.lucene.document.{Field, NumericDocValuesField, SortedDocValuesField}
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

object SKVRIndexer extends OctavoIndexer {

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
    val contentField = new Field("content","",contentFieldType)
    dd.addRequired(contentField)
    send.addRequired(contentField)
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

  private def trimSpace(value: String): String = {
    if (value==null) return null
    var len = value.length
    var st = 0
    while (st < len && (value(st) == ' ' || value(st) == '\n')) st += 1
    while (st < len && (value(len - 1) == ' ' || value(len -1) == '\n')) len -= 1
    if ((st > 0) || (len < value.length)) value.substring(st, len)
    else value
  }

  private def index(file: File): Unit = {
    logger.info("Processing: "+file)
    val r = tld.get
    r.clearMultiDocumentFields()
    val did = file.getName.substring(file.getName.lastIndexOf('/')+1,file.getName.lastIndexOf('.'))
    r.skvrIDFields.setValue(did)
    val (year,placeId,collectorId) = metadata(did)
    val (region,place) = places(placeId)
    r.yearFields.setValue(year)
    r.regionFields.setValue(region)
    r.placeFields.setValue(place)
    r.collectorFields.setValue(collectors(collectorId))
    for (pthemes <- poemThemes.get(did);themeID <- pthemes) {
      new StringSNDVFieldPair("themeID").o(r.dd,r.send).setValue(themeID)
      new TextSSDVFieldPair("theme").o(r.dd,r.send).setValue(themes(themeID))
    }
    r.urlField.setBytesValue(new BytesRef("https://skvr.fi/poem/"+did))
    val fl = Source.fromFile(file)
    val dcontents = new StringBuilder
    for (line <- fl.getLines) {
      r.lineIDField.setLongValue(sentences.incrementAndGet)
      r.contentField.setStringValue(line)
      r.contentLengthFields.setValue(line.length)
      r.contentTokensFields.setValue(getNumberOfTokens(line))
      seniw.addDocument(r.send)
      dcontents.append(line)
      dcontents.append('\n')
    }
    fl.close()
    val dcontentsS = dcontents.toString
    r.contentField.setStringValue(dcontentsS)
    r.contentLengthFields.setValue(dcontentsS.length)
    r.contentTokensFields.setValue(getNumberOfTokens(dcontentsS))
    diw.addDocument(r.dd)
    logger.info("Processed: "+file)
  }

  var diw, seniw = null.asInstanceOf[IndexWriter]

  val ds = new Sort(new SortField("skvrID",SortField.Type.STRING))
  val sens = new Sort(new SortField("skvrID",SortField.Type.STRING), new SortField("lineID", SortField.Type.LONG))

  val themes = new mutable.HashMap[Int,String]
  val poemThemes = new mutable.HashMap[String,ArrayBuffer[Int]]
  val places = new mutable.HashMap[Int,(String,String)]
  val collectors = new mutable.HashMap[Int,String]
  val metadata = new mutable.HashMap[String,(Int,Int,Int)]

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val dpostings = opt[String](default = Some("blocktree"))
      val senpostings = opt[String](default = Some("blocktree"))
      val metadataCsv = opt[String](required = true)
      val placeCsv = opt[String](required = true)
      val collectorCsv = opt[String](required = true)
      val themeCsv = opt[String](required = true)
      val themePoemsCsv = opt[String](required = true)
      verify()
    }
    for (row <- CSVReader(opts.metadataCsv()))
      metadata.put(row(0),(row(1).toInt,row(2).toInt,row(3).toInt))
    for (row <- CSVReader(opts.placeCsv()))
      places.put(row(0).toInt,(row(1),row(2)))
    for (row <- CSVReader(opts.collectorCsv()))
      collectors.put(row(0).toInt,row(1))
    for (row <- CSVReader(opts.themeCsv()))
      themes.put(row(0).toInt,row(1))
    for (row <- CSVReader(opts.themePoemsCsv()))
      poemThemes.getOrElseUpdate(row(3), new ArrayBuffer[Int]) += row(0).toInt
    diw = iw(opts.index()+"/dindex", ds, opts.indexMemoryMb()/2)
    seniw = iw(opts.index()+"/senindex", sens, opts.indexMemoryMb()/2)
    feedAndProcessFedTasksInParallel(() => {
      opts.directories().toStream.flatMap(p => getFileTree(new File(p)))
      .filter(_.getName.endsWith(".txt")).foreach(file => addTask(file.getName, () => index(file)))
    })
    waitForTasks(
      runSequenceInOtherThread(
        () => close(diw),
        () => merge(opts.index()+"/dindex", ds, opts.indexMemoryMb()/2, toCodec(opts.dpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(seniw),
        () => merge(opts.index()+"/senindex", sens, opts.indexMemoryMb()/2, toCodec(opts.senpostings(), termVectorFields))
      )
    )
  }
}
