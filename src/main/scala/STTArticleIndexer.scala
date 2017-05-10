import fi.seco.lexical.combined.CombinedLexicalAnalysisService
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService
import scala.collection.JavaConverters._
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService.WordToResults
import org.apache.lucene.index.IndexWriter
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.File
import org.json4s._
import org.json4s.native.JsonParser._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods.{render, pretty}
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.StreamConverters._
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField

import org.rogach.scallop._
import scala.language.postfixOps
import org.apache.lucene.document.StringField
import org.apache.lucene.document.Field
import org.apache.lucene.document.Document
import org.apache.lucene.document.IntPoint
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.SortedDocValuesField
import org.apache.lucene.util.BytesRef
import org.apache.lucene.analysis.TokenStream
import java.lang.ThreadLocal
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute
import org.apache.lucene.util.NumericUtils
import scala.collection.mutable.HashSet
import org.apache.lucene.index.IndexOptions
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.util.function.Predicate
import java.util.concurrent.atomic.AtomicLong
import fi.seco.lucene.MorphologicalAnalysisTokenStream
import fi.seco.lucene.MorphologicalAnalyzer
import fi.seco.lucene.WordToAnalysis
import org.joda.time.format.ISODateTimeFormat
import org.apache.lucene.document.LongPoint
import org.apache.lucene.document.StoredField
import java.util.function.BiPredicate
import org.apache.lucene.index.FieldInfo
import scala.xml.pull.XMLEventReader
import scala.io.Source
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvText
import scala.xml.pull.EvElemEnd
import scala.xml.pull.EvEntityRef
import scala.xml.parsing.XhtmlEntities


object STTArticleIndexer extends OctavoIndexer {
  
/*  contentFieldType.setStoreTermVectorPositions(true)
  contentFieldType.setStoreTermVectorPayloads(true) */
  
  indexingCodec.termVectorFilter = new BiPredicate[FieldInfo,BytesRef]() {
    override def test(f: FieldInfo, b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }
  
  /*finalCodec.termVectorFilter = new Predicate[BytesRef]() {
    override def test(b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }*/
  contentFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
  
  case class Article(
      id: String, 
      genre: Int, 
      department: Int,
      timePublished: String, 
      topics: Seq[Int],
      geoTopics: Seq[Int],
      categories: Seq[Int],
      headline: String,
      analyzedText: List[JValue])
  
  private val paragraphs = new AtomicLong
  
  class Reuse {
    val pd = new Document()
    val ad = new Document()
    val articleIDFields = new StringSDVFieldPair("articleID", pd, ad)
    val genreFields = new StringNDVFieldPair("genre", pd, ad)
    val departmentFields = new StringNDVFieldPair("department", pd, ad)
    val timePublishedSField = new StoredField("timePublished", "")
    pd.add(timePublishedSField)
    ad.add(timePublishedSField)
    val timePublishedLongPoint = new LongPoint("timePublished", 0)
    pd.add(timePublishedLongPoint)
    ad.add(timePublishedLongPoint)
    val headlineFields = new StringSDVFieldPair("headline", pd, ad)
    val textField = new Field("text", "", contentFieldType)
    pd.add(textField)
    ad.add(textField)
    val analyzedTextField = new StoredField("analyzedText", "")
    pd.add(analyzedTextField)
    ad.add(analyzedTextField)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID", pd)
    def clearOptionalDocumentFields() {
      pd.removeFields("topic")
      ad.removeFields("topic")
      pd.removeFields("category")
      ad.removeFields("category")
    }
  }
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  implicit val formats = DefaultFormats
 
  private def index(article: Article): Unit = {
    val d = tld.get
    d.articleIDFields.setValue(article.id)
    d.genreFields.setValue(article.genre)
    d.departmentFields.setValue(article.department)
    d.timePublishedSField.setStringValue(article.timePublished)
    d.timePublishedLongPoint.setLongValue(ISODateTimeFormat.dateTimeNoMillis.parseMillis(article.timePublished))
    d.headlineFields.setValue(article.headline)
    d.clearOptionalDocumentFields()
    for (topic <- article.topics) {
      val f = new StringSNDVFieldPair("topic", d.ad, d.pd)
      f.setValue(topic)
    }
    for (category <- article.categories) {
      val f = new StringSNDVFieldPair("category", d.ad, d.pd)
      f.setValue(category)
    }
    val textTokens = new ArrayBuffer[Iterable[(Int, String, Iterable[(Float,Iterable[Iterable[String]])])]]
    var text = ""
    var textOffset = 0
    for (paragraphAnalysis <- article.analyzedText) { 
      val unwrappedAnalysis = paragraphAnalysis.asInstanceOf[JArray].children.map(_.extract[WordToAnalysis])
      val tokens = MorphologicalAnalyzer.scalaAnalysisToTokenStream(unwrappedAnalysis)
      val paragraphText = unwrappedAnalysis.map(_.word).mkString("")
      d.textField.setStringValue(paragraphText)
      d.textField.setTokenStream(new MorphologicalAnalysisTokenStream(tokens))
      d.analyzedTextField.setStringValue(pretty(render(paragraphAnalysis)))
      d.paragraphIDFields.setValue(paragraphs.getAndIncrement)
      piw.addDocument(d.pd)
      if (!tokens.isEmpty) {
        text += paragraphText + "\n\n"
        textTokens += tokens.map(p => (textOffset + p._1, p._2, p._3))
        textOffset += tokens.last._1 + tokens.last._2.length // -2(w=) + 2(\n\n) = 0
      }
    }
    if (!text.isEmpty) {
      d.textField.setStringValue(text.substring(0, text.length -2))
      d.textField.setTokenStream(new MorphologicalAnalysisTokenStream(textTokens.flatten))
      d.analyzedTextField.setStringValue(pretty(render(article.analyzedText)))
      aiw.addDocument(d.ad)
    }
  }
  
  var aiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var piw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  class STTOpts(arguments: Seq[String]) extends AOctavoOpts(arguments) {
    val xml = opt[String](required = true)
    verify()
  }
  
  def main(args: Array[String]): Unit = {
    val opts = new STTOpts(args)
    val xmlPath = opts.xml()
    aiw = iw(opts.index()+"/aindex",new Sort(new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMb() / 2)
    piw = iw(opts.index()+"/pindex",new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMb() / 2)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toArray.flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".analysis.json")).forEach(file => Try({
        val analyzedText = parse(new InputStreamReader(new FileInputStream(file))).asInstanceOf[JArray].children
        val xml = new XMLEventReader(Source.fromFile(new File(xmlPath+"/"+file.getName.substring(0,file.getName.length - ".analysis.json".length)), "UTF-8"))
        var id: String = file.getName.substring(0, file.getName.length - ".xml.analysis.json".length)
        var genre = 0
        var department = 0
        var timePublished = ""
        val headline = new StringBuilder
        val topics = new ArrayBuffer[Int]
        val geoTopics = new ArrayBuffer[Int]
        val categories = new ArrayBuffer[Int]
        var break = false
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_,"genre",attrs,_) =>
            val qcode = attrs("qcode")(0).text
            if (qcode.startsWith("sttgenre:")) genre = qcode.substring("sttgenre:".length).toInt
          case EvElemStart(_,"subject",attrs,_) =>
            val sType = attrs("type")(0).text
            val qcode = attrs("qcode")(0).text
            sType match {
              case "cpnat:abstract" => topics += qcode.substring("stt-topics:".length).toInt
              case "cpnat:department" => department = qcode.substring("sttdepartment:".length).toInt
              case "cpnat:geoArea" => geoTopics += qcode.substring("sttlocationalias:".length).toInt
            }
          case EvElemStart(_,"contentModified",_,_) =>
            timePublished = xml.next.asInstanceOf[EvText].text
          case EvElemStart(_,"headline",_,_) =>
            var break = false
            while (xml.hasNext & !break) xml.next match {
              case EvText(text) => headline.append(text)
              case er: EvEntityRef => 
                headline.append(XhtmlEntities.entMap.get(er.entity) match {
                  case Some(chr) => chr
                  case _ => er.entity
                })
              case EvElemEnd(_,"headline") => break = true
            }
          case EvElemEnd(_,"contentMeta") => break = true
          case _ => 
        }
        val article = Article(
            id,
            genre,
            department,
            timePublished,
            topics,
            geoTopics,
            categories,
            headline.toString,
            analyzedText)
        addTask(file.getName,() => index(article))
      }) match {
            case Success(_) => 
              logger.info("File "+file+" processed.")
            case Failure(e) => logger.error("Error processing file "+file, e)
          }))
    close(aiw)
    close(piw)
    mergeIndices(Seq(
     (opts.index()+"/aindex", new Sort(new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMb() / 2),
     (opts.index()+"/pindex", new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMb() / 2)))
  }
}
