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


object YLEArticleIndexer extends OctavoIndexer {
  
/*  contentFieldType.setStoreTermVectorPositions(true)
  contentFieldType.setStoreTermVectorPayloads(true) */
  
  indexingCodec.termVectorFilter = new BiPredicate[FieldInfo,BytesRef]() {
    override def test(f: FieldInfo, b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }
  
  /*finalCodec.termVectorFilter = new Predicate[BytesRef]() {
    override def test(b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }*/
  contentFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
  
  case class Article(id: String, url: String, publisher: String, timePublished: String, coverage: String, headline: String, lead: String, text: List[String], analyzedText: List[JValue])
  
  private val paragraphs = new AtomicLong
  
  class Reuse {
    val pd = new Document()
    val ad = new Document()
    val urlFields = new StringSDVFieldPair("url", pd, ad)
    val articleIDFields = new StringSDVFieldPair("articleID", pd, ad)
    val publisherFields = new StringSDVFieldPair("publisher", pd, ad)
    val timePublishedSField = new StoredField("timePublished", "")
    pd.add(timePublishedSField)
    ad.add(timePublishedSField)
    val timePublishedLongPoint = new LongPoint("timePublished", 0)
    pd.add(timePublishedLongPoint)
    ad.add(timePublishedLongPoint)
    val coverageFields = new StringSDVFieldPair("coverage", pd, ad)
    /*
    val leadField = new Field("lead", "", contentFieldType)
    pd.add(leadField)
    ad.add(leadField)
    val headlineField = new Field("headline", "", contentFieldType)
    pd.add(headlineField)
    ad.add(headlineField)*/
    val leadFields = new StringSDVFieldPair("lead", pd, ad)
    val headlineFields = new StringSDVFieldPair("headline", pd, ad)
    val textField = new Field("text", "", contentFieldType)
    pd.add(textField)
    ad.add(textField)
    val analyzedTextField = new StoredField("analyzedText", "")
    pd.add(analyzedTextField)
    ad.add(analyzedTextField)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID", pd)
  }
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  implicit val formats = DefaultFormats
 
  private def index(article: Article): Unit = {
    val d = tld.get
    d.articleIDFields.setValue(article.id)
    d.publisherFields.setValue(article.publisher)
    d.timePublishedSField.setStringValue(article.timePublished)
    d.timePublishedLongPoint.setLongValue(ISODateTimeFormat.dateTimeNoMillis.parseMillis(article.timePublished))
    d.coverageFields.setValue(article.coverage)
    d.headlineFields.setValue(article.headline)
    d.leadFields.setValue(article.lead)
    val textTokens = new ArrayBuffer[Iterable[(Int, String, Iterable[(Float,Iterable[Iterable[String]])])]]
    var text = ""
    var textOffset = 0
    for ((paragraphText,paragraphAnalysis) <- article.text.zip(article.analyzedText)) { 
      val tokens = MorphologicalAnalyzer.scalaAnalysisToTokenStream(paragraphAnalysis.asInstanceOf[JArray].children.map(_.extract[WordToAnalysis]))
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
  
  def main(args: Array[String]): Unit = {
    val opts = new OctavoOpts(args)
    aiw = iw(opts.index()+"/aindex",new Sort(new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMb() / 2)
    piw = iw(opts.index()+"/pindex",new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMb() / 2)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toArray.flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".json")).forEach(file => {
        Try(parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
          val obj = ObjParser.parseObject(p)
          val analyzedText = (obj \\ "analyzedText")
          val paragraphs = if (analyzedText.isInstanceOf[JObject]) analyzedText.asInstanceOf[JObject].children else if (analyzedText.isInstanceOf[JArray]) List(analyzedText.asInstanceOf[JArray]) else List.empty
          val article = new Article(
              (obj \ "id").asInstanceOf[JString].values,
              (obj \ "url" \ "full").asInstanceOf[JString].values,
              (obj \ "publisher" \ "name").asInstanceOf[JString].values,
              (obj \ "datePublished").asInstanceOf[JString].values,
              if ((obj \ "coverage").isInstanceOf[JString]) (obj \ "coverage").asInstanceOf[JString].values else "",
              if ((obj \ "headline" \ "full").isInstanceOf[JString]) (obj \ "headline" \ "full").asInstanceOf[JString].values else "",
              if ((obj \ "lead").isInstanceOf[JString]) (obj \ "lead").asInstanceOf[JString].values else "",
              if ((obj \\ "text").isInstanceOf[JObject]) (obj \\ "text").asInstanceOf[JObject].children.map(_.asInstanceOf[JString].values) else if ((obj \\ "text").isInstanceOf[JString]) List((obj \\ "text").asInstanceOf[JString].values) else List.empty,
              paragraphs
            )
            addTask(file.getName,() => index(article))
      })) match {
            case Success(_) => 
              logger.info("File "+file+" processed.")
            case Failure(e) => logger.error("Error processing file "+file, e)
          }}))
    close(aiw)
    close(piw)
    mergeIndices(Seq(
     (opts.index()+"/aindex", new Sort(new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMb() / 2),
     (opts.index()+"/pindex", new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMb() / 2)))
  }
}
