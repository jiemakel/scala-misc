import fi.seco.lexical.combined.CombinedLexicalAnalysisService
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService
import play.api.libs.json.Writes
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import scala.collection.JavaConverters._
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService.WordToResults
import org.apache.lucene.index.IndexWriter
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.File
import org.json4s._
import org.json4s.native.JsonParser._
import org.json4s.JsonDSL._
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

object YLEArticleIndexer extends OctavoIndexer {
  
/*  contentFieldType.setStoreTermVectorPositions(true)
  contentFieldType.setStoreTermVectorPayloads(true) */
  
  indexingCodec.termVectorFilter = new Predicate[BytesRef]() {
    override def test(b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }
  
  /*finalCodec.termVectorFilter = new Predicate[BytesRef]() {
    override def test(b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }*/
  contentFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
  
  case class Article(id: String, publisher: String, coverage: String, headline: String, lead: String, text: List[List[WordToAnalysis]])
  
  private val paragraphs = new AtomicLong
  
  class Reuse {
    val pd = new Document()
    val ad = new Document()
    val articleIDSField = new Field("articleID","",StringField.TYPE_NOT_STORED)
    pd.add(articleIDSField)
    ad.add(articleIDSField)
    val articleIDSDVField = new SortedDocValuesField("articleID", new BytesRef)
    pd.add(articleIDSDVField)
    ad.add(articleIDSDVField)
    val publisherSField = new Field("publisher", "", StringField.TYPE_NOT_STORED)
    pd.add(publisherSField)
    ad.add(publisherSField)
    val publisherSDVField = new SortedDocValuesField("publisher", new BytesRef)
    pd.add(publisherSDVField)
    ad.add(publisherSDVField)
    val coverageSDVField = new SortedDocValuesField("coverage", new BytesRef)
    pd.add(coverageSDVField)
    ad.add(coverageSDVField)
    val textField = new Field("text", "", contentFieldType)
    pd.add(textField)
    ad.add(textField)
    val paragraphIDSField = new Field("paragraphID","",StringField.TYPE_NOT_STORED)
    pd.add(paragraphIDSField)
    val paragraphIDNField = new NumericDocValuesField("paragraphID", 0)
    pd.add(paragraphIDNField)
  }
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  
  private def index(article: Article): Unit = {
    val d = tld.get
    d.articleIDSField.setStringValue(article.id)
    d.articleIDSDVField.setBytesValue(new BytesRef(article.id))
    d.publisherSField.setStringValue(article.publisher)
    d.publisherSDVField.setBytesValue(new BytesRef(article.publisher))
    
    val textTokens = new ArrayBuffer[Iterable[(Int, String, Iterable[(Float,Iterable[Iterable[String]])])]]
    var text = ""
    var textOffset = 0
    for (paragraph <- article.text) { 
      val ptext = paragraph.map(_.word).mkString("")      
      d.textField.setStringValue(ptext)
      val tokens = MorphologicalAnalyzer.scalaAnalysisToTokenStream(paragraph)
      d.textField.setTokenStream(new MorphologicalAnalysisTokenStream(tokens))
      if (!tokens.isEmpty) {
        text += ptext + "\n\n"
        textTokens += tokens.map(p => (textOffset + p._1, p._2, p._3))
        textOffset += tokens.last._1 + tokens.last._2.length // -2(w=) + 2(\n\n) = 0
      }
      val paragraphNum = paragraphs.getAndIncrement
      d.paragraphIDSField.setStringValue(""+paragraphNum)
      d.paragraphIDNField.setLongValue(paragraphNum)
      piw.addDocument(d.pd)
    }
    if (!text.isEmpty) {
      d.textField.setStringValue(text.substring(0, text.length -2))
      d.textField.setTokenStream(new MorphologicalAnalysisTokenStream(textTokens.flatten))
      aiw.addDocument(d.ad)
    }
  }
  
  var aiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var piw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  implicit val formats = DefaultFormats
 
  class Opts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val index = opt[String](default = Some("/srv/yle2017"))
    val indexMemoryMB = opt[Long](default = Some(Runtime.getRuntime.maxMemory()/1024/1024*3/4), validate = (0<))
    val directories = trailArg[List[String]](required = true)
    verify()
  }
  
  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    aiw = iw(opts.index()+"/aindex",new Sort(new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMB())
    clear(aiw)
    piw = iw(opts.index()+"/pindex",new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMB())
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toArray.flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".json")).forEach(file => {
        Try(parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
          val obj = ObjParser.parseObject(p)
          val article = new Article(
              (obj \ "id").asInstanceOf[JString].values,
              (obj \ "publisher" \ "name").asInstanceOf[JString].values,
              Try((obj \ "coverage").asInstanceOf[JString].values).getOrElse(""),
              Try((obj \ "headline" \ "full").asInstanceOf[JString].values).getOrElse(""),
              Try((obj \ "lead").asInstanceOf[JString].values).getOrElse(""),
              {
                val analyzedText = (obj \\ "analyzedText")
                val paragraphs = Try(analyzedText.asInstanceOf[JObject].children).orElse(Try(List(analyzedText.asInstanceOf[JArray]))).getOrElse(List.empty)
                paragraphs.map(_.asInstanceOf[JArray].children.map(_.extract[WordToAnalysis]))
              }
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
     (opts.index()+"/aindex", new Sort(new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMB()),
     (opts.index()+"/pindex", new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMB())))
  }
}
