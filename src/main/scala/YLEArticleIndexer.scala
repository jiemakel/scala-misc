import java.io.{File, FileInputStream, InputStreamReader}
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiPredicate

import fi.seco.lucene.{MorphologicalAnalysisTokenStream, MorphologicalAnalyzer, WordToAnalysis}
import org.apache.lucene.document.{Field, LongPoint, SortedDocValuesField, StoredField}
import org.apache.lucene.index.{FieldInfo, IndexWriter}
import org.apache.lucene.search.{Sort, SortField}
import org.apache.lucene.util.BytesRef
import org.joda.time.format.ISODateTimeFormat
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods.{pretty, render}
import org.json4s.native.JsonParser._
import org.rogach.scallop._

import scala.collection.mutable.ArrayBuffer
import scala.language.{postfixOps, reflectiveCalls}


object YLEArticleIndexer extends OctavoIndexer {
  
/*  contentFieldType.setStoreTermVectorPositions(true)
  contentFieldType.setStoreTermVectorPayloads(true) */
  
  indexingCodec.termVectorFilter = new BiPredicate[FieldInfo,BytesRef]() {
    override def test(f: FieldInfo, b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }
  
  /*finalCodec.termVectorFilter = new Predicate[BytesRef]() {
    override def test(b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }*/
  
  case class Article(id: String, url: String, publisher: String, timePublished: String, coverage: String, headline: String, lead: String, analyzedText: List[JValue])
  
  private val paragraphs = new AtomicLong
  private val sentences = new AtomicLong
  
  class Reuse {
    val sd = new FluidDocument()
    val pd = new FluidDocument()
    val ad = new FluidDocument()
    val urlFields = new StringSDVFieldPair("url").r(sd, pd, ad)
    val articleIDFields = new StringSDVFieldPair("articleID").r(sd, pd, ad)
    val publisherFields = new StringSDVFieldPair("publisher").r(sd, pd, ad)
    val timePublishedSDVField = new SortedDocValuesField("timePublished", new BytesRef())
    sd.addRequired(timePublishedSDVField)
    pd.addRequired(timePublishedSDVField)
    ad.addRequired(timePublishedSDVField)
    val timePublishedLongPoint = new LongPoint("timePublished", 0)
    sd.addRequired(timePublishedLongPoint)
    pd.addRequired(timePublishedLongPoint)
    ad.addRequired(timePublishedLongPoint)
    val coverageFields = new StringSDVFieldPair("coverage").r(sd, pd, ad)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength").r(sd, pd, ad)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens").r(sd, pd, ad)
    val leadFields = new StringSDVFieldPair("lead").r(sd, pd, ad)
    val headlineFields = new StringSDVFieldPair("headline").r(sd, pd, ad)
    val textField = new Field("text", "", contentFieldType)
    sd.addRequired(textField)
    pd.addRequired(textField)
    ad.addRequired(textField)
    val analyzedTextField = new StoredField("analyzedText", "")
    sd.addRequired(analyzedTextField)
    pd.addRequired(analyzedTextField)
    ad.addRequired(analyzedTextField)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID").r(pd, sd)
    val sentenceIDFields = new StringNDVFieldPair("sentenceID").r(sd)
    val paragraphsFields = new IntPointNDVFieldPair("paragraphs").r(ad)
    val sentencesFields = new IntPointNDVFieldPair("sentences").r(pd, ad)
  }
  
  val tld = new java.lang.ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  implicit val formats = DefaultFormats
  
  private def index(file: File): Unit = {
    parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
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
          paragraphs
        )
      index(article)
      logger.info("File "+file+" processed.")
    })
  }
 
  private def index(article: Article): Unit = {
    val d = tld.get
    d.articleIDFields.setValue(article.id)
    d.urlFields.setValue(article.url)
    d.publisherFields.setValue(article.publisher)
    d.timePublishedSDVField.setBytesValue(new BytesRef(article.timePublished))
    d.timePublishedLongPoint.setLongValue(ISODateTimeFormat.dateTimeNoMillis.parseMillis(article.timePublished))
    d.coverageFields.setValue(article.coverage)
    d.headlineFields.setValue(article.headline)
    d.leadFields.setValue(article.lead)
    d.paragraphsFields.setValue(article.analyzedText.length)
    val textTokens = new ArrayBuffer[Iterable[(Int, String, Iterable[(Float,Iterable[Iterable[String]])])]]
    var text = ""
    var textOffset = 0
    var sentenceCount = 0
    for (paragraphAnalysis <- article.analyzedText) {
      val unwrappedAnalysis = paragraphAnalysis.asInstanceOf[JArray].children.map(_.extract[WordToAnalysis])
      val sentenceStartIndices = unwrappedAnalysis.zipWithIndex.filter(_._1.analysis(0).globalTags.exists(_.contains("FIRST_IN_SENTENCE"))).map(_._2)
      d.paragraphIDFields.setValue(paragraphs.getAndIncrement)
      val sentenceAnalyses = lsplit(paragraphAnalysis.asInstanceOf[JArray].children,sentenceStartIndices)
      val unwrappedSentenceAnalyses = lsplit(unwrappedAnalysis,sentenceStartIndices)
      var paragraphSentenceCount = 0
      for ((sentenceAnalysis,unwrappedSentenceAnalysis) <- sentenceAnalyses.zip(unwrappedSentenceAnalyses).filter(_._2.exists(!_.analysis(0).globalTags.contains("WHITESPACE")))) {
        paragraphSentenceCount += 1
        d.sentenceIDFields.setValue(sentences.getAndIncrement)
        val tokens = MorphologicalAnalyzer.scalaAnalysisToTokenStream(unwrappedSentenceAnalysis)
        val sentenceText = unwrappedSentenceAnalysis.map(_.word).mkString("").trim
        d.textField.setStringValue(sentenceText)
        d.contentLengthFields.setValue(sentenceText.length)
        d.contentTokensFields.setValue(unwrappedSentenceAnalysis.length)
        d.textField.setTokenStream(new MorphologicalAnalysisTokenStream(tokens))
        d.analyzedTextField.setStringValue(pretty(render(sentenceAnalysis)))
        siw.addDocument(d.sd)
      }
      val tokens = MorphologicalAnalyzer.scalaAnalysisToTokenStream(unwrappedAnalysis)
      val paragraphText = unwrappedAnalysis.map(_.word).mkString("")
      d.sentencesFields.setValue(paragraphSentenceCount)
      sentenceCount += paragraphSentenceCount
      d.textField.setStringValue(paragraphText)
      d.contentLengthFields.setValue(paragraphText.length)
      d.contentTokensFields.setValue(unwrappedAnalysis.length)
      d.textField.setTokenStream(new MorphologicalAnalysisTokenStream(tokens))
      d.analyzedTextField.setStringValue(pretty(render(paragraphAnalysis)))
      piw.addDocument(d.pd)
      if (!tokens.isEmpty) {
        text += paragraphText + "\n\n"
        textTokens += tokens.map(p => (textOffset + p._1, p._2, p._3))
        textOffset += tokens.last._1 + tokens.last._2.length // -2(w=) + 2(\n\n) = 0
      }
    }
    if (!text.isEmpty) {
      d.sentencesFields.setValue(sentenceCount)
      d.textField.setStringValue(text.substring(0, text.length -2))
      d.contentLengthFields.setValue(text.length - 2)
      val flattenedTokens = textTokens.flatten
      d.contentTokensFields.setValue(flattenedTokens.length)
      d.textField.setTokenStream(new MorphologicalAnalysisTokenStream(flattenedTokens))
      d.analyzedTextField.setStringValue(pretty(render(article.analyzedText)))
      aiw.addDocument(d.ad)
    }
  }
  
  var siw: IndexWriter = null.asInstanceOf[IndexWriter]
  var aiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var piw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  val as = new Sort(new SortField("articleID",SortField.Type.STRING))
  val ps = new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG))
  val ss = new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG), new SortField("sentenceID", SortField.Type.LONG))
  
  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val apostings = opt[String](default = Some("fst"))
      val ppostings = opt[String](default = Some("fst"))
      val spostings = opt[String](default = Some("fst"))
      verify()
    }
    if (!opts.onlyMerge()) {
      aiw = iw(opts.index()+"/aindex",as,opts.indexMemoryMb() / 3)
      piw = iw(opts.index()+"/pindex",ps,opts.indexMemoryMb() / 3)
      siw = iw(opts.index()+"/sindex",ss,opts.indexMemoryMb() / 3)
      feedAndProcessFedTasksInParallel(() =>
        opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".json")).foreach(file => addTask(file.getName, () => index(file)))
      )
    }
    val termVectorFields = Seq("text")
    waitForTasks(
      runSequenceInOtherThread(
        () => close(aiw), 
        () => merge(opts.index()+"/aindex", as,opts.indexMemoryMb() / 3, toCodec(opts.apostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(piw), 
        () => merge(opts.index()+"/pindex", ps,opts.indexMemoryMb() / 3, toCodec(opts.ppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(siw), 
        () => merge(opts.index()+"/sindex", ss,opts.indexMemoryMb() / 3, toCodec(opts.spostings(), termVectorFields))
      )
    )
  }
}
