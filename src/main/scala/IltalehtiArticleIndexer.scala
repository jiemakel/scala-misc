import java.io.{File, FileInputStream, InputStreamReader}
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiPredicate

import fi.seco.lucene.{MorphologicalAnalysisTokenStream, MorphologicalAnalyzer, WordToAnalysis}
import org.apache.lucene.document.StoredField
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


object IltalehtiArticleIndexer extends OctavoIndexer {
  
/*  contentFieldType.setStoreTermVectorPositions(true)
  contentFieldType.setStoreTermVectorPayloads(true) */
  
  indexingCodec.termVectorFilter = new BiPredicate[FieldInfo,BytesRef]() {
    override def test(f: FieldInfo, b: BytesRef) = f.name=="lead" || f.name=="headline" || b.bytes(b.offset + 1) == 'L'
  }
  
  /*finalCodec.termVectorFilter = new Predicate[BytesRef]() {
    override def test(b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }*/
  
  private val paragraphs = new AtomicLong
  private val sentences = new AtomicLong
  
  class Reuse {
    val sd = new FluidDocument()
    val pd = new FluidDocument()
    val ad = new FluidDocument()
    val urlFields = new StringSDVFieldPair("url").r(sd, pd, ad)
    val articleIDFields = new StringSDVFieldPair("articleID").r(sd, pd, ad)
    val publisherFields = new StringSDVFieldPair("publisher").r(sd, pd, ad)
    val timePublishedFields = new LongPointSDVDateTimeFieldPair("timePublished",ISODateTimeFormat.dateTimeNoMillis).r(sd, pd, ad)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength").r(sd, pd, ad)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens").r(sd, pd, ad)
    val aleadField = new TextSDVTVFieldPair("alead").r(ad)
    val nleadField = new TextSDVTVFieldPair("lead").r(ad,pd,sd)
    val aheadlineField = new TextSDVTVFieldPair("aheadline").r(ad)
    val nheadlineField = new TextSDVTVFieldPair("headline").r(ad,pd,sd)
    val textField = new  TextSDVTVFieldPair("text").r(sd, pd, ad)
    val authorFields = new TextSSDVFieldPair("author").o(sd, pd, ad)
    val newsValueFields = new IntPointNDVFieldPair("newsValue").r(sd, pd, ad)
    val analyzedTextField = new StoredField("analyzedText", new BytesRef())
    sd.addRequired(analyzedTextField)
    pd.addRequired(analyzedTextField)
    ad.addRequired(analyzedTextField)
    val analyzedHeadlineField = new StoredField("analyzedHeadline", new BytesRef())
    ad.addRequired(analyzedHeadlineField)
    val analyzedLeadField = new StoredField("analyzedLead", new BytesRef())
    ad.addRequired(analyzedLeadField)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID").r(pd, sd)
    val sentenceIDFields = new StringNDVFieldPair("sentenceID").r(sd)
    val paragraphsFields = new IntPointNDVFieldPair("paragraphs").r(ad)
    val sentencesFields = new IntPointNDVFieldPair("sentences").r(pd, ad)
    def clearOptionalArticleFields(): Unit = {
      ad.clearOptional()
      sd.clearOptional()
      pd.clearOptional()
    }
  }
  
  val tld = new java.lang.ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  implicit val formats = DefaultFormats
  
  private def index(file: File): Unit = {
    parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
      index(ObjParser.parseObject(p))
      logger.info("File "+file+" processed.")
    })
  }

  private def processAnalysis(analyzedText: JArray, f: TextSDVTVFieldPair, f2: TextSDVTVFieldPair, s: StoredField): Unit = {
    val unwrappedAnalysis = analyzedText.children.map(_.extract[WordToAnalysis])
    val tokens = MorphologicalAnalyzer.scalaAnalysisToTokenStream(unwrappedAnalysis)
    val text = unwrappedAnalysis.map(_.word).mkString("")
    f.setValue(text,new MorphologicalAnalysisTokenStream(tokens))
    f2.setValue(text)
    s.setBytesValue(new BytesRef(pretty(render(analyzedText))))
  }
 
  private def index(a: JValue): Unit = {
    val d = tld.get
    d.clearOptionalArticleFields()
    val id = (a \ "article_id").asInstanceOf[JString].values
    d.articleIDFields.setValue(id)
    d.urlFields.setValue("https://www.iltalehti.fi/digi/a/"+id)
    d.publisherFields.setValue((a \ "category").asInstanceOf[JString].values)
    d.timePublishedFields.setValue((a \ "published_at").asInstanceOf[JString].values)
    d.newsValueFields.setValue((a \ "news_value").asInstanceOf[JInt].values.intValue)
    a \ "authors" match {
      case JString(authors) if authors.nonEmpty =>
        for (author <- authors.split(",")) new TextSSDVFieldPair("author").o(d.sd, d.pd, d.ad).setValue(author)
      case _ =>
    }
    val analyzedLead = (a \\ "analyzedLead").asInstanceOf[JArray]
    processAnalysis(analyzedLead,d.aleadField,d.nleadField,d.analyzedLeadField)
    val analyzedTitle = (a \\ "analyzedTitle").asInstanceOf[JArray]
    processAnalysis(analyzedTitle,d.aheadlineField,d.nheadlineField,d.analyzedHeadlineField)
    val analyzedText = (a \\ "analyzedBody")
    val unwrappedAnalysis = analyzedText.asInstanceOf[JArray].children.map(_.extract[WordToAnalysis])
    d.paragraphsFields.setValue(1)
    //d.paragraphsFields.setValue(aparagraphs.length)
    val textTokens = new ArrayBuffer[Iterable[(Int, String, Iterable[(Float,Iterable[Iterable[String]])])]]
    var text = ""
    var textOffset = 0
    var sentenceCount = 0
    val sentenceStartIndices = unwrappedAnalysis.zipWithIndex.filter(_._1.analysis(0).globalTags.exists(_.contains("FIRST_IN_SENTENCE"))).map(_._2)
    d.paragraphIDFields.setValue(paragraphs.getAndIncrement)
    val sentenceAnalyses = lsplit(analyzedText.asInstanceOf[JArray].children,sentenceStartIndices)
    val unwrappedSentenceAnalyses = lsplit(unwrappedAnalysis,sentenceStartIndices)
    var paragraphSentenceCount = 0
    for ((sentenceAnalysis,unwrappedSentenceAnalysis) <- sentenceAnalyses.zip(unwrappedSentenceAnalyses).filter(_._2.exists(!_.analysis(0).globalTags.contains("WHITESPACE")))) {
      paragraphSentenceCount += 1
      d.sentenceIDFields.setValue(sentences.getAndIncrement)
      val tokens = MorphologicalAnalyzer.scalaAnalysisToTokenStream(unwrappedSentenceAnalysis)
      val sentenceText = unwrappedSentenceAnalysis.map(_.word).mkString("").trim
      d.textField.setValue(sentenceText,new MorphologicalAnalysisTokenStream(tokens))
      d.contentLengthFields.setValue(sentenceText.length)
      d.contentTokensFields.setValue(unwrappedSentenceAnalysis.length)
      d.analyzedTextField.setBytesValue(new BytesRef(pretty(render(sentenceAnalysis))))
      siw.addDocument(d.sd)
    }
    val tokens = MorphologicalAnalyzer.scalaAnalysisToTokenStream(unwrappedAnalysis)
    val paragraphText = unwrappedAnalysis.map(_.word).mkString("")
    d.sentencesFields.setValue(paragraphSentenceCount)
    sentenceCount += paragraphSentenceCount
    d.textField.setValue(paragraphText,new MorphologicalAnalysisTokenStream(tokens))
    d.contentLengthFields.setValue(paragraphText.length)
    d.contentTokensFields.setValue(unwrappedAnalysis.length)
    d.analyzedTextField.setBytesValue(new BytesRef(pretty(render(analyzedText))))
    piw.addDocument(d.pd)
    if (tokens.nonEmpty) {
      text += paragraphText + "\n\n"
      textTokens += tokens.map(p => (textOffset + p._1, p._2, p._3))
      textOffset += tokens.last._1 + tokens.last._2.length // -2(w=) + 2(\n\n) = 0
    }
    if (!text.isEmpty) {
      d.sentencesFields.setValue(sentenceCount)
      d.contentLengthFields.setValue(text.length - 2)
      val flattenedTokens = textTokens.flatten
      d.contentTokensFields.setValue(flattenedTokens.length)
      d.textField.setValue(text.substring(0, text.length -2),new MorphologicalAnalysisTokenStream(flattenedTokens))
      d.analyzedTextField.setBytesValue(new BytesRef(pretty(render(analyzedText))))
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
        opts.directories().to(LazyList).flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".json")).foreach(file => addTask(file.getName, () => index(file)))
      )
    }
    val termVectorFields = Seq("text","headline","lead","aheadline","alead")
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
