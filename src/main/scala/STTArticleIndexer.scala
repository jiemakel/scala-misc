import java.io.{File, FileInputStream, InputStreamReader}
import java.util.concurrent.atomic.AtomicLong
import java.util.function.{BiPredicate, LongFunction}

import com.bizo.mighty.csv.CSVReader
import com.koloboke.collect.map.hash.HashLongObjMaps
import fi.seco.lucene.{MorphologicalAnalysisTokenStream, MorphologicalAnalyzer, WordToAnalysis}
import org.apache.lucene.document.{Field, StoredField}
import org.apache.lucene.index.{FieldInfo, IndexWriter}
import org.apache.lucene.search.{Sort, SortField}
import org.apache.lucene.util.BytesRef
import org.joda.time.format.ISODateTimeFormat
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods.{pretty, render}
import org.json4s.native.JsonParser._
import org.json4s.{DefaultFormats, JArray}
import org.rogach.scallop._

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.util.Try


object STTArticleIndexer extends OctavoIndexer {
  
  indexingCodec.termVectorFilter = new BiPredicate[FieldInfo,BytesRef]() {
    override def test(f: FieldInfo, b: BytesRef) = b.bytes(b.offset + 1) == 'L'
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
    //val urlFields = new StringSDVFieldPair("url").r(sd, pd, ad)
    val articleIDFields = new StringNDVFieldPair("articleID").r(sd, pd, ad)
    val publisherFields = new StringSDVFieldPair("publisher").r(sd, pd, ad)
    val timePublishedFields = new LongPointSDVDateTimeFieldPair("timePublished",ISODateTimeFormat.dateTimeNoMillis).r(sd,pd,ad)
    val timeModifiedFields = new LongPointSDVDateTimeFieldPair("timeModified",ISODateTimeFormat.dateTimeNoMillis).r(sd,pd,ad)

    val contentLengthFields = new IntPointNDVFieldPair("contentLength").r(sd, pd, ad)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens").r(sd, pd, ad)
    //val leadFields = new StringSDVFieldPair("lead").r(sd, pd, ad)
    val headlineFields = new TextSDVFieldPair("headline").r(sd, pd, ad)
    val creditlineFields = new TextSDVFieldPair("creditline").o(sd, pd, ad)
    val bylineFields = new TextSDVFieldPair("byline").o(sd, pd, ad)
    val versionFields = new StringSDVFieldPair("version").r(sd,pd,ad)
    val urgencyFields = new IntPointNDVFieldPair("urgency").r(sd,pd,ad)
    val genreFields = new StringSDVFieldPair("genre").r(sd,pd,ad)
    //mw.write(Seq("id","version","urgency","department","genre","timePublished","timeModified","headline","creditline","byline"))

    val textField = new Field("text", "", contentFieldType)
    sd.addRequired(textField)
    pd.addRequired(textField)
    ad.addRequired(textField)
    val analyzedTextField = new StoredField("analyzedText", "")
    sd.addRequired(analyzedTextField)
    pd.addRequired(analyzedTextField)
    ad.addRequired(analyzedTextField)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID").r(sd, pd)
    val sentenceIDFields = new StringNDVFieldPair("sentenceID").r(sd)
    val paragraphsFields = new IntPointNDVFieldPair("paragraphs").r(ad)
    val sentencesFields = new IntPointNDVFieldPair("sentences").r(pd, ad)
  }
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  implicit val formats = DefaultFormats

  private def index(file: File): Unit = {
    val wr = CSVReader(file.getPath)
    logger.info("Processing "+file)
    wr.next()
    for (r <- wr) {
      val id = Try(r(0).toLong).getOrElse(-1L)
      val fname = id+".xml"
      val analysisFile = new File(analyses+"/"+Math.abs(fname.hashCode()%10)+"/"+Math.abs(fname.hashCode()%100/10)+"/"+fname+".analysis.json")
      if (analysisFile.exists)
        addTask(""+id, () => index(id,r, analysisFile))
      else logger.error(analysisFile+" not found")
    }
  }
 
  private def index(id: Long, r: Seq[String], analysisFile: File): Unit = {
    val analyzedText = parse(new InputStreamReader(new FileInputStream(analysisFile))).asInstanceOf[JArray].children
    val d = tld.get
    d.ad.clearOptional()
    d.pd.clearOptional()
    d.sd.clearOptional()
    //mw.write(Seq("id","version","urgency","department","genre","timePublished","timeModified","headline","creditline","byline"))
    d.articleIDFields.setValue(id)
    d.versionFields.setValue(r(1))
    d.urgencyFields.setValue(r(2).toInt)
    d.publisherFields.setValue(r(3))
    d.genreFields.setValue(r(4))
    if (r(5).nonEmpty)
      d.timePublishedFields.setValue(r(5)+'Z')
    else d.timePublishedFields.setValue("0000-00-00T00:00Z")
    if (r(6).nonEmpty)
      d.timeModifiedFields.setValue(r(6)+'Z')
    else d.timeModifiedFields.setValue("0000-00-00T00:00Z")
    d.headlineFields.setValue(r(7)) // 7
    for (s <- Option(subjects.get(id)).getOrElse(Seq.empty))
      new StringSSDVFieldPair("subject").o(d.sd,d.pd,d.ad).setValue(s)
    if (r(8).nonEmpty)
      d.creditlineFields.setValue(r(8))
    if (r(9).nonEmpty)
      d.bylineFields.setValue(r(9))
    d.paragraphsFields.setValue(analyzedText.length)
    val textTokens = new ArrayBuffer[Iterable[(Int, String, Iterable[(Float,Iterable[Iterable[String]])])]]
    var text = ""
    var textOffset = 0
    var sentenceCount = 0
    for (paragraphAnalysis <- analyzedText) {
      val unwrappedAnalysis = paragraphAnalysis.asInstanceOf[JArray].children.map(_.extract[WordToAnalysis])
      val sentenceStartIndices = unwrappedAnalysis.zipWithIndex.filter(_._1.analysis.head.globalTags.exists(_.contains("FIRST_IN_SENTENCE"))).map(_._2)
      d.paragraphIDFields.setValue(paragraphs.getAndIncrement)
      val sentenceAnalyses = lsplit(paragraphAnalysis.asInstanceOf[JArray].children,sentenceStartIndices)
      val unwrappedSentenceAnalyses = lsplit(unwrappedAnalysis,sentenceStartIndices)
      var paragraphSentenceCount = 0
      for ((sentenceAnalysis,unwrappedSentenceAnalysis) <- sentenceAnalyses.zip(unwrappedSentenceAnalyses).filter(_._2.exists(!_.analysis.head.globalTags.contains("WHITESPACE")))) {
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
      if (tokens.nonEmpty) {
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
      d.analyzedTextField.setStringValue(pretty(render(analyzedText)))
      aiw.addDocument(d.ad)
    }
    logger.info("Successfully processed "+id)
  }
  
  var aiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var piw: IndexWriter = null.asInstanceOf[IndexWriter]
  var siw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  class HSOpts(arguments: Seq[String]) extends AOctavoOpts(arguments) {
    val analyses = opt[String](required = true)
    val subjects = opt[String](required = true)
    val apostings = opt[String](default = Some("fst"))
    val ppostings = opt[String](default = Some("fst"))
    val spostings = opt[String](default = Some("fst"))
    verify()
  }
  
  val as = new Sort(new SortField("articleID",SortField.Type.LONG))
  val ps = new Sort(new SortField("articleID",SortField.Type.LONG), new SortField("paragraphID", SortField.Type.LONG))
  val ss = new Sort(new SortField("articleID",SortField.Type.LONG), new SortField("paragraphID", SortField.Type.LONG), new SortField("sentenceID", SortField.Type.LONG))
  
  var analyses: String = _

  var subjects = HashLongObjMaps.newUpdatableMap[ArrayBuffer[String]]()
  
  def main(args: Array[String]): Unit = {
    val opts = new HSOpts(args)
    opts.subjects()
    for (r<-CSVReader(opts.subjects()))
      subjects.computeIfAbsent(r.head.toLong,new LongFunction[ArrayBuffer[String]] {
        override def apply(value: Long) = new ArrayBuffer[String]
      }) += r.last
    if (!opts.onlyMerge()) {
      analyses = opts.analyses()
      aiw = iw(opts.index()+"/aindex",as, opts.indexMemoryMb() / 3)
      piw = iw(opts.index()+"/pindex",ps, opts.indexMemoryMb() / 3)
      siw = iw(opts.index()+"/sindex",ss, opts.indexMemoryMb() / 3)
      feedAndProcessFedTasksInParallel(() =>
        opts.directories().toStream.flatMap(n => getFileTree(new File(n))).foreach(file => index(file))
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
