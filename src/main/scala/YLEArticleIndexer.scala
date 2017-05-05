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
import org.joda.time.format.ISODateTimeFormat
import org.apache.lucene.document.LongPoint

object YLEArticleIndexer extends OctavoIndexer {
  
/*  contentFieldType.setStoreTermVectorPositions(true)
  contentFieldType.setStoreTermVectorPayloads(true) */
  
/*  indexingCodec.termVectorFilter = new Predicate[BytesRef]() {
    override def test(b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  } */ 
  
  finalCodec.termVectorFilter = new Predicate[BytesRef]() {
    override def test(b: BytesRef) = b.bytes(b.offset + 1) == 'L'
  }
  //contentFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
  
  case class Article(id: String, url: String, publisher: String, timestamp: Long, coverage: String, headline: String, lead: String, text: List[List[WordToAnalysis]])
  
  private val paragraphs = new AtomicLong
  
  class Reuse {
    val pd = new Document()
    val ad = new Document()
    val urlFields = new StringSDVFieldPair("url", pd, ad)
    val articleIDFields = new StringSDVFieldPair("articleID", pd, ad)
    val publisherFields = new StringSDVFieldPair("publisher", pd, ad)
    val timeFields = new LongPointNDVFieldPair("timePublished", pd, ad) 
    val coverageFields = new StringSDVFieldPair("coverage", pd, ad)
    val leadField = new Field("lead", "", contentFieldType)
    pd.add(leadField)
    ad.add(leadField)
    val headlineField = new Field("headline", "", contentFieldType)
    pd.add(headlineField)
    ad.add(headlineField)
    val textField = new Field("text", "", contentFieldType)
    pd.add(textField)
    ad.add(textField)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID", pd, ad)
  }
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  def filterGlobalTag(tag: String): Boolean = tag match {
    case "WHITESPACE" => true
    case "POS_MATCH" => true
    case "BEST_MATCH" => true
    case "BASEFORM_FREQUENCY" => true
    case "FIRST_IN_SENTENCE" => true
    case _ => false
  }

  def filterTag(tag: String): Boolean = tag match {
    case "BASEFORM_FREQUENCY" => true
    case "SEGMENT" => true
    case _ => false
  }


  private class LemmaTokenStream(text: List[WordToAnalysis]) extends TokenStream {
    
    private val termAttr: CharTermAttribute = addAttribute(classOf[CharTermAttribute])
    val lemmas = new ArrayBuffer[String]
    
    for (word <- text; if !(word.analysis(0).globalTags.exists(_.contains("WHITESPACE"))); analysis <- word.analysis)
      lemmas += (if (word.analysis(0).globalTags.exists(_.contains("BEST_MATCH"))) "B" else "O") + analysis.wordParts.map(_.lemma).mkString("")
    
    var ci: Iterator[String] = lemmas.iterator
     
    final override def incrementToken(): Boolean = {
      if (!ci.hasNext) return false
      termAttr.append(ci.next)
      return true
    }
  }

  private class AnalysisTokenStream(tokens: Iterable[Iterable[String]]) extends TokenStream {
    
    private val termAttr: CharTermAttribute = addAttribute(classOf[CharTermAttribute])
    private val posAttr: PositionIncrementAttribute = addAttribute(classOf[PositionIncrementAttribute])
    //private val offAttr: OffsetAttribute = addAttribute(classOf[OffsetAttribute])
    //private val plAttr: PayloadAttribute = addAttribute(classOf[PayloadAttribute])
        
    //offset = 0
    
    var iterator: Iterator[Iterable[String]] = null
    
    override def reset(): Unit = {
      iterator = tokens.iterator
    }
    
    var ci: Iterator[String] = Iterator.empty
     
    final override def incrementToken(): Boolean = {
      val t = if (!ci.hasNext) {
        if (!iterator.hasNext) return false
        /*val (wsoffset, ctokens) = iterator.next
        offset += wsoffset
        ci = ctokens.iterator*/
        ci = iterator.next.iterator
        posAttr.setPositionIncrement(1)
        val t = ci.next
        /*offAttr.setOffset(offset, offset + t.length)
        offset += t.length*/
        t
      } else {
        posAttr.setPositionIncrement(0)
        ci.next
      }
      /*val payload = new BytesRef(8)
      NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(30.1), payload.bytes, 0)
      plAttr.setPayload(payload)*/
      termAttr.setEmpty()
      termAttr.append(t)
      return true
    }
  }
  
  private def index(article: Article): Unit = {
    val d = tld.get
    d.articleIDFields.setValue(article.id)
    d.publisherFields.setValue(article.publisher)
    d.timeFields.setValue(article.timestamp)
    d.coverageFields.setValue(article.coverage)
    d.headlineField.setStringValue(article.headline)
    d.leadField.setStringValue(article.lead)
    val textTokens = new ArrayBuffer[Iterable[Iterable[String]]]
    var text = ""
    //var offset = 0
    for (paragraph <- article.text) { 
      val ptext = paragraph.map(_.word).mkString("")
      text += ptext + "\n\n"
      d.textField.setStringValue(ptext)
      val tokens = for (word <- paragraph; if !(word.analysis(0).globalTags.exists(_.contains("WHITESPACE")))) yield {
  /*      if (word.analysis(0).globalTags.exists(_.contains("WHITESPACE"))) offset += word.word.length 
        else { */
          val ctokens = new HashSet[String]
          ctokens += "W="+word.word
          if (word.analysis(0).globalTags.contains("FIRST_IN_SENTENCE")) ctokens += "FIRST_IN_SENTENCE=TRUE"
          for (analysis <- word.analysis) {
            val prefix = if (analysis.globalTags.exists(_.contains("BEST_MATCH"))) "B" else "O"
            for (globalTags <- analysis.globalTags; (tag, tagValues) <- globalTags.toSeq; if !filterGlobalTag(tag); tagValue <- tagValues) ctokens += prefix + tag+"="+tagValue
            var lemma = ""
            for (wordPart <- analysis.wordParts) {
              lemma += wordPart.lemma
              for (tags <- wordPart.tags; (tag, tagValues) <- tags; if !filterTag(tag); tagValue <- tagValues) ctokens += prefix + tag+"="+tagValue
            }
            ctokens += prefix + "L="+lemma
          }
          ctokens
          /*tokens += ((offset, ctokens))
          offset = 0
        }*/
      }
      textTokens += tokens
      d.textField.setTokenStream(new AnalysisTokenStream(tokens))
      d.paragraphIDFields.setValue(paragraphs.getAndIncrement)
      piw.addDocument(d.pd)
    }
    if (text.length>2) d.textField.setStringValue(text.substring(0, text.length -2))
    d.textField.setTokenStream(new AnalysisTokenStream(textTokens.flatten))
    aiw.addDocument(d.ad)
  }
  
  var aiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var piw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  implicit val formats = DefaultFormats
  
  case class WordToAnalysis(
    word: String,
    analysis: List[Analysis]
  )
  
  case class Analysis(
    weight: Double,
    wordParts: List[WordPart],
    globalTags: List[Map[String,List[String]]]
  )
   
  case class WordPart(
    lemma: String, 
    tags: List[Map[String,List[String]]]
  )
  
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
        parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
          val obj = ObjParser.parseObject(p)
          val article = Try(new Article(
              (obj \ "id").asInstanceOf[JString].values,
              (obj \ "url" \ "full").asInstanceOf[JString].values,
              (obj \ "publisher" \ "name").asInstanceOf[JString].values,
              ISODateTimeFormat.dateTimeNoMillis.parseMillis((obj \ "datePublished").asInstanceOf[JString].values),
              (obj \ "coverage").asInstanceOf[JString].values,
              (obj \ "headline" \ "full").asInstanceOf[JString].values,
              if ((obj \ "lead").isInstanceOf[JString]) (obj \ "lead").asInstanceOf[JString].values else "",
              if ((obj \\ "analyzedText").isInstanceOf[JObject]) (obj \\ "analyzedText").asInstanceOf[JObject].children.map(_.asInstanceOf[JArray].children.map(_.extract[WordToAnalysis])) else List.empty
              ))
          article match {
            case Success(a) => 
              addTask(file.getName,() => index(a))
              logger.info("File "+file+" processed.")
            case Failure(e) => logger.error("Error processing file "+file, e)
          }
      })}))
    close(aiw)
    close(piw)
    mergeIndices(Seq(
     (opts.index()+"/aindex", new Sort(new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMB()),
     (opts.index()+"/pindex", new Sort(new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMB())))
  }
}
