import org.apache.lucene.store.FSDirectory
import java.nio.file.FileSystems
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.document.StoredField
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Collector
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.search.Scorer
import java.io.File
import scala.io.Source
import scala.xml.pull.XMLEventReader
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvText
import scala.xml.pull.EvEntityRef
import scala.xml.pull.EvComment
import scala.xml.pull.EvElemEnd
import org.apache.lucene.document.StringField
import org.apache.lucene.document.Field.Store
import org.json4s._
import org.json4s.native.JsonParser._
import org.json4s.JsonDSL._
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.StreamConverters._

import scala.collection.JavaConverters._
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import org.apache.lucene.document.FieldType
import scala.collection.mutable.HashMap
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.Environment
import com.sleepycat.je.DatabaseConfig
import scala.collection.mutable.Buffer
import org.apache.lucene.document.IntPoint
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.codecs.FilterCodec
import org.apache.lucene.codecs.lucene62.Lucene62Codec
import org.apache.lucene.codecs.memory.FSTOrdPostingsFormat
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.codecs.Codec
import fi.seco.lucene.FSTOrdTermVectorsCodec
import org.apache.lucene.analysis.CharArraySet
import org.apache.lucene.index.UpgradeIndexMergePolicy
import com.bizo.mighty.csv.CSVReader
import org.apache.lucene.index.SegmentCommitInfo
import com.typesafe.scalalogging.LazyLogging
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.LongPoint
import org.apache.lucene.index.BinaryDocValues
import org.apache.lucene.document.BinaryDocValuesField
import org.apache.lucene.util.BytesRef
import org.apache.lucene.document.SortedDocValuesField
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import java.io.StringWriter
import java.io.PrintWriter
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.ArrayBlockingQueue
import scala.concurrent.Promise
import org.apache.lucene.document.SortedSetDocValuesField
import org.json4s.JsonAST.JValue
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import scala.util.Try
import scala.util.Success
import scala.util.Failure

import org.rogach.scallop._
import scala.language.postfixOps
import java.lang.ThreadLocal
import scala.xml.parsing.XhtmlEntities
import scala.xml.MetaData
import scala.collection.Searching

object BritishNewspaperIndexer extends OctavoIndexer {

  private val paragraphs = new AtomicLong
  
  class Reuse {
    val pd = new Document() // paragraph
    val ad = new Document() // article
    val id = new Document() // issue
    
    val issueContents = new StringBuilder()
    val articleContents = new StringBuilder()
    
    val collectionIDFields = new StringSDVFieldPair("collectionID", pd, ad, id) // given as parameter
    
    val issueIDFields = new StringSDVFieldPair("issueID", pd, ad, id) // <issue ID="N0176185" FAID="BBCN0001" COLID="C00000" contentType="Newspaper">
    val newspaperIDFields = new StringSDVFieldPair("newspaperID", pd, ad, id) // <newspaperID>
    val articleIDFields = new StringSDVFieldPair("articleID", pd, ad) // <id>WO2_B0897WEJOSAPO_1724_11_28-0001-001</id>
    val paragraphIDFields = new StringNDVFieldPair("paragraphID", pd)
    
    val languageFields = new StringSDVFieldPair("language", pd, ad, id) // <language ocr="English" primary="Y">English</language>
    val dayOfWeekFields = new StringSDVFieldPair("dayOfWeek", pd, ad, id) // <dw>Saturday</dw>
    val issueNumberFields = new StringSDVFieldPair("issueNumber", pd, ad, id) //<is>317</is> 
    val dateStartFields = new IntPointNDVFieldPair("dateStart", pd, ad, id) // <searchableDateStart>17851129</searchableDateStart>
    val dateEndFields = new IntPointNDVFieldPair("dateEnd", pd, ad, id) // <searchableDateEnd>17851129</searchableDateEnd>
    
    val lengthFields = new IntPointNDVFieldPair("length", pd, ad, id)
    val tokensFields = new IntPointNDVFieldPair("tokens", pd, ad, id)

    var pagesInArticle = 0
    var paragraphsInArticle = 0
    var paragraphsInIssue = 0
    var articlesInIssue = 0
    
    val paragraphsFields = new IntPointNDVFieldPair("paragraphs", ad, id)
    val articlesFields = new IntPointNDVFieldPair("articles", id)
    
    val articlePagesFields = new IntPointNDVFieldPair("pages", ad)
    val issuePagesFields = new IntPointNDVFieldPair("pages", id)
    
    val textField = new Field("text", "", contentFieldType)
    pd.add(textField)
    ad.add(textField)
    id.add(textField)
    val titleField = new Field("title", "", contentFieldType) // <ti>The Ducal Shopkeepers and Petty Hucksters</ti>
    pd.add(titleField)
    ad.add(titleField)
    val articleTypeFields = new StringSDVFieldPair("articleType", pd, ad, id) // <ct>Arts and entertainment</ct>
    def clearOptionalDocumentFields() {
      paragraphsInIssue = 0
      articlesInIssue = 0
      issueContents.clear()
      dayOfWeekFields.setValue("")
      issueNumberFields.setValue("")
      dateStartFields.setValue(0)
      dateEndFields.setValue(Int.MaxValue)
      languageFields.setValue("")
    }
    def clearOptionalArticleFields() {
      pagesInArticle = 0
      paragraphsInArticle = 0
      articleContents.clear()
      articleTypeFields.setValue("")
      titleField.setStringValue("")
    }
  }

  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
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
    return content.toString.trim
  }
  
  case class Word(midX: Int, startX: Int, endX: Int, startY: Int, endY: Int, word: String) extends Ordered[Word] {
    def compare(that: Word): Int = this.midX-that.midX
  }
  
  class State {
    val content = new StringBuilder()
    var currentLine: ArrayBuffer[Word] = new ArrayBuffer //midx,startx,endx,starty,endy,content  
    var lastLine: ArrayBuffer[Word] = null
    def testParagraphBreakAtEndOfLine(guessParagraphs: Boolean): Option[String] = {
      var ret: Option[String] = None
      if (lastLine != null) { // we have two lines, check for a paragraph change between them.
        content.append(lastLine.map(_.word).mkString(" "))
        if (guessParagraphs) {
          val (lastSumEndY, lastSumHeight) = lastLine.foldLeft((0,0))((t,v) => (t._1 + v.endY, t._2 + (v.endY - v.startY)))
          val lastAvgHeight = lastSumHeight / lastLine.length
          val lastAvgEndY = lastSumEndY / lastLine.length
          val (currentSumStartY, currentSumHeight) = currentLine.foldLeft((0,0))((t,v) => (t._1 + v.startY, t._2 + (v.endY - v.startY)))
          val currentAvgHeight = currentSumHeight / currentLine.length
          val currentAvgStartY = currentSumStartY / currentLine.length
          
          val lastMaxHeight = lastLine.map(p => p.endY - p.startY).max
          val currentMaxHeight = currentLine.map(p => p.endY - p.startY).max

          val (lastSumWidth, lastSumChars) = lastLine.foldLeft((0,0))((t,v) => (t._1 + (v.endX - v.startX), t._2 + v.word.length))
          val (currentSumWidth, currentSumChars) = currentLine.foldLeft((0,0))((t,v) => (t._1 + (v.endX - v.startX), t._2 + v.word.length))
          
          val lastAvgCharWidth = lastSumWidth / lastSumChars
          val currentAvgCharWidth = currentSumWidth / currentSumChars

          val lastStartX = lastLine(0).startX
          val currentStartX = currentLine(0).startX
          if (
            (currentAvgStartY - lastAvgEndY > 0.90*math.min(lastMaxHeight, currentMaxHeight)) || // spacing 
            (currentStartX > lastStartX + 1.5*math.min(lastAvgCharWidth,currentAvgCharWidth)) || // indentation 
            (lastStartX > currentStartX + 2*math.min(lastAvgCharWidth,currentAvgCharWidth)) // catch edge cases
          ) {
            ret = Some(content.toString)
            content.clear()
          } else content.append('\n')
        } else content.append('\n')
      }
      lastLine = currentLine
      currentLine = new ArrayBuffer
      ret
    }
  }
  
  private def readNextWordPossiblyEmittingAParagraph(attrs: MetaData, state: State, guessParagraphs: Boolean)(implicit xml: XMLEventReader): Option[String] = {
    val word = readContents
    val pos = attrs("pos")(0).text.split(",")
    val curStartX = pos(0).toInt
    val curEndX = pos(2).toInt
    val curMidX = (curStartX + curEndX) / 2
    val curStartY = pos(1).toInt
    val curEndY = pos(3).toInt
    val curHeight = curEndY - curStartY
    var ret: Option[String] = None 
    if (!state.currentLine.isEmpty) {
      var pos = Searching.search(state.currentLine).search(Word(curMidX,-1,-1,-1,-1,"")).insertionPoint - 1
      val Word(_,_,_,lastStartY, lastEndY, _) = state.currentLine(if (pos == -1) 0 else pos)
      if (curStartY > lastEndY || curMidX < state.currentLine.last.midX) // new line or new paragraph
        ret = state.testParagraphBreakAtEndOfLine(guessParagraphs)
      state.currentLine += (Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString))
    } else state.currentLine += (Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString))
    ret
  }
  
  private def processParagraph(paragraph: String, d: Reuse): Unit = {
    d.articleContents.append(paragraph + "\n\n")
    d.issueContents.append(paragraph + "\n\n")
    d.paragraphIDFields.setValue(paragraphs.getAndIncrement)
    d.textField.setStringValue(paragraph)
    d.lengthFields.setValue(paragraph.length)
    d.tokensFields.setValue(getNumberOfTokens(paragraph))
    d.paragraphsInArticle += 1
    d.paragraphsInIssue += 1
    piw.addDocument(d.pd)
  }
  
  private def index(collectionID: String, file: File): Unit = {
    val d = tld.get
    d.clearOptionalDocumentFields()
    d.collectionIDFields.setValue(collectionID)
    implicit val xml = new XMLEventReader(Source.fromFile(file, "ISO-8859-1"))
    val state = new State()
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"issue",attrs,_) => d.issueIDFields.setValue(attrs("ID")(0).text)
      case EvElemStart(_,"newspaperID",_,_) => d.newspaperIDFields.setValue(readContents)
      case EvElemStart(_,"language",_,_) => d.languageFields.setValue(readContents)
      case EvElemStart(_,"dw",_,_) => d.dayOfWeekFields.setValue(readContents)
      case EvElemStart(_,"is",_,_) => d.issueNumberFields.setValue(readContents)
      case EvElemStart(_,"pc",_,_) => d.issuePagesFields.setValue(readContents.toInt)
      case EvElemStart(_,"searchableDateStart",_,_) => 
        val date = readContents.toInt
        d.dateStartFields.setValue(date)
        d.dateEndFields.setValue(date)
      case EvElemStart(_,"searchableDateEnd",_,_) => d.dateEndFields.setValue(readContents.toInt)
      case EvElemStart(_,"article",_,_) => d.clearOptionalArticleFields()
      case EvElemStart(_,"id",_,_) => d.articleIDFields.setValue(readContents)
      case EvElemStart(_,"pi",_,_) => d.pagesInArticle += 1
      case EvElemEnd(_,"article") =>
        val article = d.articleContents.toString
        d.textField.setStringValue(article)
        d.lengthFields.setValue(article.length)
        d.tokensFields.setValue(getNumberOfTokens(article))
        d.paragraphsFields.setValue(d.paragraphsInArticle)
        d.articlePagesFields.setValue(d.pagesInArticle)
        aiw.addDocument(d.ad)
        d.articlesInIssue += 1
      case EvElemStart(_,"ti",_,_) =>
        val title = readContents
        d.titleField.setStringValue(title)
        d.issueContents.append(title + "\n\n")
      case EvElemStart(_,"ct",_,_) => d.articleTypeFields.setValue(readContents)
      case EvElemStart(_,"wd",attrs,_) => readNextWordPossiblyEmittingAParagraph(attrs, state, true) match {
        case Some(paragraph) => processParagraph(paragraph, d)
        case None => 
      }
      case EvElemEnd(_,"p") => 
        state.testParagraphBreakAtEndOfLine(true) match {
          case Some(paragraph) => 
            processParagraph(paragraph, d)
          case None => 
        }
        state.content.append(state.lastLine.map(_.word).mkString(" "))
        if (state.content.length>0)
          processParagraph(state.content.toString, d)
        state.content.clear()
        state.lastLine = null
        state.currentLine.clear
      case _ => 
    }
    val issue = d.issueContents.toString
    d.textField.setStringValue(issue)
    d.lengthFields.setValue(issue.length)
    d.tokensFields.setValue(getNumberOfTokens(issue))
    d.paragraphsFields.setValue(d.paragraphsInIssue)
    iiw.addDocument(d.id)
    logger.info("File "+file+" processed.")
  }

  var piw: IndexWriter = null.asInstanceOf[IndexWriter]
  var aiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var iiw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  def main(args: Array[String]): Unit = {
    val opts = new OctavoOpts(args)
    piw = iw(opts.index()+"/pindex",new Sort(new SortField("issueID", SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMb() / 3)
    aiw = iw(opts.index()+"/aindex",new Sort(new SortField("issueID", SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMb() / 3)
    iiw = iw(opts.index()+"/iindex",new Sort(new SortField("issueID",SortField.Type.STRING)),opts.indexMemoryMb() / 3)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toStream.par.flatMap(p => {
        val parts = p.split(':')
        getFileTree(new File(parts(0))).map((parts(1),_))
      }).filter(_._2.getName.endsWith(".xml")).foreach(pair => addTask(pair._2.getPath, () => index(pair._1,pair._2)))
    )
    close(Seq(piw,aiw,iiw))
    mergeIndices(Seq(
     (opts.index()+"/pindex", new Sort(new SortField("issueID",SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)),opts.indexMemoryMb() / 3),
     (opts.index()+"/aindex", new Sort(new SortField("issueID",SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING)),opts.indexMemoryMb() / 3),
     (opts.index()+"/iindex", new Sort(new SortField("issueID",SortField.Type.STRING)),opts.indexMemoryMb() / 3)
     
    ))  
  }
}
