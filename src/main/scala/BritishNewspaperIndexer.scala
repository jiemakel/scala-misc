import java.io.{File, FileInputStream, PushbackInputStream}
import java.text.BreakIterator
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import org.apache.lucene.document.{Document, Field, NumericDocValuesField}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.rogach.scallop._

import scala.collection.Searching
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.language.{postfixOps, reflectiveCalls}
import scala.xml.MetaData
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._

object BritishNewspaperIndexer extends OctavoIndexer {

  private val paragraphs = new AtomicLong
  private val sentences = new AtomicLong
  
  class Reuse {
    val sd = new Document // sentence
    val pd = new Document // paragraph
    val ad = new Document // article
    val id = new Document // issue

    val sbi = BreakIterator.getSentenceInstance(new Locale("en_GB"))
    
    val issueContents = new StringBuilder()
    val articleContents = new StringBuilder()
    
    val collectionIDFields = new StringSDVFieldPair("collectionID", sd, pd, ad, id) // given as parameter
    
    val issueIDFields = new StringSDVFieldPair("issueID", sd, pd, ad, id) // <issue ID="N0176185" FAID="BBCN0001" COLID="C00000" contentType="Newspaper">
    val newspaperIDFields = new StringSDVFieldPair("newspaperID", sd, pd, ad, id) // <newspaperID>
    val articleIDFields = new StringSDVFieldPair("articleID", sd, pd, ad) // <id>WO2_B0897WEJOSAPO_1724_11_28-0001-001</id>
    val paragraphIDFields = new StringNDVFieldPair("paragraphID", sd, pd)
    val sentenceIDField = new NumericDocValuesField("sentenceID", 0)
    sd.add(sentenceIDField)
    
    val languageFields = new StringSDVFieldPair("language", sd, pd, ad, id) // <language ocr="English" primary="Y">English</language>
    val dayOfWeekFields = new StringSDVFieldPair("dayOfWeek", sd, pd, ad, id) // <dw>Saturday</dw>
    val issueNumberFields = new StringSDVFieldPair("issueNumber", sd, pd, ad, id) //<is>317</is>
    val dateStartFields = new IntPointNDVFieldPair("dateStart", sd, pd, ad, id) // <searchableDateStart>17851129</searchableDateStart>
    val dateEndFields = new IntPointNDVFieldPair("dateEnd", sd, pd, ad, id) // <searchableDateEnd>17851129</searchableDateEnd>
    val authorFields = new TextSDVFieldPair("author", sd, pd, ad) // <au_composed>Tom Whipple</au_composed>
    val sectionFields = new StringSDVFieldPair("section", sd, pd, ad) // <sc>A</sc>

    val ocrConfidenceFields = new IntPointNDVFieldPair("ocrConfidence", sd, pd, ad, id)
    val lengthFields = new IntPointNDVFieldPair("length", sd, pd, ad, id)
    val tokensFields = new IntPointNDVFieldPair("tokens", sd, pd, ad, id)

    var pagesInArticle = 0
    var paragraphsInArticle = 0
    var sentencesInArticle = 0
    var illustrationsInArticle = 0
    var illustrationsInIssue = 0
    var paragraphsInIssue = 0
    var sentencesInIssue = 0
    var articlesInIssue = 0
    
    val paragraphsFields = new IntPointNDVFieldPair("paragraphs", ad, id)
    val sentencesFields = new IntPointNDVFieldPair("sentences", pd, ad, id)
    val articlesFields = new IntPointNDVFieldPair("articles", id)
    val illustrationsFields = new IntPointNDVFieldPair("illustrations", ad, id)
    
    val articlePagesFields = new IntPointNDVFieldPair("pages", ad)
    val issuePagesFields = new IntPointNDVFieldPair("pages", id)
    
    val textField = new Field("text", "", contentFieldType)
    sd.add(textField)
    pd.add(textField)
    ad.add(textField)
    id.add(textField)
    
    val supplementTitleFields = new TextSDVFieldPair("supplementTitle", sd, pd, ad)
    
    val titleFields = new TextSDVFieldPair("title", sd, pd, ad) // <ti>The Ducal Shopkeepers and Petty Hucksters</ti>
    
    val subtitles = new StringBuilder()
    val subtitlesFields = new TextSDVFieldPair("subtitles", sd, pd, ad) //   <ta>Sketch of the week</ta> <ta>Slurs and hyperbole vied with tosh to make the headlines, says Tom Whipple</ta>
    
    val articleTypeFields = new StringSDVFieldPair("articleType", sd, pd, ad) // <ct>Arts and entertainment</ct>
    def clearOptionalIssueFields() {
      sentencesInIssue = 0
      paragraphsInIssue = 0
      articlesInIssue = 0
      illustrationsInIssue = 0
      issueContents.clear()
      dayOfWeekFields.setValue("")
      issueNumberFields.setValue("")
      dateStartFields.setValue(0)
      dateEndFields.setValue(Int.MaxValue)
      languageFields.setValue("")
      id.removeFields("containsGraphicOfType")
      id.removeFields("containsGraphicCaption")
    }
    def clearOptionalArticleFields() {
      sentencesInArticle = 0
      pagesInArticle = 0
      paragraphsInArticle = 0
      illustrationsInArticle = 0
      articleContents.clear()
      articleTypeFields.setValue("")
      titleFields.setValue("")
      authorFields.setValue("")
      sectionFields.setValue("")
      supplementTitleFields.setValue("")
      subtitles.clear()
      subtitlesFields.setValue("")
      ad.removeFields("containsGraphicOfType")
      ad.removeFields("containsGraphicCaption")
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
        case _ =>
          logger.warn("Encountered unknown entity "+er.entity)
          content.append('[')
          content.append(er.entity)
          content.append(']')
      }
      case EvComment(comment) if (comment == " unknown entity apos; ") => content.append('\'')
      case EvComment(comment) if (comment.startsWith(" unknown entity")) =>
        val entity = content.substring(16, content.length - 2)
        logger.warn("Encountered unknown entity "+entity)
        content.append('[')
        content.append(entity)
        content.append(']')
      case EvComment(comment) =>
        logger.debug("Encountered comment: "+comment)
      case EvElemEnd(_,_) => break = true
    }
    content.toString
  }
  
  case class Word(midX: Int, startX: Int, endX: Int, startY: Int, endY: Int, word: String) extends Ordered[Word] {
    def compare(that: Word): Int = this.midX-that.midX
  }
  
  class State {
    val content = new StringBuilder()
    var currentLine: ArrayBuffer[Word] = new ArrayBuffer //midx,startx,endx,starty,endy,content  
    var lastLine: ArrayBuffer[Word] = _
    def testParagraphBreakAtEndOfLine(guessParagraphs: Boolean): Option[String] = {
      var ret: Option[String] = None
      if (lastLine != null) { // we have two lines, check for a paragraph change between them.
        content.append(lastLine.map(_.word).mkString(" "))
        if (guessParagraphs) {
          val (lastSumEndY, _) = lastLine.foldLeft((0,0))((t,v) => (t._1 + v.endY, t._2 + (v.endY - v.startY)))
          //val lastAvgHeight = lastSumHeight / lastLine.length
          val lastAvgEndY = lastSumEndY / lastLine.length
          val (currentSumStartY, _) = currentLine.foldLeft((0,0))((t,v) => (t._1 + v.startY, t._2 + (v.endY - v.startY)))
          //val currentAvgHeight = currentSumHeight / currentLine.length
          val currentAvgStartY = currentSumStartY / currentLine.length
          
          val lastMaxHeight = lastLine.map(p => p.endY - p.startY).max
          val currentMaxHeight = currentLine.map(p => p.endY - p.startY).max

          val (lastSumWidth, lastSumChars) = lastLine.foldLeft((0,0))((t,v) => (t._1 + (v.endX - v.startX), t._2 + v.word.length))
          val (currentSumWidth, currentSumChars) = currentLine.foldLeft((0,0))((t,v) => (t._1 + (v.endX - v.startX), t._2 + v.word.length))
          
          if (lastSumChars == 0 || currentSumChars == 0) {
            logger.warn("Encountered a line without content when comparing lastLine:"+lastLine+", currentLinw:"+currentLine+". Cannot determine if there should be a paragraph break")
            content.append('\n')
          } else {
            val lastAvgCharWidth = lastSumWidth / lastSumChars
            val currentAvgCharWidth = currentSumWidth / currentSumChars

            val lastStartX = lastLine(0).startX
            val currentStartX = currentLine(0).startX
            if (
              (currentAvgStartY - lastAvgEndY > 0.90 * math.min(lastMaxHeight, currentMaxHeight)) || // spacing
                (currentStartX > lastStartX + 1.5 * math.min(lastAvgCharWidth, currentAvgCharWidth)) || // indentation
                (lastStartX > currentStartX + 2 * math.min(lastAvgCharWidth, currentAvgCharWidth)) // catch edge cases
            ) {
              ret = Some(content.toString)
              content.clear()
            } else content.append('\n')
          }
        } else content.append('\n')
      }
      lastLine = currentLine
      currentLine = new ArrayBuffer
      ret
    }
  }
  
  private def readNextWordPossiblyEmittingAParagraph(attrs: MetaData, state: State, guessParagraphs: Boolean)(implicit xml: XMLEventReader): Option[String] = {
    val word = readContents
    val posa = attrs("pos").head.text.split(",")
    val curStartX = posa(0).toInt
    val curEndX = posa(2).toInt
    val curMidX = (curStartX + curEndX) / 2
    val curStartY = posa(1).toInt
    val curEndY = posa(3).toInt
    //val curHeight = curEndY - curStartY
    var ret: Option[String] = None 
    if (state.currentLine.nonEmpty) {
      val pos = Searching.search(state.currentLine).search(Word(curMidX,-1,-1,-1,-1,"")).insertionPoint - 1
      val Word(_,_,_,_, lastEndY, _) = state.currentLine(if (pos == -1) 0 else pos)
      if (curStartY > lastEndY || curMidX < state.currentLine.last.midX) // new line or new paragraph
        ret = state.testParagraphBreakAtEndOfLine(guessParagraphs)
      state.currentLine += Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString)
    } else state.currentLine += Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString)
    ret
  }
  
  private def processParagraph(paragraph: String, d: Reuse): Unit = {
    d.sbi.setText(paragraph)
    var start = d.sbi.first()
    var end = d.sbi.next()
    var csentences = 0
    while (end != BreakIterator.DONE) {
      val sentence = paragraph.substring(start,end)
      d.sentenceIDField.setLongValue(sentences.incrementAndGet)
      d.textField.setStringValue(sentence)
      d.lengthFields.setValue(sentence.length)
      d.tokensFields.setValue(getNumberOfTokens(sentence))
      if (siw != null) siw.addDocument(d.sd)
      start = end
      end = d.sbi.next()
      csentences += 1
    }
    d.sentencesFields.setValue(csentences)
    d.sentencesInIssue += csentences
    d.sentencesInArticle += csentences
    d.articleContents.append(paragraph)
    d.articleContents.append("\n\n")
    d.issueContents.append(paragraph)
    d.issueContents.append("\n\n")
    d.paragraphIDFields.setValue(paragraphs.getAndIncrement)
    d.textField.setStringValue(paragraph)
    d.lengthFields.setValue(paragraph.length)
    d.tokensFields.setValue(getNumberOfTokens(paragraph))
    d.paragraphsInArticle += 1
    d.paragraphsInIssue += 1
    piw.addDocument(d.pd)
  }
  
  private def getXMLEventReaderWithCorrectEncoding(file: File): XMLEventReader = {
    val fis = new PushbackInputStream(new FileInputStream(file),2)
    var second = 0
    var first = fis.read()
    if (first == 0xEF) { // BOM
      fis.read()
      fis.read()
    } else fis.unread(first)
    var encoding = "ISO-8859-1"
    do {
      first = fis.read()
      second = fis.read()
      if (first == '<' && second=='!') while (fis.read()!='\n') {}
      if (first == '<' && (second=='?')) {
        for (_ <- 0 until 28) fis.read()
        val es = new StringBuilder()
        var b = fis.read
        while (b!='"' && b!='\'') {
          es.append(b.toChar)
          b = fis.read()
        }
        encoding = es.toString
        while (fis.read()!='\n') {}
      }
    } while (first == '<' && (second=='?' || second=='!'))
    fis.unread(second)
    fis.unread(first)
    new XMLEventReader(Source.fromInputStream(fis,encoding))
  }
  
  private def index(collectionID: String, file: File): Unit = {
    val d = tld.get
    d.clearOptionalIssueFields()
    d.collectionIDFields.setValue(collectionID)
    implicit val xml = getXMLEventReaderWithCorrectEncoding(file)
    try {
      val state = new State()
      var edate = 0
      var issueConfidence = 0
      var articleConfidence = 0
      var articleConfidenceCount = 0
      var break = false
      // read metadatainfo first. If it doesn't exist, we have a broken issue and the whole thing gets skipped
      while (xml.hasNext && !break) xml.next match {
        case EvElemStart(_, "issue", attrs, _) => d.issueIDFields.setValue(attrs("ID").head.text)
        case EvElemStart(_, "newspaperID", _, _) => d.newspaperIDFields.setValue(readContents)
        case EvElemStart(_, "ocr", _, _) =>
          val confidence = readContents
          val confidenceAsInt = (confidence.substring(0,confidence.indexOf('.'))+confidence.substring(confidence.indexOf('.')+1,confidence.length)).toInt
          issueConfidence = confidenceAsInt
        case EvElemStart(_, "searchableDateStart", _, _) =>
          val dateS = readContents
          val sdate = dateS.toInt
          edate = dateS.replace("00","99").toInt
          d.dateStartFields.setValue(sdate)
          d.dateEndFields.setValue(edate)
        case EvElemStart(_, "searchableDateEnd", _, _) =>
          edate = readContents.toInt
          d.dateEndFields.setValue(edate)
        case EvElemStart(_, "dw", _, _) => d.dayOfWeekFields.setValue(readContents)
        case EvElemStart(_, "is", _, _) => d.issueNumberFields.setValue(readContents)
        case EvElemStart(_, "language", _, _) => d.languageFields.setValue(readContents)
        case EvElemEnd(_,"metadatainfo") => break = true
      }
      while (xml.hasNext) xml.next match {
        case EvElemStart(_, "ocrLanguage", _, _) => d.languageFields.setValue(readContents)
        case EvElemStart(_, "pc", _, _) => d.issuePagesFields.setValue(readContents.toInt)
        case EvElemStart(_, "ocr", _, _) =>
          val confidence = readContents
          val confidenceAsInt = (confidence.substring(0,confidence.indexOf('.'))+confidence.substring(confidence.indexOf('.')+1,confidence.length)).toInt
          articleConfidence += confidenceAsInt
          articleConfidenceCount += 1
          d.ocrConfidenceFields.setValue(confidenceAsInt)
        case EvElemStart(_, "article", _, _) =>
          d.clearOptionalArticleFields()
          articleConfidence = 0
          articleConfidenceCount = 0
        case EvElemStart(_, "id", _, _) => d.articleIDFields.setValue(readContents)
        case EvElemStart(_, "au_composed", _, _) => d.authorFields.setValue(readContents)
        case EvElemStart(_, "sc", _, _) => d.sectionFields.setValue(readContents)
        case EvElemStart(_, "supptitle", _, _) => d.supplementTitleFields.setValue(readContents)
        case EvElemStart(_, "ti", _, _) =>
          val title = readContents
          d.titleFields.setValue(title)
          d.issueContents.append(title + "\n\n")
        case EvElemStart(_, "ta", _, _) =>
          val subtitle = readContents + "\n\n"
          d.issueContents.append(subtitle)
          d.subtitles.append(subtitle)
        case EvElemStart(_, "il", attrs, _) =>
          d.illustrationsInArticle += 1
          d.illustrationsInIssue += 1
          attrs("type").headOption.foreach(n => {
            val f = new Field("containsGraphicOfType", n.text, notStoredStringFieldWithTermVectors)
            d.id.add(f)
            d.ad.add(f)
          })
          val c = readContents
          if (!c.isEmpty) {
            val f = new Field("containsGraphicCaption", c, normsOmittingStoredTextField)
            d.id.add(f)
            d.ad.add(f)
          }
        case EvElemStart(_, "pi", _, _) => d.pagesInArticle += 1
        case EvElemEnd(_, "article") =>
          val article = d.articleContents.toString
          d.ocrConfidenceFields.setValue(articleConfidence/articleConfidenceCount)
          d.textField.setStringValue(article)
          d.lengthFields.setValue(article.length)
          d.tokensFields.setValue(getNumberOfTokens(article))
          d.paragraphsFields.setValue(d.paragraphsInArticle)
          d.articlePagesFields.setValue(d.pagesInArticle)
          d.subtitlesFields.setValue(d.subtitles.toString)
          d.illustrationsFields.setValue(d.illustrationsInArticle)
          d.sentencesFields.setValue(d.sentencesInArticle)
          aiw.addDocument(d.ad)
          d.articlesInIssue += 1
        case EvElemStart(_, "ct", _, _) => d.articleTypeFields.setValue(readContents)
        case EvElemStart(_, "wd", attrs, _) => readNextWordPossiblyEmittingAParagraph(attrs, state, guessParagraphs = true) match {
          case Some(paragraph) => processParagraph(paragraph, d)
          case None =>
        }
        case EvElemEnd(_, "p") =>
          if (state.currentLine.nonEmpty) state.testParagraphBreakAtEndOfLine(true) match {
            case Some(paragraph) =>
              processParagraph(paragraph, d)
            case None =>
          }
          if (state.lastLine != null) state.content.append(state.lastLine.map(_.word).mkString(" "))
          if (state.content.nonEmpty)
            processParagraph(state.content.toString, d)
          state.content.clear()
          state.lastLine = null
          state.currentLine.clear
        case _ =>
      }
      val issue = d.issueContents.toString
      if (issue.nonEmpty) {
        d.ocrConfidenceFields.setValue(issueConfidence)
        d.textField.setStringValue(issue)
        d.lengthFields.setValue(issue.length)
        d.tokensFields.setValue(getNumberOfTokens(issue))
        d.paragraphsFields.setValue(d.paragraphsInIssue)
        d.illustrationsFields.setValue(d.illustrationsInIssue)
        d.sentencesFields.setValue(d.sentencesInIssue)
        d.articlesFields.setValue(d.articlesInIssue)
        iiw.addDocument(d.id)
        logger.info("File " + file + " processed.")
      } else logger.warn("File " + file + "yielded no content.")
    } finally {
      while (xml.hasNext) xml.next() // stupid XMLEventReader.stop() deadlocks and leaves threads hanging
    }
  }

  var siw: IndexWriter = _
  var piw: IndexWriter = _
  var aiw: IndexWriter = _
  var iiw: IndexWriter = _

  val ss = new Sort(new SortField("issueID",SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG), new SortField("sentenceID", SortField.Type.LONG))
  val ps = new Sort(new SortField("issueID",SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG))
  val as = new Sort(new SortField("issueID",SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING))
  val is = new Sort(new SortField("issueID",SortField.Type.STRING))
  
  val postingsFormats = Seq("text","containsGraphicOfType")

  var startYear: Option[Int] = None
  var endYear: Option[Int] = None
  
  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val spostings = opt[String](default = Some("blocktree"))
      val ppostings = opt[String](default = Some("blocktree"))
      val ipostings = opt[String](default = Some("blocktree"))
      val apostings = opt[String](default = Some("blocktree"))
      val noSentenceIndex= opt[Boolean]()
      val startYear = opt[String]()
      val endYear = opt[String]()
      verify()
    }
    startYear = opts.startYear.toOption.map(v => (v+"0000").toInt)
    endYear = opts.endYear.toOption.map(v => (v+"9999").toInt)
    if (!opts.onlyMerge()) {
      if (!opts.noSentenceIndex()) siw = iw(opts.index()+"/sindex",ss,opts.indexMemoryMb() / 4)
      piw = iw(opts.index()+"/pindex",ps,opts.indexMemoryMb() / 4)
      aiw = iw(opts.index()+"/aindex",as,opts.indexMemoryMb() / 4)
      iiw = iw(opts.index()+"/iindex",is,opts.indexMemoryMb() / 4)
      feedAndProcessFedTasksInParallel(() =>
        opts.directories().foreach(p => {
          val parts = p.split(':')
          getFileTree(new File(parts(0))).filter(_.getName.endsWith(".xml")).foreach(f => addTask(f.getPath, () => index(parts(1),f)))
        })
      )
    }
    waitForTasks(
      runSequenceInOtherThread(
        () => if (siw != null) close(siw),
        () => if (siw != null) merge(opts.index()+"/sindex", ss,opts.indexMemoryMb() / 4, toCodec(opts.spostings(), postingsFormats))
      ),
      runSequenceInOtherThread(
        () => close(piw), 
        () => merge(opts.index()+"/pindex", ps,opts.indexMemoryMb() / 4, toCodec(opts.ppostings(), postingsFormats))
      ),
      runSequenceInOtherThread(
        () => close(aiw), 
        () => merge(opts.index()+"/aindex", as,opts.indexMemoryMb() / 4, toCodec(opts.apostings(), postingsFormats))
      ),
      runSequenceInOtherThread(
        () => close(iiw), 
        () => merge(opts.index()+"/iindex", is,opts.indexMemoryMb() / 4, toCodec(opts.ipostings(), postingsFormats))
      )
    )
  }
}
