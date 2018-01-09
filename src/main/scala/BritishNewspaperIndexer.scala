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
      siw.addDocument(d.sd)
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
        for (i <- 0 until 28) fis.read()
        val es = new StringBuilder()
        var b = fis.read
        while (b!='"') {
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
      case EvElemStart(_,"au_composed",_,_) => d.authorFields.setValue(readContents)
      case EvElemStart(_,"sc",_,_) => d.sectionFields.setValue(readContents)
      case EvElemStart(_,"supptitle",_,_) => d.supplementTitleFields.setValue(readContents)
      case EvElemStart(_,"ti",_,_) =>
        val title = readContents
        d.titleFields.setValue(title)
        d.issueContents.append(title + "\n\n")
      case EvElemStart(_,"ta",_,_) =>
        val subtitle = readContents + "\n\n"
        d.issueContents.append(subtitle)
        d.subtitles.append(subtitle)
      case EvElemStart(_,"il",attrs,_) =>
        d.illustrationsInArticle += 1
        d.illustrationsInIssue += 1
        attrs("type").headOption.foreach(n => {
          val f = new Field("containsGraphicOfType",n.text, notStoredStringFieldWithTermVectors)
          d.id.add(f)
          d.ad.add(f)
        })
        val c = readContents
        if (!c.isEmpty) {
          val f = new Field("containsGraphicCaption", c, normsOmittingStoredTextField)
          d.id.add(f)
          d.ad.add(f)
        }
      case EvElemStart(_,"pi",_,_) => d.pagesInArticle += 1
      case EvElemEnd(_,"article") =>
        val article = d.articleContents.toString
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
    d.illustrationsFields.setValue(d.illustrationsInIssue)
    d.sentencesFields.setValue(d.sentencesInIssue)
    d.articlesFields.setValue(d.articlesInIssue)
    iiw.addDocument(d.id)
    logger.info("File "+file+" processed.")
  }

  class ArticleMetadata {
    var id: String = null
    var language: String = null
    var sc: String = null
    var pc: Int = 0
    var ct: String = null
    var ti: String = null
    var ta: StringBuilder = new StringBuilder()
    var supptitle: String = null
    var au_composed: String = null
    var illustrationsInArticle = 0
    var ilCaptions= new scala.collection.mutable.HashMap[String,Int].withDefaultValue(0)
    var ilTypes = new scala.collection.mutable.HashMap[String,Int].withDefaultValue(0)
  }

  private def index2(collectionID: String, textFile: File): Unit = {
    val metadataFile = new File(textFile.getPath.replace("_Text.xml","_Issue.xml"))
    val d = tld.get
    d.clearOptionalIssueFields()
    d.collectionIDFields.setValue(collectionID)
    implicit val metadataXML = getXMLEventReaderWithCorrectEncoding(metadataFile)
    val amm = new scala.collection.mutable.HashMap[String,ArticleMetadata]
    var am: ArticleMetadata = null
    var articlesInIssue = 0
    while (metadataXML.hasNext) metadataXML.next match {
      case EvElemStart(_,"PSMID",_,_) =>
        val psmID = readContents
        d.issueIDFields.setValue(psmID.substring(psmID.lastIndexOf('-')))
      case EvElemStart(_,"newspaperID",_,_) => d.newspaperIDFields.setValue("NICNF0"+readContents)
      case EvElemStart(_,"language",_,_) => d.languageFields.setValue(readContents)
      case EvElemStart(_,"dw",_,_) => d.dayOfWeekFields.setValue(readContents) // maybe isn't there?
      case EvElemStart(_,"is",_,_) => d.issueNumberFields.setValue(readContents)
      case EvElemStart(_,"ip",_,_) => d.issuePagesFields.setValue(readContents.toInt) // not pc
      case EvElemStart(_,"searchableDateStart",_,_) =>
        val date = readContents.toInt
        d.dateStartFields.setValue(date)
        d.dateEndFields.setValue(date)
      case EvElemStart(_,"searchableDateEnd",_,_) => d.dateEndFields.setValue(readContents.toInt)
      case EvElemStart(_,"article",_,_) => am = new ArticleMetadata
      case EvElemStart(_,"id",_,_) => am.id = readContents
      case EvElemStart(_,"au_composed",_,_) => am.au_composed = readContents
      case EvElemStart(_,"sc",_,_) => am.sc = readContents
      case EvElemStart(_,"supptitle",_,_) => am.supptitle = readContents
      case EvElemStart(_,"ti",_,_) => am.ti = readContents
      case EvElemStart(_,"ta",_,_) =>
        am.ta.append(readContents)
        am.ta.append("\n\n")
      case EvElemStart(_,"il",attrs,_) =>
        am.illustrationsInArticle += 1
        d.illustrationsInIssue += 1
        attrs("type").headOption.foreach(n =>
          am.ilTypes.put(n.text, am.ilTypes(n.text) + 1)
        )
        val c = readContents
        if (!c.isEmpty)
          am.ilCaptions.put(c, am.ilCaptions(c) + 1)
      case EvElemStart(_,"pc",_,_) => am.pc = readContents.toInt
      case EvElemEnd(_,"article") =>
        amm.put(am.id, am)
        articlesInIssue += 1
      case EvElemStart(_,"ct",_,_) => am.ct = readContents
      case _ =>
    }
    implicit val xml = getXMLEventReaderWithCorrectEncoding(textFile)
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"artInfo",attrs,_) =>
        d.clearOptionalArticleFields()
        am = amm(attrs("id")(0).text)
        if (am.language != null) d.languageFields.setValue(am.language)
        if (am.sc != null) d.sectionFields.setValue(am.sc)
        d.articlePagesFields.setValue(am.pc)
        if (am.ct != null) d.articleTypeFields.setValue(am.ct)
        if (am.ti != null) d.titleFields.setValue(am.ti)
        if (!am.ta.isEmpty) d.subtitlesFields.setValue(am.ta.toString)
        if (am.supptitle != null) d.supplementTitleFields.setValue(am.supptitle)
        if (am.au_composed != null) d.authorFields.setValue(am.au_composed)
        d.illustrationsFields.setValue(am.illustrationsInArticle)
        for (
          (itype,times) <- am.ilTypes;
          _ <- 0.until(times)
        ) {
          val f = new Field("containsGraphicOfType", itype, notStoredStringFieldWithTermVectors)
          d.id.add(f)
          d.ad.add(f)
        }
        for (
          (c,times) <- am.ilCaptions;
          _ <- 0.until(times)
        ) {
          val f = new Field("containsGraphicCaption", c, normsOmittingStoredTextField)
          d.id.add(f)
          d.ad.add(f)
        }
      case EvElemStart(_,"ocrText",_,_) =>
        for (paragraph <- readContents(xml).split("\n")) processParagraph(paragraph.trim, d)
      case EvElemEnd(_,"article") =>
        val article = d.articleContents.toString
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
      case _ =>
    }
    val issue = d.issueContents.toString
    d.textField.setStringValue(issue)
    d.lengthFields.setValue(issue.length)
    d.tokensFields.setValue(getNumberOfTokens(issue))
    d.paragraphsFields.setValue(d.paragraphsInIssue)
    d.illustrationsFields.setValue(d.illustrationsInIssue)
    d.sentencesFields.setValue(d.sentencesInIssue)
    d.articlesFields.setValue(d.articlesInIssue)
    iiw.addDocument(d.id)
    logger.info("File "+textFile+" processed.")
  }

  var siw: IndexWriter = null.asInstanceOf[IndexWriter]
  var piw: IndexWriter = null.asInstanceOf[IndexWriter]
  var aiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var iiw: IndexWriter = null.asInstanceOf[IndexWriter]

  val ss = new Sort(new SortField("issueID",SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG), new SortField("sentenceID", SortField.Type.LONG))
  val ps = new Sort(new SortField("issueID",SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG))
  val as = new Sort(new SortField("issueID",SortField.Type.STRING), new SortField("articleID",SortField.Type.STRING))
  val is = new Sort(new SortField("issueID",SortField.Type.STRING))
  
  val postingsFormats = Seq("text","containsGraphicOfType")
  
  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val spostings = opt[String](default = Some("blocktree"))
      val ppostings = opt[String](default = Some("blocktree"))
      val ipostings = opt[String](default = Some("blocktree"))
      val apostings = opt[String](default = Some("blocktree"))
      verify()
    }
    if (!opts.onlyMerge()) {
      siw = iw(opts.index()+"/sindex",ss,opts.indexMemoryMb() / 4)
      piw = iw(opts.index()+"/pindex",ps,opts.indexMemoryMb() / 4)
      aiw = iw(opts.index()+"/aindex",as,opts.indexMemoryMb() / 4)
      iiw = iw(opts.index()+"/iindex",is,opts.indexMemoryMb() / 4)
      feedAndProcessFedTasksInParallel(() =>
        opts.directories().foreach(p => {
          val parts = p.split(':')
          if (parts.length == 2)
            getFileTree(new File(parts(0))).filter(_.getName.endsWith(".xml")).foreach(f => addTask(f.getPath, () => index(parts(1),f)))
          else
            getFileTree(new File(parts(0))).filter(_.getName.endsWith("_Text.xml")).foreach(f => addTask(f.getPath, () => index2(parts(1),f)))
        })
      )
    }
    waitForTasks(
      runSequenceInOtherThread(
        () => close(siw),
        () => merge(opts.index()+"/sindex", ss,opts.indexMemoryMb() / 4, toCodec(opts.spostings(), postingsFormats))
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
