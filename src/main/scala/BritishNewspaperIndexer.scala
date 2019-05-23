import java.io.{File, FileInputStream, PushbackInputStream}
import java.text.BreakIterator
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import jetbrains.exodus.bindings.{IntegerBinding, StringBinding}
import jetbrains.exodus.env._
import jetbrains.exodus.util.LightOutputStream
import jetbrains.exodus.{ArrayByteIterable, CompoundByteIterable}
import org.apache.lucene.document.{Field, NumericDocValuesField}
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
    val sd = new FluidDocument // sentence
    val pd = new FluidDocument // paragraph
    val ad = new FluidDocument // article
    val id = new FluidDocument // issue

    val sbi = BreakIterator.getSentenceInstance(new Locale("en_GB"))
    
    val issueContents = new StringBuilder()
    val articleContents = new StringBuilder()
    
    val collectionIDFields = new StringSDVFieldPair("collectionID").r(sd, pd, ad, id) // given as parameter

    var issueIDEntry: ArrayByteIterable = _
    val offsetData = new LightOutputStream()
    val offsetDataBytes = new Array[Int](4)
    val issueIDFields = new StringSDVFieldPair("issueID").r(sd, pd, ad, id) // psmID, was <issue ID="N0176185" FAID="BBCN0001" COLID="C00000" contentType="Newspaper">
    val newspaperIDFields = new StringSDVFieldPair("newspaperID").r(sd, pd, ad, id) // <newspaperID>
    val articleIDFields = new StringSDVFieldPair("articleID").r(sd, pd, ad) // <id>WO2_B0897WEJOSAPO_1724_11_28-0001-001</id>
    val paragraphIDFields = new StringNDVFieldPair("paragraphID").r(sd, pd)
    val sentenceIDField = new NumericDocValuesField("sentenceID", 0)
    sd.addRequired(sentenceIDField)

    val dateStartFields = new IntPointNDVFieldPair("dateStart").r(sd, pd, ad, id) // <searchableDateStart>17851129</searchableDateStart>
    val dateEndFields = new IntPointNDVFieldPair("dateEnd").r(sd, pd, ad, id) // <searchableDateEnd>17851129</searchableDateEnd>
    val languageFields = new StringSDVFieldPair("language").o(sd, pd, ad) // <language ocr="English" primary="Y">English</language>
    // val issueLanguageFields = new StringSSDVFieldPair("language").o(id)
    val dayOfWeekFields = new StringSDVFieldPair("dayOfWeek").o(sd, pd, ad, id) // <dw>Saturday</dw>
    val issueNumberFields = new StringSDVFieldPair("issueNumber").o(sd, pd, ad, id) //<is>317</is>

    val ocrConfidenceFields = new IntPointNDVFieldPair("ocrConfidence").r(sd, pd, ad, id)
    val lengthFields = new IntPointNDVFieldPair("length").r(sd, pd, ad, id)
    val tokensFields = new IntPointNDVFieldPair("tokens").r(sd, pd, ad, id)

    val startOffsetFields = new IntPointNDVFieldPair("startOffset").r(sd, pd, ad)
    val endOffsetFields = new IntPointNDVFieldPair("endOffset").r(sd, pd, ad)

    var pagesInArticle = 0
    var paragraphsInArticle = 0
    var sentencesInArticle = 0
    var illustrationsInArticle = 0
    var illustrationsInIssue = 0
    var paragraphsInIssue = 0
    var sentencesInIssue = 0
    var articlesInIssue = 0
    
    val paragraphsFields = new IntPointNDVFieldPair("paragraphs").r(ad, id)
    val sentencesFields = new IntPointNDVFieldPair("sentences").r(pd, ad, id)
    val articlesFields = new IntPointNDVFieldPair("articles").r(id)
    val illustrationsFields = new IntPointNDVFieldPair("illustrations").r(ad, id)
    
    val articlePagesFields = new IntPointNDVFieldPair("pages").r(ad)
    val issuePagesFields = new IntPointNDVFieldPair("pages").r(id)
    
    val textField = new ContentField("text").r(sd,pd,ad,id)

    val supplementTitleFields = new TextSDVFieldPair("supplementTitle").o(sd, pd, ad)

    val sectionFields = new StringSDVFieldPair("section").o(sd, pd, ad) // <sc>A</sc>

    val authorFields = new TextSDVFieldPair("author").o(sd, pd, ad) // <au_composed>Tom Whipple</au_composed>
    val titleFields = new TextSDVFieldPair("title").o(sd, pd, ad) // <ti>The Ducal Shopkeepers and Petty Hucksters</ti>
    
    val subtitles = new StringBuilder()
    val subtitlesFields = new TextSDVFieldPair("subtitles").o(sd, pd, ad) //   <ta>Sketch of the week</ta> <ta>Slurs and hyperbole vied with tosh to make the headlines, says Tom Whipple</ta>
    
    val articleTypeFields = new StringSDVFieldPair("articleType").o(sd, pd, ad) // <ct>Arts and entertainment</ct>

    def clearOptionalIssueFields() {
      sentencesInIssue = 0
      paragraphsInIssue = 0
      articlesInIssue = 0
      illustrationsInIssue = 0
      issueContents.clear()
      id.clearOptional()
      dateStartFields.setValue(0)
      dateEndFields.setValue(Int.MaxValue)
/*
      dayOfWeekFields.setValue("")
      issueNumberFields.setValue("")
      languageFields.setValue("")

      id.removeFields("containsGraphicOfType")
      id.removeFields("containsGraphicCaption") */
    }
    def clearOptionalArticleFields() {
      sentencesInArticle = 0
      pagesInArticle = 0
      paragraphsInArticle = 0
      illustrationsInArticle = 0
      articleContents.clear()
      subtitles.clear()
      ad.clearOptional()
      pd.clearOptional()
      sd.clearOptional()
/*
      articleTypeFields.setValue("")
      titleFields.setValue("")
      authorFields.setValue("")
      sectionFields.setValue("")
      supplementTitleFields.setValue("")
      subtitlesFields.setValue("")

      ad.removeFields("containsGraphicOfType")
      ad.removeFields("containsGraphicCaption") */
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
      case EvComment(comment) if comment == " unknown entity apos; " => content.append('\'')
      case EvComment(comment) if comment.startsWith(" unknown entity") =>
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
    var content = new ArrayBuffer[ArrayBuffer[Word]]()
    var currentLine: ArrayBuffer[Word] = new ArrayBuffer //midx,startx,endx,starty,endy,content  
    var lastLine: ArrayBuffer[Word] = _
    def testParagraphBreakAtEndOfLine(guessParagraphs: Boolean): Option[Seq[Seq[Word]]] = {
      var ret: Option[Seq[Seq[Word]]] = None
      if (lastLine != null) { // we have two lines, check for a paragraph change between them.
        content.append(lastLine)
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
            logger.warn("Encountered a line without content when comparing lastLine:"+lastLine+", currentLine:"+currentLine+". Cannot determine if there should be a paragraph break")
            // content.append('\n')
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
              ret = Some(content)
              content = new ArrayBuffer[ArrayBuffer[Word]]()
            } // else content.append('\n')
          }
        } // else content.append('\n')
      }
      lastLine = currentLine
      currentLine = new ArrayBuffer
      ret
    }
  }
  
  private def readNextWordPossiblyEmittingAParagraph(attrs: MetaData, state: State, guessParagraphs: Boolean)(implicit xml: XMLEventReader): Option[Seq[Seq[Word]]] = {
    val word = readContents
    val posa = attrs("pos").head.text.split(",")
    val curStartX = posa(0).toInt
    val curEndX = posa(2).toInt
    val curMidX = (curStartX + curEndX) / 2
    val curStartY = posa(1).toInt
    val curEndY = posa(3).toInt
    //val curHeight = curEndY - curStartY
    var ret: Option[Seq[Seq[Word]]] = None
    if (state.currentLine.nonEmpty) {
      val pos = Searching.search(state.currentLine).search(Word(curMidX,-1,-1,-1,-1,"")).insertionPoint - 1
      val Word(_,_,_,_, lastEndY, _) = state.currentLine(if (pos == -1) 0 else pos)
      if (curStartY > lastEndY || curMidX < state.currentLine.last.midX) // new line or new paragraph
        ret = state.testParagraphBreakAtEndOfLine(guessParagraphs)
      state.currentLine += Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString)
    } else state.currentLine += Word(curMidX, curStartX, curEndX, curStartY, curEndY, word.toString)
    ret
  }
  
  private def processParagraph(paragraph: Seq[Seq[Word]], d: Reuse): Unit = {
    //val t = ie.beginExclusiveTransaction()
    val issueStartOffset = d.issueContents.length
    var offset = issueStartOffset
    val offsetEntry = IntegerBinding.intToCompressedEntry(d.issueContents.length)
    ics synchronized {
      for (line <- paragraph) {
        for (word <- line) {
          d.offsetData.clear()
          IntegerBinding.writeCompressed(d.offsetData,word.startX,d.offsetDataBytes)
          IntegerBinding.writeCompressed(d.offsetData,word.startY,d.offsetDataBytes)
          IntegerBinding.writeCompressed(d.offsetData,word.endX - word.startX,d.offsetDataBytes)
          IntegerBinding.writeCompressed(d.offsetData,word.endY - word.startY,d.offsetDataBytes)
          val k = new CompoundByteIterable(Array(d.issueIDEntry,IntegerBinding.intToCompressedEntry(offset)))
          val v = d.offsetData.asArrayByteIterable()
          ics.add(t,k,v)
          storedOffsets += 1
          if ((storedOffsets & 1048575) == 0)
            t.flush()
          d.articleContents.append(word.word)
          d.articleContents.append(' ')
          d.issueContents.append(word.word)
          d.issueContents.append(' ')
          offset += word.word.length + 1
        }
        if (line.nonEmpty) {
          d.articleContents.setCharAt(d.articleContents.length - 1, '\n')
          d.issueContents.setCharAt(d.articleContents.length - 1, '\n')
        }
      }
    }
    //t.commit()
    d.articleContents.append('\n')
    d.issueContents.append('\n')
    val ptext = paragraph.map(_.map(_.word).mkString(" ")).mkString("\n")
    d.sbi.setText(ptext)
    var start = d.sbi.first()
    var end = d.sbi.next()
    var csentences = 0
    while (end != BreakIterator.DONE) {
      val sentence = ptext.substring(start,end)
      if (siw != null) {
        d.sentenceIDField.setLongValue(sentences.incrementAndGet)
        d.textField.setValue(sentence)
        d.lengthFields.setValue(sentence.length)
        d.tokensFields.setValue(d.textField.numberOfTokens)
        d.startOffsetFields.setValue(issueStartOffset + start)
        d.endOffsetFields.setValue(issueStartOffset + end)
        siw.addDocument(d.sd)
      }
      start = end
      end = d.sbi.next()
      csentences += 1
    }
    d.sentencesInIssue += csentences
    d.sentencesInArticle += csentences
    d.paragraphsInArticle += 1
    d.paragraphsInIssue += 1
    if (piw != null) {
      d.sentencesFields.setValue(csentences)
      d.paragraphIDFields.setValue(paragraphs.getAndIncrement)
      d.startOffsetFields.setValue(issueStartOffset)
      d.endOffsetFields.setValue(issueStartOffset + ptext.length)
      d.textField.setValue(ptext)
      d.lengthFields.setValue(ptext.length)
      d.tokensFields.setValue(d.textField.numberOfTokens)
      piw.addDocument(d.pd)
    }
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
      var articleStartOffset = 0
      var break = false
      // read metadatainfo first. If it doesn't exist, we have a broken issue and the whole thing gets skipped
      while (xml.hasNext && !break) xml.next match {
/*        case EvElemStart(_, "issue", attrs, _) =>
          d.issueIDFields.setValue(attrs("ID").head.text) */
        case EvElemStart(_,"PSMID",_,_) =>
          val psmID = readContents
          d.issueIDFields.setValue(psmID)
          d.issueIDEntry = StringBinding.stringToEntry(psmID)
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
        case EvElemStart(_, "language", _, _) => new StringSSDVFieldPair("language").o(d.id).setValue(readContents)
        case EvElemEnd(_,"metadatainfo") => break = true
        case _ =>
      }
      while (xml.hasNext) xml.next match {
        case EvElemStart(_, "ocrLanguage", _, _) =>
          val lang = readContents
          new StringSSDVFieldPair("language").o(d.id).setValue(lang)
          d.languageFields.setValue(lang)
        case EvElemStart(_, "pc", _, _) => d.issuePagesFields.setValue(readContents.toInt)
        case EvElemStart(_, "ocr", _, _) =>
          val confidence = readContents
          val confidenceAsInt = (confidence.substring(0,confidence.indexOf('.'))+confidence.substring(confidence.indexOf('.')+1,confidence.length)).toInt
          articleConfidence += confidenceAsInt
          articleConfidenceCount += 1
          d.ocrConfidenceFields.setValue(confidenceAsInt)
        case EvElemStart(_, "article", _, _) =>
          d.clearOptionalArticleFields()
          articleStartOffset = d.issueContents.length
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
            d.id.addOptional(f)
            d.ad.addOptional(f)
          })
          val c = readContents
          if (!c.isEmpty) {
            val f = new Field("containsGraphicCaption", c, normsOmittingStoredTextField)
            d.id.addOptional(f)
            d.ad.addOptional(f)
          }
        case EvElemStart(_, "pi", _, _) =>
          new StringSSDVFieldPair("pageID").o(d.ad,d.sd,d.pd).setValue(readContents)
          d.pagesInArticle += 1
        case EvElemEnd(_, "article") =>
          val article = d.articleContents.toString
          d.ocrConfidenceFields.setValue(if (articleConfidenceCount==0) 0 else articleConfidence/articleConfidenceCount)
          d.textField.setValue(article)
          d.lengthFields.setValue(article.length)
          d.tokensFields.setValue(d.textField.numberOfTokens)
          d.paragraphsFields.setValue(d.paragraphsInArticle)
          d.articlePagesFields.setValue(d.pagesInArticle)
          d.subtitlesFields.setValue(d.subtitles.toString)
          d.illustrationsFields.setValue(d.illustrationsInArticle)
          d.sentencesFields.setValue(d.sentencesInArticle)
          d.startOffsetFields.setValue(articleStartOffset)
          d.endOffsetFields.setValue(articleStartOffset + article.length)
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
          if (state.lastLine != null) state.content.append(state.lastLine)
          if (state.content.nonEmpty)
            processParagraph(state.content, d)
          state.content.clear()
          state.lastLine = null
          state.currentLine.clear
        case _ =>
      }
      val issue = d.issueContents.toString
      if (issue.nonEmpty) {
        d.ocrConfidenceFields.setValue(issueConfidence)
        d.textField.setValue(issue)
        d.lengthFields.setValue(issue.length)
        d.tokensFields.setValue(d.textField.numberOfTokens)
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

  var ie: Environment = _
  var ics: Store = _
  var storedOffsets = 0l
  var t: Transaction = _

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val spostings = opt[String](default = Some("blocktree"))
      val ppostings = opt[String](default = Some("blocktree"))
      val ipostings = opt[String](default = Some("blocktree"))
      val apostings = opt[String](default = Some("blocktree"))
      val noSentenceIndex= opt[Boolean]()
      val noParagraphIndex = opt[Boolean]()
      val startYear = opt[String]()
      val endYear = opt[String]()
      val noClear = opt[Boolean]()
      val noMerge = opt[Boolean]()
      verify()
    }
    var divisor = 4
    if (opts.noSentenceIndex()) divisor -= 1
    if (opts.noParagraphIndex()) divisor -= 1
    if (!opts.onlyMerge()) {
      ie = Environments.newInstance(opts.index()+"/offsetdata", new EnvironmentConfig().setLogFileSize(Int.MaxValue+1l).setMemoryUsage(1073741824l))
      if (!opts.noClear()) ie.clear()
      t = ie.beginExclusiveTransaction()
      ics = ie.openStore("offsetdata", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, t)
      t.commit()
      t = ie.beginExclusiveTransaction()
      startYear = opts.startYear.toOption.map(v => (v+"0000").toInt)
      endYear = opts.endYear.toOption.map(v => (v+"9999").toInt)
      if (!opts.noSentenceIndex()) siw = iw(opts.index()+"/sindex",ss,opts.indexMemoryMb() / divisor, !opts.noClear())
      if (!opts.noParagraphIndex()) piw = iw(opts.index()+"/pindex",ps,opts.indexMemoryMb() / divisor, !opts.noClear())
      aiw = iw(opts.index()+"/aindex",as,opts.indexMemoryMb() / divisor, !opts.noClear())
      iiw = iw(opts.index()+"/iindex",is,opts.indexMemoryMb() / divisor, !opts.noClear())
      /* val t = ie.beginExclusiveTransaction()
      opts.directories().foreach(p => {
        val parts = p.split(':')
        getFileTree(new File(parts(0))).filter(_.getName.endsWith(".xml")).foreach(f => addTask(f.getPath, () => index(parts(1),f)))
      })
      t.commit() */
      feedAndProcessFedTasksInParallel(() =>
        opts.directories().foreach(p => {
          val parts = p.split(':')
          getFileTree(new File(parts(0))).filter(_.getName.endsWith(".xml")).foreach(f => addTask(f.getPath, () => index(parts(1),f)))
        })
      )
      t.commit()
      ie.close()
    }
    waitForTasks(
      runSequenceInOtherThread(
        () => if (siw != null) close(siw),
        () => if (siw != null && !opts.noMerge()) merge(opts.index()+"/sindex", ss,opts.indexMemoryMb() / divisor, toCodec(opts.spostings(), postingsFormats))
      ),
      runSequenceInOtherThread(
        () => if (piw != null) close(piw),
        () => if (piw != null && !opts.noMerge()) merge(opts.index()+"/pindex", ps,opts.indexMemoryMb() / divisor, toCodec(opts.ppostings(), postingsFormats))
      ),
      runSequenceInOtherThread(
        () => close(aiw), 
        () => if (!opts.noMerge()) merge(opts.index()+"/aindex", as,opts.indexMemoryMb() / divisor, toCodec(opts.apostings(), postingsFormats))
      ),
      runSequenceInOtherThread(
        () => close(iiw), 
        () => if (!opts.noMerge()) merge(opts.index()+"/iindex", is,opts.indexMemoryMb() / divisor, toCodec(opts.ipostings(), postingsFormats))
      )
    )
  }
}
