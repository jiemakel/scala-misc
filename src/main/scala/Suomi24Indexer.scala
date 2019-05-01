import java.io.File
import java.util.concurrent.atomic.AtomicLong

import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, OffsetAttribute, PositionIncrementAttribute}
import org.apache.lucene.document._
import org.apache.lucene.index.{FieldInfo, IndexWriter}
import org.apache.lucene.search.{Sort, SortField}
import org.apache.lucene.util.BytesRef
import org.joda.time.format.ISODateTimeFormat
import org.rogach.scallop._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.language.{postfixOps, reflectiveCalls}

case class LBWordToAnalysis(word: String, analysis: Seq[String] = null) {
}

class LBAnalysisTokenStream(var tokens: Iterable[(Int,LBWordToAnalysis)] = null) extends TokenStream {

  private val termAttr: CharTermAttribute = addAttribute(classOf[CharTermAttribute])
  private val posAttr: PositionIncrementAttribute = addAttribute(classOf[PositionIncrementAttribute])
  private val offAttr: OffsetAttribute = addAttribute(classOf[OffsetAttribute])

  private var wordsIterator: Iterator[(Int,LBWordToAnalysis)] = _

  override def reset(): Unit = {
    wordsIterator = tokens.iterator
    analysesIterator = Iterator.empty
  }

  private var analysesIterator: Iterator[String] = _

  private var startOffset = -1
  private var endOffset = -1

  final override def incrementToken(): Boolean = {
    clearAttributes()
    val analysisToken = if (!analysesIterator.hasNext) { // end of analyses
      if (!wordsIterator.hasNext) return false // end of words
      val (offsetIncr,n) = wordsIterator.next // next word
      analysesIterator = n.analysis.iterator
      posAttr.setPositionIncrement(1)
      startOffset = endOffset + offsetIncr
      endOffset = startOffset + n.word.length
      "W="+n.word
    } else {
      posAttr.setPositionIncrement(0)
      analysesIterator.next // next analysis
    }
    offAttr.setOffset(startOffset, endOffset)
    termAttr.append(analysisToken)
    true
  }
}

object Suomi24Indexer extends OctavoIndexer {

  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }

  class Reuse {
    var scontents = new ArrayBuffer[LBWordToAnalysis]
    val scontentS = new StringBuilder
    var parcontents = new ArrayBuffer[Seq[LBWordToAnalysis]]
    val parcontentS = new StringBuilder
    var postcontents = new ArrayBuffer[Seq[Seq[LBWordToAnalysis]]]
    val postcontentS = new StringBuilder
    val tcontents = new ArrayBuffer[Seq[Seq[Seq[LBWordToAnalysis]]]]
    val tcontentS = new StringBuilder

    val td = new FluidDocument()
    val postd = new FluidDocument()
    val pard = new FluidDocument()
    val send = new FluidDocument()

    val threadIDFields = new StringNDVFieldPair("threadID").r(td,postd,pard,send)
    val postIDFields = new StringNDVFieldPair("postID").r(postd,pard,send)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID").r(pard,send)
    val sentenceIDField = new NumericDocValuesField("sentenceID", 0)
    send.addRequired(sentenceIDField)
    val contentField = new Field("content","",contentFieldType)
    td.addRequired(contentField)
    postd.addRequired(contentField)
    pard.addRequired(contentField)
    send.addRequired(contentField)
    val analysisField = new StoredField("analysis", "")
    td.addRequired(analysisField)
    postd.addRequired(analysisField)
    pard.addRequired(analysisField)
    send.addRequired(analysisField)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength").r(td, postd, pard, send)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens").r(td, postd, pard, send)
    val sectionFields = new StringSDVFieldPair("section").r(td,postd,pard,send)
    val threadDateStartFields = new LongPointSDVDateTimeFieldPair("startdatetime",ISODateTimeFormat.basicDateTimeNoMillis).r(td,postd,pard,send)
    val threadDateEndFields = new LongPointSDVDateTimeFieldPair("enddatetime",ISODateTimeFormat.basicDateTime).r(td,postd,pard,send)
    val dateTimeFields = new LongPointSDVDateTimeFieldPair("datetime",ISODateTimeFormat.basicDateTimeNoMillis).r(postd,pard,send)
    val commentsFields = new IntPointNDVFieldPair("comments").r(td,postd,pard,send)
    val paragraphsFields = new IntPointNDVFieldPair("paragraphs").r(td,postd)
    val sentencesFields = new IntPointNDVFieldPair("sentences").r(td,postd)
    val viewsFields = new IntPointNDVFieldPair("views").r(postd,pard,send)
    var urlFields = new StringSDVFieldPair("url").r(td,postd,pard,send)
    val headingFields = new StringSDVFieldPair("heading").r(td,postd,pard,send)
    val nickFields = new StringSDVFieldPair("nick").r(postd,pard,send)
    val isThreadStarterFields = new StringSDVFieldPair("isThreadStarter").r(postd,pard,send)
    def clearMultiThreadFields() {
      td.clearOptional()
      postd.clearOptional()
      pard.clearOptional()
      send.clearOptional()
/*      removeFields("subsection", td, postd, pard, send)
      removeFields("nick", td)
      removeFields("datetime", td) */
    }
  }

  val termVectorFields = Seq("content")

  indexingCodec.termVectorFilter = (f: FieldInfo, b: BytesRef) => b.bytes(b.offset) == 'L'

  private def trimSpace(value: String): String = {
    if (value==null) return null
    var len = value.length
    var st = 0
    while (st < len && (value(st) == ' ' || value(st) == '\n')) st += 1
    while (st < len && (value(len - 1) == ' ' || value(len -1) == '\n')) len -= 1
    if ((st > 0) || (len < value.length)) value.substring(st, len)
    else value
  }

  val headingR = "title=\"([^\"]*)\"".r
  val sectionR = "discussionarea=\"([^\"]*)\"".r
  val viewsR = "views=\"([0-9]+)\"".r
  val commsR = "comms=\"([0-9]*)\"".r
  val datefromR = "datefrom=\"([0-9]{8})\"".r
  val datetoR = "datefrom=\"([0-9]{8})\"".r
  val timeR = "time=\"([0-9]{2}):([0-9]{2})\"".r
  val urlR = "urlmsg=\"([^\"]*)\"".r
  val nickR = "anonnick=\"([^\"]*)\"".r
  val ssectionR = "subsect=\"([^\"]+)\"".r

  private val postsc = new AtomicLong
  private val paragraphs = new AtomicLong
  private val sentences = new AtomicLong

  private def appendLine(line: String, sbs: StringBuilder*) = for (sb <- sbs) {
    sb.append(line)
    sb.append('\n')
  }

  private def indexThread(tid: Int, tinfo: String, posts: Seq[(String,Seq[String])]): Unit = {
    val r = tld.get
    r.tcontents.clear()
    r.tcontentS.clear()
    appendLine(tinfo,r.tcontentS)
    r.clearMultiThreadFields()
    r.threadIDFields.setValue(tid)
    r.commentsFields.setValue(commsR.findFirstMatchIn(tinfo).get.group(1).toInt)
    r.sectionFields.setValue(sectionR.findFirstMatchIn(tinfo).get.group(1))
    for (ssection <- ssectionR.findAllMatchIn(tinfo).map(_.group(1)))
      new StringSSDVFieldPair("subsection").o(r.td,r.postd,r.pard,r.send).setValue(ssection)
    r.threadDateStartFields.setValue(datefromR.findFirstMatchIn(tinfo).get.group(1)+"T000000Z")
    r.threadDateEndFields.setValue(datetoR.findFirstMatchIn(tinfo).get.group(1)+"T235959.999Z")
    r.isThreadStarterFields.setValue("T")
    for ((pinfo,post) <- posts) {
      val postID = postsc.incrementAndGet()
      if (postID % 10000 == 0) logger.info(postID+": "+tid)
      r.postIDFields.setValue(postID)
      r.postcontents = new ArrayBuffer[Seq[Seq[LBWordToAnalysis]]](r.postcontents.length)
      r.postcontentS.clear()
      appendLine(pinfo,r.postcontentS,r.tcontentS)
      r.headingFields.setValue(headingR.findFirstMatchIn(pinfo).get.group(1))
      r.viewsFields.setValue(viewsR.findFirstMatchIn(pinfo).map(_.group(1).toInt).getOrElse(0))
      val timeM = timeR.findFirstMatchIn(pinfo).get
      val datetime = datefromR.findFirstMatchIn(pinfo).get.group(1)+"T"+timeM.group(1)+timeM.group(2)+"00Z"
      r.dateTimeFields.setValue(datetime)
      new LongPointSSDVDateTimeFieldPair("datetime",ISODateTimeFormat.basicDateTimeNoMillis).o(r.td).setValue(datetime)
      r.urlFields.setValue(urlR.findFirstMatchIn(pinfo).get.group(1))
      val nick = nickR.findFirstMatchIn(pinfo).get.group(1)
      r.nickFields.setValue(nick)
      new StringSSDVFieldPair("nick").o(r.td).setValue(nick)
      for (line <- post.dropRight(1)) { // drop </text>
        if (line.startsWith("<paragraph")) {
          r.parcontentS.clear()
          appendLine(line,r.tcontentS,r.postcontentS,r.parcontentS)
          r.paragraphIDFields.setValue(paragraphs.incrementAndGet())
          r.parcontents = new ArrayBuffer[Seq[LBWordToAnalysis]](r.parcontents.length)
        } else if (line.startsWith("<sentence")) {
          r.scontentS.clear()
          appendLine(line,r.tcontentS,r.postcontentS,r.parcontentS,r.scontentS)
          r.sentenceIDField.setLongValue(sentences.incrementAndGet())
          r.scontents = new ArrayBuffer[LBWordToAnalysis](r.scontents.length)
        } else if (line == "</paragraph>") {
          appendLine(line,r.tcontentS,r.postcontentS,r.parcontentS)
          if (r.parcontents.nonEmpty) {
            val parcontents = r.parcontents.flatten
            val content = parcontents.map(_.word).mkString(" ")
            r.contentField.setStringValue(content)
            r.contentField.setTokenStream(new LBAnalysisTokenStream(parcontents.map((1, _))))
            r.contentLengthFields.setValue(content.length)
            r.contentTokensFields.setValue(parcontents.length)
            r.sentencesFields.setValue(r.parcontents.length)
            r.analysisField.setStringValue(r.parcontentS.toString)
            pariw.addDocument(r.pard)
            r.postcontents += r.parcontents
          }
        } else if (line == "</sentence>") {
          appendLine(line,r.tcontentS,r.postcontentS,r.parcontentS,r.scontentS)
          if (r.scontents.nonEmpty) {
            val content = r.scontents.map(_.word).mkString(" ")
            r.contentField.setStringValue(content)
            r.contentField.setTokenStream(new LBAnalysisTokenStream(r.scontents.map((1, _))))
            r.contentLengthFields.setValue(content.length)
            r.contentTokensFields.setValue(r.scontents.length)
            r.analysisField.setStringValue(r.scontentS.toString)
            seniw.addDocument(r.send)
            r.parcontents += r.scontents
          }
        } else {
          appendLine(line,r.tcontentS,r.postcontentS,r.parcontentS,r.scontentS)
          val parts = line.split("\t")
          var analysis: ArrayBuffer[String] = new ArrayBuffer[String]
          if (parts.length >= 3)
            analysis += "L="+parts(2) else analysis += "L="+parts(0) // else also guards against segments with no terms for termvector
          if (parts.length >= 4)
            analysis += "POS="+parts(3)
          if (parts.length >= 5)
            analysis ++= parts(4).split("\\|").filter(_!="_").map("A="+_)
          if (parts.length >= 7) {
            analysis += "HEAD=" + parts(5)
            analysis += "DEPREL=" + parts(6)
          }
          if (parts.length >= 8 && parts(7) != "_")
            analysis += "TAG=" + parts(7)
          r.scontents += LBWordToAnalysis(parts(0), analysis)
        }
      }
      appendLine("</text>",r.tcontentS,r.postcontentS)
      if (r.postcontents.nonEmpty) {
        val content = r.postcontents.map(_.flatten.map(_.word).mkString(" ")).mkString("\n\n")
        r.contentField.setStringValue(content)
        val postcontents = new ArrayBuffer[(Int,LBWordToAnalysis)]
        for (par <- r.postcontents) {
          val pari = par.iterator
          val pfs = pari.next
          val pfsi = pfs.iterator
          postcontents += ((2, pfsi.next))
          for (w <- pfsi) postcontents += ((1, w))
          for (w <- pari.flatten) postcontents += ((1, w))
        }
        postcontents(0) = (1,postcontents(0)._2)
        r.contentField.setTokenStream(new LBAnalysisTokenStream(postcontents))
        r.contentLengthFields.setValue(content.length)
        r.contentTokensFields.setValue(postcontents.length)
        r.paragraphsFields.setValue(r.postcontents.length)
        r.sentencesFields.setValue(r.postcontents.foldLeft(0){(l,v) => l+v.length})
        r.analysisField.setStringValue(r.postcontentS.toString)
        postiw.addDocument(r.postd)
        r.tcontents += r.postcontents
      }
      r.isThreadStarterFields.setValue("F")
    }
    if (r.tcontents.nonEmpty) {
      val content = r.tcontents.map(_.map(_.flatten.map(_.word).mkString(" ")).mkString("\n\n")).mkString("\n\n====\n\n")
      r.contentField.setStringValue(content)
      val threadcontents = new ArrayBuffer[(Int, LBWordToAnalysis)]
      for (post <- r.tcontents) {
        val posti = post.iterator
        val fpost = posti.next
        val pari = fpost.iterator
        val pfs = pari.next
        val pfsi = pfs.iterator
        threadcontents += ((8, pfsi.next))
        for (w <- pfsi) threadcontents += ((1, w))
        for (w <- pari.flatten) threadcontents += ((1, w))
        for (par <- posti) {
          val pari = par.iterator
          val pfs = pari.next
          val pfsi = pfs.iterator
          threadcontents += ((2, pfsi.next))
          for (w <- pfsi) threadcontents += ((1, w))
          for (w <- pari.flatten) threadcontents += ((1, w))
        }
      }
      threadcontents(0) = (1, threadcontents(0)._2)
      r.contentField.setTokenStream(new LBAnalysisTokenStream(threadcontents))
      r.contentLengthFields.setValue(content.length)
      r.contentTokensFields.setValue(threadcontents.length)
      r.paragraphsFields.setValue(r.tcontents.foldLeft(0){(l,v) => l+v.length})
      r.sentencesFields.setValue(r.tcontents.foldLeft(0){(l,v) => l+v.foldLeft(0){(l2,v2) => l2+v2.length}})
      r.analysisField.setStringValue(r.tcontentS.toString)
      tiw.addDocument(r.td)
    }
  }

  val tidR = "tid=\"([0-9]*)\"".r

  private def index(file: File): Unit = {
    val id = file.getPath
    logger.info("Processing: "+file)
    val fl = Source.fromFile(file)
    var cthread = new ArrayBuffer[(String,Seq[String])]
    var cpost = new ArrayBuffer[String]
    var ctid : String = null
    var ctinfo: String = null
    for (line <- fl.getLines) {
      if (line.startsWith("<text ")) {
        val ntid = tidR.findFirstMatchIn(line).get.group(1)
        if (ctid != ntid) {
          if (ctid!=null) {
            val mytid = ctid.toInt
            val mytinfo = ctinfo
            val mythread = cthread
            addTask(id + ":" + mytid, () => indexThread(mytid, mytinfo, mythread))
          }
          ctid = ntid
          ctinfo = line
          cthread = new ArrayBuffer[(String,Seq[String])](cthread.length)
        }
        cpost = new ArrayBuffer[String](cpost.length)
        cthread += ((line,cpost))
      } else cpost += line
    }
    if (ctid != null) addTask(id, () => indexThread(ctid.toInt,ctinfo,cthread))
    fl.close()
    logger.info("Processed: "+file)
  }

  var tiw, postiw, pariw, seniw = null.asInstanceOf[IndexWriter]

  val ts = new Sort(new SortField("threadID",SortField.Type.LONG))
  val posts = new Sort(new SortField("threadID",SortField.Type.LONG), new SortField("postID", SortField.Type.LONG))
  val pars = new Sort(new SortField("threadID",SortField.Type.LONG), new SortField("postID", SortField.Type.LONG), new SortField("parID", SortField.Type.LONG))
  val sens = new Sort(new SortField("threadID",SortField.Type.LONG), new SortField("postID", SortField.Type.LONG), new SortField("parID", SortField.Type.LONG), new SortField("sentenceID", SortField.Type.LONG))

  val themes = new mutable.HashMap[Int,String]
  val poemThemes = new mutable.HashMap[String,ArrayBuffer[Int]]
  val places = new mutable.HashMap[Int,(String,String)]
  val collectors = new mutable.HashMap[Int,String]
  val metadata = new mutable.HashMap[String,(Int,Int,Int)]

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val tpostings = opt[String](default = Some("blocktree"))
      val postpostings = opt[String](default = Some("blocktree"))
      val parpostings = opt[String](default = Some("blocktree"))
      val senpostings = opt[String](default = Some("blocktree"))
      verify()
    }
    tiw = iw(opts.index()+"/tindex", ts, opts.indexMemoryMb()/4)
    postiw = iw(opts.index()+"/postindex", posts, opts.indexMemoryMb()/4)
    pariw = iw(opts.index()+"/parindex", pars, opts.indexMemoryMb()/4)
    seniw = iw(opts.index()+"/senindex", sens, opts.indexMemoryMb()/4)
    feedAndProcessFedTasksInParallel(() => {
      opts.directories().toStream.flatMap(p => getFileTree(new File(p)))
        .filter(_.getName.endsWith(".cvrt")).foreach(file => index(file))
    })
    waitForTasks(
      runSequenceInOtherThread(
        () => close(tiw),
        () => merge(opts.index()+"/tindex", ts, opts.indexMemoryMb()/4, toCodec(opts.tpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(postiw),
        () => merge(opts.index()+"/postindex", posts, opts.indexMemoryMb()/4, toCodec(opts.postpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(pariw),
        () => merge(opts.index()+"/parindex", pars, opts.indexMemoryMb()/4, toCodec(opts.parpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(seniw),
        () => merge(opts.index()+"/senindex", sens, opts.indexMemoryMb()/4, toCodec(opts.senpostings(), termVectorFields))
      )
    )
  }
}
