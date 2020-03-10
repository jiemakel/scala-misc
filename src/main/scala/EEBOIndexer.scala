import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.text.BreakIterator
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import com.brein.time.timeintervals.collections.ListIntervalCollection
import com.brein.time.timeintervals.indexes.{IntervalTree, IntervalTreeBuilder}
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder.IntervalType
import com.brein.time.timeintervals.intervals.IntegerInterval
import com.sleepycat.je._
import org.apache.lucene.document.{Document, Field, NumericDocValuesField}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.rogach.scallop._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.language.{postfixOps, reflectiveCalls}
import scala.xml.parsing.XhtmlEntities
import XMLEventReaderSupport._
import com.github.tototoshi.csv.CSVReader
import javax.xml.stream.XMLEventReader

object EEBOIndexer extends OctavoIndexer {

  private def readContents(implicit xml: Iterator[EvEvent]): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_) => return null
      case EvText(text) => content.append(text)
      case EvEntityRef(entity) => XhtmlEntities.entMap.get(entity) match {
        case Some(chr) => content.append(chr)
        case _ => content.append(entity)
      }
      case EvComment(_) =>
      case EvElemEnd(_,_) => break = true
    }
    content.toString
  }

  private val sentences = new AtomicLong
  private val paragraphs = new AtomicLong
  private val sections = new AtomicLong
  private val documentparts = new AtomicLong
  private val works = new AtomicLong

  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }

  class ReuseInterval(startIndex: Int, endIndex: Int, val reuseID: Long) extends IntegerInterval(startIndex,endIndex, false, true) {

  }

  class Reuse {
    val bookToClusters = bookToClustersDB.openCursor(null, null)
    val bckeya = new Array[Byte](java.lang.Long.BYTES)
    val bcbb = ByteBuffer.wrap(bckeya)
    val bckey = new DatabaseEntry(bckeya)
    val bcval = new DatabaseEntry
    val sbi = BreakIterator.getSentenceInstance(new Locale("en_GB"))
    val dd = new FluidDocument()
    val wd = new FluidDocument()
    val dpd = new FluidDocument()
    val sd = new FluidDocument()
    val pd = new FluidDocument()
    val send = new FluidDocument()
    val collectionIDFields = new StringSDVFieldPair("collectionID").r(dd, wd, dpd, sd, pd, send)
    val documentIDFields = new StringSDVFieldPair("documentID").r(dd, wd, dpd, sd, pd, send)
    val estcIDFields = new StringSDVFieldPair("ESTCID").r(dd, wd, dpd, sd, pd, send)
    val vidFields = new StringSDVFieldPair("VID").r(dd, wd, dpd, sd, pd, send)
    val totalPagesFields = new IntPointNDVFieldPair("totalPages").r(dd, wd, dpd, sd, pd, send)
    val languageFields = new StringSDVFieldPair("language").r(dd, wd, dpd, sd, pd, send)
    val fullTitleFields = new TextSDVFieldPair("fullTitle").r(dd, wd,dpd,sd,pd, send)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength").r(dd, wd, dpd, sd, pd, send)
    val documentLengthFields = new IntPointNDVFieldPair("documentLength").r(dd, wd, dpd, sd, pd, send)
    val totalParagraphsFields = new IntPointNDVFieldPair("totalParagraphs").r(dd, wd, dpd, sd, pd, send)
    val startOffsetFields = new IntPointNDVFieldPair("startOffset").r(wd, dpd, sd, pd, send)
    val endOffsetFields = new IntPointNDVFieldPair("endOffset").r(wd, dpd, sd, pd, send)
    val reusesFields = new IntPointNDVFieldPair("reuses").r(dd, wd, dpd, sd, pd, send)
    def clearOptionalDocumentFields() {
      totalPagesFields.setValue(0)
      languageFields.setValue("")
      fullTitleFields.setValue("")
      dd.clearOptional()
      dd.removeRequiredFields("altTitle")
      wd.removeRequiredFields("altTitle")
      dpd.removeRequiredFields("altTitle")
      sd.removeRequiredFields("altTitle")
      pd.removeRequiredFields("altTitle")
      send.removeRequiredFields("altTitle")
/*      dd.removeFields("containsGraphicOfType")
      dd.removeFields("containsGraphicCaption")
      dd.removeFields("reuseID") */
    }
    def clearOptionalWorkFields(): Unit = {
      wd.clearOptional()
/*      wd.removeFields("containsGraphicOfType")
      wd.removeFields("containsGraphicCaption")
      wd.removeFields("reuseID") */
    }
    def clearOptionalDocumentPartFields() {
      dpd.clearOptional()
/*      dpd.removeFields("containsGraphicOfType")
      dpd.removeFields("containsGraphicCaption")
      dpd.removeFields("reuseID") */
    }
    def clearOptionalSectionFields() {
      sd.clearOptional()
/*      sd.removeFields("reuseID") */
    }
    def clearOptionalParagraphFields() {
      pd.clearOptional()
/*      pd.removeFields("sectionID")
      pd.removeFields("headingLevel")
      pd.removeFields("heading")
      pd.removeFields("reuseID") */
    }
    def clearOptionalSentenceFields() {
      send.clearOptional()
/*      send.removeFields("sectionID")
      send.removeFields("headingLevel")
      send.removeFields("heading")
      send.removeFields("reuseID") */
    }
    def addAltTitle(altTitle: String): Unit = {
      new TextSSDVFieldPair("altTitle").r(dd, wd, dpd, sd, pd, send).setValue(altTitle)
    }
    val workIdFields = new StringNDVFieldPair("workID").r(wd, dpd, sd, pd, send)
    val documentPartIdFields = new StringNDVFieldPair("partID").r(dpd, sd, pd, send)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID").r(pd, send)
    val sentenceIDField = new NumericDocValuesField("sentenceID", 0)
    send.addRequired(sentenceIDField)
    val contentField = new Field("content","",contentFieldType)
    dd.addRequired(contentField)
    wd.addRequired(contentField)
    dpd.addRequired(contentField)
    sd.addRequired(contentField)
    pd.addRequired(contentField)
    send.addRequired(contentField)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens").r(dd, dpd, sd, pd, send)
    val documentPartTypeFields = new StringSDVFieldPair("documentPartType").r(dpd, sd, pd, send)
    val sectionIDFields = new StringNDVFieldPair("sectionID").r(sd)
    val headingFields = new TextSDVFieldPair("heading").r(sd)
    val headingLevelFields = new IntPointNDVFieldPair("headingLevel").r(sd)
  }

  val termVectorFields = Seq("content", "containsGraphicOfType")

  class SectionInfo(val sectionID: Long, val headingLevel: Int, val heading: String) {
    val paragraphSectionIDFields = new StringSNDVFieldPair("sectionID")
    paragraphSectionIDFields.setValue(sectionID)
    val paragraphHeadingLevelFields = new IntPointSNDVFieldPair("headingLevel")
    paragraphHeadingLevelFields.setValue(headingLevel)
    val paragraphHeadingFields = new TextSSDVFieldPair("heading")
    val content = new StringBuilder()
    def addToDocument(pd: FluidDocument) {
      paragraphSectionIDFields.o(pd)
      paragraphHeadingLevelFields.o(pd)
      paragraphHeadingFields.o(pd)
    }
  }

  private def trimSpace(value: String): String = {
    if (value==null) return null
    var len = value.length
    var st = 0
    while (st < len && (value(st) == ' ' || value(st) == '\n')) st += 1
    while (st < len && (value(len - 1) == ' ' || value(len -1) == '\n')) len -= 1
    if ((st > 0) || (len < value.length)) value.substring(st, len)
    else value
  }

  private val treeBuilder = IntervalTreeBuilder.newBuilder()
    .usePredefinedType(IntervalType.INTEGER)
    .collectIntervals(_ => new ListIntervalCollection())

  private val dpmatcher = "^[^_]*_([0-9]*)_text_([0-9]*)_([^_]*).txt$".r

  private case class DocumentPartFile(workNumber: Int, documentPartNumber: Int, filename: String) {
  }

  private def index(cid: String, filePrefix: String): Unit = {
    logger.info("Processing: "+filePrefix+"*")
    val did = filePrefix.substring(filePrefix.lastIndexOf('/')+1)
    val xmls = new FileInputStream(filePrefix+".headed.metadata.xml")
    implicit val xml = getXMLEventReader(xmls)
    val r = tld.get
    r.clearOptionalDocumentFields()
    r.collectionIDFields.setValue(cid)
    var documentID: String = "1"+(did(0) match {
      case 'A' => "0"
      case 'B' => "1"
    }) + did.substring(1)
    r.documentIDFields.setValue(documentID)
    val documentClusters: IntervalTree = treeBuilder.build()
    r.bcbb.putLong(0, documentID.toLong)
    if (r.bookToClusters.getSearchKey(r.bckey, r.bcval, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
      val vbb = ByteBuffer.wrap(r.bcval.getData)
      documentClusters.add(new ReuseInterval(vbb.getInt,vbb.getInt,vbb.getLong))
      while (r.bookToClusters.getNextDup(r.bckey, r.bcval, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
        val vbb = ByteBuffer.wrap(r.bcval.getData)
        documentClusters.add(new ReuseInterval(vbb.getInt,vbb.getInt,vbb.getLong))
      }
    }
    var estcID: String = null
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"IDNO",attr) if attr.get("TYPE").contains("stc") =>
        val tdocumentID = trimSpace(readContents)
        if (tdocumentID.startsWith("ESTC ")) {
          estcID = tdocumentID.substring(5)
          r.estcIDFields.setValue(estcID)
        }
      case EvElemStart(_,"VID",_) =>
        r.vidFields.setValue(trimSpace(readContents))
      case EvElemStart(_,"LANGUSAGE",attr) =>
        r.languageFields.setValue(attr("ID"))
      case EvElemStart(_,"TITLE",attr) if attr("TYPE") != null && attr("TYPE") == "alt" =>
        r.addAltTitle(trimSpace(readContents))
      case EvElemStart(_,"TITLE",attr) if attr("TYPE") != null && attr("TYPE") == "245" =>
        r.fullTitleFields.setValue(trimSpace(readContents))
      case _ =>
    }
    xmls.close()
    if (estcID==null) logger.error("No ESTC ID for "+filePrefix)
    val dcontents = new StringBuilder
    var lastPage = 0
    val filesToProcess = new ArrayBuffer[(Int,Int,String,File)]
    for (file <- getFileTree(new File(filePrefix).getParentFile); if file.getName.endsWith(".txt") && file.getName.startsWith(did)) if (file.getName.contains("_page"))
      for (curPage <- "_page([0-9]+)".r.findFirstMatchIn(file.getName).map(_.group(1).toInt); if curPage>lastPage) lastPage = curPage
    else file.getName match {
      case dpmatcher(workNumber,partNumber,partType) => filesToProcess += ((workNumber.toInt,partNumber.toInt,partType,file))
      case _ =>
    }
    r.totalPagesFields.setValue(lastPage)
    var totalParagraphs = 0
    var totalLength = 0
    for ((_,_,_,file) <- filesToProcess;line <- Source.fromFile(file).getLines) {
      if (line.isEmpty) totalParagraphs += 1
      totalLength += line.length
    }
    r.documentLengthFields.setValue(totalLength)
    r.totalParagraphsFields.setValue(totalParagraphs)
    val workToFiles = filesToProcess.groupBy(_._1)
    for (workId <- workToFiles.keys.toSeq.sorted) {
      val wcontents = new StringBuilder
      r.clearOptionalWorkFields()
      r.workIdFields.setValue(works.incrementAndGet)
      val workParts = workToFiles(workId)
      for ((wn, pn, partType, file) <- workParts.sortBy(_._2)) {
        logger.info("Processing "+wn+","+pn+","+partType+","+file)
        val dpcontents = new StringBuilder
        r.clearOptionalDocumentPartFields()
        r.documentPartIdFields.setValue(documentparts.incrementAndGet)
        r.documentPartTypeFields.setValue(partType)
        if (new File(file.getPath.replace(".txt", "-graphics.csv")).exists)
          for (row <- CSVReader.open(file.getPath.replace(".txt", "-graphics.csv"))) {
            val gtype = if (row(1) == "") "unknown" else row(1)
            val f = new Field("containsGraphicOfType", gtype, notStoredStringFieldWithTermVectors)
            r.dpd.addOptional(f)
            r.dd.addOptional(f)
            if (row(3) != "") {
              val f = new Field("containsGraphicCaption", row(3), normsOmittingStoredTextField)
              r.dpd.addOptional(f)
              r.dd.addOptional(f)
            }
          }
        val headingInfos = new ArrayBuffer[Seq[SectionInfo]]
        val pcontents = new StringBuilder
        val fl = Source.fromFile(file)
        for (line <- fl.getLines) {
          if (line.isEmpty) {
            if (pcontents.nonEmpty) {
              val c = pcontents.toString
              r.clearOptionalParagraphFields()
              r.paragraphIDFields.setValue(paragraphs.incrementAndGet)
              r.contentField.setStringValue(c)
              r.contentLengthFields.setValue(c.length)
              r.contentTokensFields.setValue(getNumberOfTokens(c))
              headingInfos.foreach(_.foreach(hi => {
                hi.addToDocument(r.pd)
                hi.addToDocument(r.send)
              }))
              r.startOffsetFields.setValue(dcontents.size + dpcontents.size - c.length)
              r.endOffsetFields.setValue(dcontents.size + dpcontents.size)
              val reuses: Seq[ReuseInterval] = documentClusters.overlap(new IntegerInterval(dcontents.size + dpcontents.size - c.length, dcontents.size + dpcontents.size, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
              r.reusesFields.setValue(reuses.size)
              for (reuse <- reuses) {
                val f = new StringSNDVFieldPair("reuseID").o(r.pd)
                f.setValue(reuse.reuseID)
              }
              piw.addDocument(r.pd)
              r.sbi.setText(c)
              var start = r.sbi.first()
              var end = r.sbi.next()
              while (end != BreakIterator.DONE) {
                r.clearOptionalSentenceFields()
                val sentence = c.substring(start, end)
                r.sentenceIDField.setLongValue(sentences.incrementAndGet)
                r.contentField.setStringValue(sentence)
                r.contentLengthFields.setValue(sentence.length)
                r.contentTokensFields.setValue(getNumberOfTokens(sentence))
                r.startOffsetFields.setValue(dcontents.size + dpcontents.size - c.length + start)
                r.endOffsetFields.setValue(dcontents.size + dpcontents.size - c.length + end)
                val reuses: Seq[ReuseInterval] = documentClusters.overlap(new IntegerInterval(dcontents.size + dpcontents.size - c.length + start, dcontents.size + dpcontents.size - c.length + end, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
                r.reusesFields.setValue(reuses.size)
                for (reuse <- reuses) {
                  val f = new StringSNDVFieldPair("reuseID").o(r.send)
                  f.setValue(reuse.reuseID)
                }
                seniw.addDocument(r.send)
                start = end
                end = r.sbi.next()
              }
              pcontents.clear()
            }
          } else {
            pcontents.append(line)
            pcontents.append('\n')
          }
          if (line.startsWith("# ") || line.startsWith("## ") || line.startsWith("### ")) {
            r.clearOptionalSectionFields()
            val level =
              if (line.startsWith("### ")) 2
              else if (line.startsWith("## ")) 1
              else 0
            for (
              i <- level until headingInfos.length;
              headingInfoOpt = headingInfos(i)) {
              for (headingInfo <- headingInfoOpt) {
                r.sectionIDFields.setValue(headingInfo.sectionID)
                r.headingLevelFields.setValue(headingInfo.headingLevel)
                r.headingFields.setValue(headingInfo.heading)
                if (headingInfo.content.nonEmpty) {
                  val contentS = headingInfo.content.toString
                  r.startOffsetFields.setValue(dcontents.size + dpcontents.size - contentS.length)
                  r.endOffsetFields.setValue(dcontents.size + dpcontents.size)
                  val reuses: Seq[ReuseInterval] = documentClusters.overlap(new IntegerInterval(dcontents.size + dpcontents.size - contentS.length, dcontents.size + dpcontents.size, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
                  r.reusesFields.setValue(reuses.size)
                  for (reuse <- reuses) {
                    val f = new StringSNDVFieldPair("reuseID").o(r.sd)
                    f.setValue(reuse.reuseID)
                  }
                  r.contentField.setStringValue(contentS)
                  r.contentLengthFields.setValue(contentS.length)
                  r.contentTokensFields.setValue(getNumberOfTokens(contentS))
                }
                siw.addDocument(r.sd)
              }
              headingInfos(i) = Seq.empty
            }
            while (headingInfos.length - 1 < level) headingInfos += Seq.empty
            headingInfos(level) = headingInfos(level) :+ new SectionInfo(sections.incrementAndGet, level, line.substring(level + 2))
            for (i <- 0 until level) headingInfos(i).foreach(_.content.append(line))
          } else
            headingInfos.foreach(_.foreach(_.content.append(line)))
          dpcontents.append(line)
          dpcontents.append('\n')
        }
        fl.close()
        if (pcontents.nonEmpty) {
          val c = pcontents.toString
          r.clearOptionalParagraphFields()
          r.paragraphIDFields.setValue(paragraphs.incrementAndGet)
          r.contentField.setStringValue(c)
          r.contentLengthFields.setValue(c.length)
          r.contentTokensFields.setValue(getNumberOfTokens(c))
          var hasHeadingInfos = false
          headingInfos.foreach(_.foreach(hi => {
            hi.addToDocument(r.pd)
            hi.addToDocument(r.send)
            hasHeadingInfos = true
          }))
          r.startOffsetFields.setValue(dcontents.size + dpcontents.size - c.length)
          r.endOffsetFields.setValue(dcontents.size + dpcontents.size)
          val reuses: Seq[ReuseInterval] = documentClusters.overlap(new IntegerInterval(dcontents.size + dpcontents.size - c.length, dcontents.size + dpcontents.size, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
          r.reusesFields.setValue(reuses.size)
          for (reuse <- reuses) {
            val f = new StringSNDVFieldPair("reuseID").o(r.pd)
            f.setValue(reuse.reuseID)
          }
          piw.addDocument(r.pd)
          r.sbi.setText(c)
          var start = r.sbi.first()
          var end = r.sbi.next()
          while (end != BreakIterator.DONE) {
            r.clearOptionalSentenceFields()
            val sentence = c.substring(start, end)
            r.sentenceIDField.setLongValue(sentences.incrementAndGet)
            r.contentField.setStringValue(sentence)
            r.contentLengthFields.setValue(sentence.length)
            r.contentTokensFields.setValue(getNumberOfTokens(sentence))
            r.startOffsetFields.setValue(dcontents.size + dpcontents.size - c.length + start)
            r.endOffsetFields.setValue(dcontents.size + dpcontents.size - c.length + end)
            val reuses: Seq[ReuseInterval] = documentClusters.overlap(new IntegerInterval(dcontents.size + dpcontents.size - c.length + start, dcontents.size + dpcontents.size - c.length + end, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
            r.reusesFields.setValue(reuses.size)
            for (reuse <- reuses) {
              val f = new StringSNDVFieldPair("reuseID").o(r.send)
              f.setValue(reuse.reuseID)
            }
            seniw.addDocument(r.send)
            start = end
            end = r.sbi.next()
          }
        }
        for (
          headingInfoOpt <- headingInfos;
          headingInfo <- headingInfoOpt
        ) {
          r.sectionIDFields.setValue(headingInfo.sectionID)
          r.headingLevelFields.setValue(headingInfo.headingLevel)
          r.headingFields.setValue(headingInfo.heading)
          if (headingInfo.content.nonEmpty) {
            val contentS = trimSpace(headingInfo.content.toString)
            r.contentField.setStringValue(contentS)
            r.contentLengthFields.setValue(contentS.length)
            r.contentTokensFields.setValue(getNumberOfTokens(contentS))
            r.startOffsetFields.setValue(dcontents.size + dpcontents.size - contentS.length)
            r.endOffsetFields.setValue(dcontents.size + dpcontents.size)
            val reuses: Seq[ReuseInterval] = documentClusters.overlap(new IntegerInterval(dcontents.size + dpcontents.size - contentS.length, dcontents.size + dpcontents.size, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
            r.reusesFields.setValue(reuses.size)
            for (reuse <- reuses) {
              val f = new StringSNDVFieldPair("reuseID").o(r.sd)
              f.setValue(reuse.reuseID)
            }
          }
          siw.addDocument(r.sd)
        }
        val dpcontentsS = trimSpace(dpcontents.toString)
        if (dpcontentsS.length > 0) {
          r.contentField.setStringValue(dpcontentsS)
          r.contentLengthFields.setValue(dpcontentsS.length)
          r.contentTokensFields.setValue(getNumberOfTokens(dpcontentsS))
          r.startOffsetFields.setValue(dcontents.size)
          r.endOffsetFields.setValue(dcontents.size + dpcontents.size)
          val reuses: Seq[ReuseInterval] = documentClusters.overlap(new IntegerInterval(dcontents.size, dcontents.size + dpcontents.size, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
          r.reusesFields.setValue(reuses.size)
          for (reuse <- reuses) {
            val f = new StringSNDVFieldPair("reuseID").o(r.dpd)
            f.setValue(reuse.reuseID)
          }
          dpiw.addDocument(r.dpd)
        }
        wcontents.append(dpcontentsS)
        wcontents.append("\n\n")
      }
      val wcontentsS = trimSpace(wcontents.toString)
      if (wcontentsS.length > 0) {
        r.contentField.setStringValue(wcontentsS)
        r.contentLengthFields.setValue(wcontentsS.length)
        r.contentTokensFields.setValue(getNumberOfTokens(wcontentsS))
        r.startOffsetFields.setValue(dcontents.size)
        r.endOffsetFields.setValue(dcontents.size + wcontents.size)
        val reuses: Seq[ReuseInterval] = documentClusters.overlap(new IntegerInterval(dcontents.size, dcontents.size + wcontents.size, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
        r.reusesFields.setValue(reuses.size)
        for (reuse <- reuses) {
          val f = new StringSNDVFieldPair("reuseID").o(r.wd)
          f.setValue(reuse.reuseID)
        }
        wiw.addDocument(r.wd)
      }
      dcontents.append(wcontentsS)
      dcontents.append("\n\n")
    }
    val dcontentsS = trimSpace(dcontents.toString)
    val reuses: Seq[ReuseInterval] = documentClusters.asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
    r.reusesFields.setValue(reuses.size)
    for (reuse <- reuses) {
      val f = new StringSNDVFieldPair("reuseID").o(r.dd)
      f.setValue(reuse.reuseID)
    }
    r.contentField.setStringValue(dcontentsS)
    r.contentLengthFields.setValue(dcontentsS.length)
    r.contentTokensFields.setValue(getNumberOfTokens(dcontentsS))
    diw.addDocument(r.dd)
    logger.info("Processed: "+filePrefix)
  }

  var diw, wiw, dpiw, siw, piw, seniw = null.asInstanceOf[IndexWriter]

  val ds = new Sort(new SortField("documentID",SortField.Type.STRING))
  val ws = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("workID", SortField.Type.LONG))
  val dps = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("partID", SortField.Type.LONG))
  val ss = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("sectionID", SortField.Type.LONG))
  val ps = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG))
  val sens = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("sentenceID", SortField.Type.LONG))

  var bookToClustersDB: Database = _

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val dpostings = opt[String](default = Some("blocktree"))
      val wpostings = opt[String](default = Some("blocktree"))
      val dppostings = opt[String](default = Some("blocktree"))
      val spostings = opt[String](default = Some("blocktree"))
      val ppostings = opt[String](default = Some("fst"))
      val senpostings = opt[String](default = Some("blocktree"))
      val bookToClusterDb = opt[String](required = true)
      verify()
    }
    val btcenvDir = new File(opts.bookToClusterDb())
    btcenvDir.mkdirs()
    val btcenv = new Environment(btcenvDir,new EnvironmentConfig().setAllowCreate(true).setTransactional(false).setSharedCache(true).setConfigParam(EnvironmentConfig.LOG_FILE_MAX,"1073741824"))
    btcenv.setMutableConfig(btcenv.getMutableConfig.setCacheSize(opts.indexMemoryMb()*1024*1024/2))
    bookToClustersDB = btcenv.openDatabase(null, "bookToCluster", new DatabaseConfig().setAllowCreate(true).setDeferredWrite(true).setTransactional(false).setSortedDuplicates(true))
    if (!opts.onlyMerge()) {
      // document level
      diw = iw(opts.index()+"/dindex", ds, opts.indexMemoryMb()/2/6)
      // work level
      wiw = iw(opts.index()+"/windex", ws, opts.indexMemoryMb()/2/6)
      // document part level
      dpiw = iw(opts.index()+"/dpindex", dps, opts.indexMemoryMb()/2/6)
      // section level
      siw = iw(opts.index()+"/sindex", ss, opts.indexMemoryMb()/2/6)
      // paragraph level
      piw = iw(opts.index()+"/pindex", ps, opts.indexMemoryMb()/2/6)
      // sentence level
      seniw = iw(opts.index()+"/senindex", sens, opts.indexMemoryMb()/2/6)
      /*
       *
     6919 article
    51226 backmatter
    34276 book
  1377880 chapter
   147743 frontmatter
    13386 index
    25772 other
    38055 part
   290959 section
     3957 titlePage
    32836 TOC
     1481 volume
       */
      feedAndProcessFedTasksInParallel(() => {
        opts.directories().toStream.flatMap(p => {
          val parts = p.split(':')
          getFileTree(new File(parts(0))).map((parts(1),_))
        }).filter(_._2.getName.endsWith("headed.txt")).foreach(pair => {
          val path = pair._2.getPath.replace(".headed.txt","")
          addTask(path, () => index(pair._1, path))
        })
      })
    }
    waitForTasks(
      runSequenceInOtherThread(
        () => close(diw),
        () => merge(opts.index()+"/dindex", ds, opts.indexMemoryMb()/6, toCodec(opts.dpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(wiw),
        () => merge(opts.index()+"/windex", ws, opts.indexMemoryMb()/6, toCodec(opts.wpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(dpiw),
        () => merge(opts.index()+"/dpindex", dps, opts.indexMemoryMb()/6, toCodec(opts.dppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(siw),
        () => merge(opts.index()+"/sindex", ss, opts.indexMemoryMb()/6, toCodec(opts.spostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(piw),
        () => merge(opts.index()+"/pindex", ps, opts.indexMemoryMb()/6, toCodec(opts.ppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(seniw),
        () => merge(opts.index()+"/senindex", sens, opts.indexMemoryMb()/6, toCodec(opts.senpostings(), termVectorFields))
      )
    )
  }
}
