import java.io.{File, FileInputStream, InputStream}
import java.nio.ByteBuffer
import java.text.BreakIterator
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import BritishNewspaperIndexer.logger
import com.brein.time.timeintervals.collections.ListIntervalCollection
import com.brein.time.timeintervals.indexes.IntervalTreeBuilder.IntervalType
import com.brein.time.timeintervals.indexes.{IntervalTree, IntervalTreeBuilder}
import com.brein.time.timeintervals.intervals.IntegerInterval
import com.github.tototoshi.csv.CSVReader
import com.sleepycat.je._
import javax.xml.stream.{XMLEventReader, XMLInputFactory}
import javax.xml.stream.events.{Characters, Comment, EndElement, EntityReference, StartElement}
import org.apache.lucene.document.{Document, Field, NumericDocValuesField}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.rogach.scallop._

import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.language.{postfixOps, reflectiveCalls}
import scala.xml.parsing.XhtmlEntities
import scala.jdk.CollectionConverters._


object ECCOIndexer extends OctavoIndexer {

  private def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case _: StartElement => return null
      case text: Characters => content.append(text.getData)
      case er: EntityReference => XhtmlEntities.entMap.get(er.getName) match {
        case Some(chr) => content.append(chr)
        case _ =>
          logger.warn("Encountered unknown entity "+er.getName)
          content.append('[')
          content.append(er.getName)
          content.append(']')
      }
      case c: Comment if c.getText == " unknown entity apos; " => content.append('\'')
      case c: Comment if c.getText.startsWith(" unknown entity") =>
        val entity = c.getText.substring(16, c.getText.length - 2)
        logger.warn("Encountered unknown entity "+entity)
        content.append('[')
        content.append(entity)
        content.append(']')
      case c: Comment =>
        logger.debug("Encountered comment: "+c.getText)
      case _: EndElement => break = true
    }
    content.toString
  }
  
  val fileRegex = ".*_(.*)\\.txt".r
  
  private val sentences = new AtomicLong
  private val paragraphs = new AtomicLong
  private val sections = new AtomicLong
  private val documentparts = new AtomicLong
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }

  class ReuseInterval(startIndex: Int, endIndex: Int, val reuseID: Long) extends IntegerInterval(startIndex,endIndex, false, true) {

  }
  
  class Reuse {
    val bookToFragments = bookToFragmentsDB.openCursor(null, null)
    val bckeya = new Array[Byte](java.lang.Long.BYTES)
    val bcbb = ByteBuffer.wrap(bckeya)
    val bckey = new DatabaseEntry(bckeya)
    val bcval = new DatabaseEntry
    val sbi = BreakIterator.getSentenceInstance(new Locale("en_GB"))
    val dd = new FluidDocument()
    val dpd = new FluidDocument()
    val sd = new FluidDocument()
    val pd = new FluidDocument()
    val send = new FluidDocument()
    val collectionIDFields = new StringSDVFieldPair("collectionID").r(dd, dpd, sd, pd, send)
    val documentIDFields = new StringSDVFieldPair("documentID").r(dd, dpd, sd, pd, send)
    val estcIDFields = new StringSDVFieldPair("ESTCID").r(dd, dpd, sd, pd, send)
    val dateStartFields = new IntPointNDVFieldPair("dateStart").r(dd, dpd, sd, pd, send)
    val dateEndFields = new IntPointNDVFieldPair("dateEnd").r(dd, dpd, sd, pd, send)
    val totalPagesFields = new IntPointNDVFieldPair("totalPages").r(dd, dpd, sd, pd, send)
    val languageFields = new StringSDVFieldPair("language").r(dd, dpd, sd, pd, send)
    val moduleFields = new StringSDVFieldPair("module").r(dd, dpd, sd, pd, send)
    val fullTitleFields = new TextSDVFieldPair("fullTitle").r(dd,dpd,sd,pd, send)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength").r(dd, dpd, sd, pd, send)
    val documentLengthFields = new IntPointNDVFieldPair("documentLength").r(dd, dpd, sd, pd, send)
    val totalParagraphsFields = new IntPointNDVFieldPair("totalParagraphs").r(dd, dpd, sd, pd, send)
    val startOffsetFields = new IntPointNDVFieldPair("startOffset").r(dpd, sd, pd, send)
    val endOffsetFields = new IntPointNDVFieldPair("endOffset").r(dpd, sd, pd, send)
    val reusesFields = new IntPointNDVFieldPair("reuses").r(dd, dpd, sd, pd, send)
    def clearOptionalDocumentFields() {
      dateStartFields.setValue(0)
      dateEndFields.setValue(Int.MaxValue)
      totalPagesFields.setValue(0)
      languageFields.setValue("")
      moduleFields.setValue("")
      fullTitleFields.setValue("")
      dd.clearOptional()
/*      dd.removeFields("containsGraphicOfType")
      dd.removeFields("containsGraphicCaption")
      dd.removeFields("reuseID")*/
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
    val documentPartIdFields = new StringNDVFieldPair("partID").r(dpd, sd, pd, send)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID").r(pd, send)
    val sentenceIDField = new NumericDocValuesField("sentenceID", 0)
    send.addRequired(sentenceIDField)
    val contentField = new Field("content","",contentFieldType)
    dd.addRequired(contentField)
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
  
  class SectionInfo(val sectionID: Long, val headingLevel: Int, val heading: String, val startOffset: Int) {
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

  private val xmlf = XMLInputFactory.newInstance()

  import XMLEventReaderSupport._

  private def index(id: String, file: File): Unit = {
    val filePrefix = file.getName.replace("_metadata.xml","")
    logger.info("Processing: "+file.getPath.replace("_metadata.xml","*"))
    var totalPages = 0
    val xmls: java.io.InputStream = new FileInputStream(file)
    implicit val xml: XMLEventReader = xmlf.createXMLEventReader(xmls)
    val r = tld.get
    r.clearOptionalDocumentFields()
    r.collectionIDFields.setValue(id)
    var documentID: String = null
    var estcID: String = null
    val documentFragments: IntervalTree = treeBuilder.build()
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"documentID",_) =>
        documentID = trimSpace(readContents)
        r.documentIDFields.setValue(documentID)
        r.bcbb.putLong(0, documentID.toLong)
        if (r.bookToFragments.getSearchKey(r.bckey, r.bcval, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
          val vbb = ByteBuffer.wrap(r.bcval.getData)
          documentFragments.add(new ReuseInterval(vbb.getInt,vbb.getInt,vbb.getLong))
          while (r.bookToFragments.getNextDup(r.bckey, r.bcval, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            val vbb = ByteBuffer.wrap(r.bcval.getData)
            documentFragments.add(new ReuseInterval(vbb.getInt,vbb.getInt,vbb.getLong))
          }
        }
        case EvElemStart(_,"ESTCID",_) =>
          estcID = trimSpace(readContents)
          r.estcIDFields.setValue(estcID)
        case EvElemStart(_,"bibliographicID",attrs) if attrs.get("type").contains("ESTC") => // ECCO2
          estcID = trimSpace(readContents)
          r.estcIDFields.setValue(estcID)
        case EvElemStart(_,"pubDate",_) => trimSpace(readContents) match {
          case null => // ECCO2
            var break = false
            var endDateFound = false
            var startDate: String = null
            while (xml.hasNext && !break) {
              xml.next match {
                case EvElemStart(_,"pubDateStart",_) => trimSpace(readContents) match {
                  case any =>
                    startDate = any
                    r.dateStartFields.setValue(any.toInt)
                }
                case EvElemStart(_,"pubDateEnd",_) => trimSpace(readContents) match {
                  case any =>
                    endDateFound = true
                    r.dateEndFields.setValue(any.replaceAll("00","99").toInt)
                }
                case EvElemEnd(_,"pubDate") => break = true
                case _ =>
              }
            }
            if (!endDateFound && startDate != null) {
              r.dateEndFields.setValue(startDate.replaceAll("00","99").toInt)
            }
          case "" =>
          case "1809" =>
            r.dateStartFields.setValue(18090000)
            r.dateEndFields.setValue(18099999)
          case any =>
            r.dateStartFields.setValue(any.toInt)
            r.dateEndFields.setValue(any.replaceAll("01","99").toInt)
        }
        case EvElemStart(_,"totalPages",_) =>
          val tp = trimSpace(readContents)
          if (!tp.isEmpty) {
            totalPages = tp.toInt
            r.totalPagesFields.setValue(totalPages)
          }
        case EvElemStart(_,"language",_) =>
          r.languageFields.setValue(trimSpace(readContents))
        case EvElemStart(_,"module",_) =>
          r.moduleFields.setValue(trimSpace(readContents))
        case EvElemStart(_,"fullTitle",_) =>
          r.fullTitleFields.setValue(trimSpace(readContents))
      case _ => 
    }
    xml.close()
    xmls.close()
    if (documentID==null) logger.error("No document ID for "+file)
    if (estcID==null) logger.error("No ESTC ID for "+file)
    val dcontents = new StringBuilder
    var lastPage = 0
    val filesToProcess = new ArrayBuffer[File] 
    for (file <- getFileTree(file.getParentFile); if file.getName.endsWith(".txt") && file.getName.startsWith(filePrefix)) if (file.getName.contains("_page"))
      for (curPage <- "_page([0-9]+)".r.findFirstMatchIn(file.getName).map(_.group(1).toInt); if curPage>lastPage) lastPage = curPage
    else if (file.getName.indexOf('_', file.getName.indexOf('_') + 1) != -1) filesToProcess += file
    if (totalPages != lastPage) logger.warn(s"total pages $totalPages != actual $lastPage")
    var totalParagraphs = 0
    var totalLength = 0
    for (file <- filesToProcess;line <- Source.fromFile(file).getLines) {
      if (line.isEmpty) totalParagraphs += 1
      totalLength += line.length
    }
    r.documentLengthFields.setValue(totalLength)
    r.totalParagraphsFields.setValue(totalParagraphs)
/*    var curline: Seq[Int] = null
    var pcuroffset = -1l
    val ppl = Source.fromFile(file.getPath.replace("_metadata.xml","-payload.csv")).getLines
    var scuroffset = -1l
    val spl = Source.fromFile(file.getPath.replace("_metadata.xml","-payload.csv")).getLines
    var dpcuroffset = -1l
    val dppl = Source.fromFile(file.getPath.replace("_metadata.xml","-payload.csv")).getLines
    var dcuroffset = -1l
    val dpl = Source.fromFile(file.getPath.replace("_metadata.xml","-payload.csv")).getLines */
    var dpoffset = 0
    for (file <- filesToProcess.sortBy(x => x.getName.substring(x.getName.indexOf('_') + 1, x.getName.indexOf('_', x.getName.indexOf('_') + 1)).toInt)) {
      dpoffset = dcontents.length
      val dpcontents = new StringBuilder
      r.clearOptionalDocumentPartFields()
      r.documentPartIdFields.setValue(documentparts.incrementAndGet)
      r.documentPartTypeFields.setValue(fileRegex.findFirstMatchIn(file.getName).get.group(1))
      if (new File(file.getPath.replace(".txt","-graphics.csv")).exists)
        for (row <- CSVReader.open(file.getPath.replace(".txt","-graphics.csv"))) {
          val gtype = if (row(1)=="") "unknown" else row(1)
          val f = new Field("containsGraphicOfType",gtype, notStoredStringFieldWithTermVectors)
          r.dpd.addOptional(f)
          r.dd.addOptional(f)
          if (row(3)!="") {
            val f = new Field("containsGraphicCaption", row(3), normsOmittingStoredTextField)
            r.dpd.addOptional(f)
            r.dd.addOptional(f)
          }
        }
      val headingInfos = Seq(1,2,3).map(_ => None.asInstanceOf[Option[SectionInfo]]).toArray
      val pcontents = new StringBuilder
      var poffset = dpoffset
      val fl = Source.fromFile(file)
      for (line <- fl.getLines) {
        if (line.isEmpty) {
          if (pcontents.nonEmpty) {
            val c = pcontents.toString
            r.clearOptionalParagraphFields()
            r.paragraphIDFields.setValue(paragraphs.incrementAndGet)
            r.contentField.setStringValue(c)
            r.startOffsetFields.setValue(poffset)
            r.endOffsetFields.setValue(poffset + c.length)
/*            val ts = new CachingTokenFilter(new TokenFilter(analyzer.tokenStream("content",c)) {

              private val payloadAtt = addAttribute(classOf[PayloadAttribute])
              private val offsetAtt = addAttribute(classOf[OffsetAttribute])
              override def incrementToken(): Boolean = if (input.incrementToken) {
                val myoffset = dcontents.length + offsetAtt.startOffset
                while (pcuroffset < myoffset) {
                  if (!ppl.hasNext) {
                    payloadAtt.setPayload(null)
                    return true
                  }
                  curline = ppl.next.split(',').map(_.toInt)
                  pcuroffset = curline.head
                }
                if (pcuroffset == myoffset) {
                  val bytes = new Array[Byte](5+5+5+5+5)
                  val bo = new ByteArrayDataOutput(bytes)
                  bo.writeVInt(curline(1))
                  bo.writeVInt(curline(2))
                  bo.writeVInt(curline(3))
                  bo.writeVInt(curline(4))
                  bo.writeVInt(curline(5))
                  payloadAtt.setPayload(new BytesRef(bytes,0,bo.getPosition))
                } else payloadAtt.setPayload(null)
                true
              } else false
            })
            ts.reset()
            ts.incrementToken() // orderly load cache
            ts.end()
            ts.close()
            r.contentField.setTokenStream(ts)
            r.contentTokensFields.setValue(getNumberOfTokens(ts)) */
            r.contentTokensFields.setValue(getNumberOfTokens(c))
            r.contentLengthFields.setValue(c.length)
            headingInfos.foreach(_.foreach(hi => {
              hi.addToDocument(r.pd)
              hi.addToDocument(r.send)
            }))
            val reuses: Seq[ReuseInterval] = documentFragments.overlap(new IntegerInterval(dcontents.size + dpcontents.size - c.length,dcontents.size + dpcontents.size,false,true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
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
              val sentence = c.substring(start,end)
              r.sentenceIDField.setLongValue(sentences.incrementAndGet)
              r.contentField.setStringValue(sentence)
              r.startOffsetFields.setValue(poffset + start)
              r.endOffsetFields.setValue(poffset + end)
              r.contentLengthFields.setValue(sentence.length)
              r.contentTokensFields.setValue(getNumberOfTokens(sentence))
              val reuses: Seq[ReuseInterval] = documentFragments.overlap(new IntegerInterval(dcontents.size + dpcontents.size - c.length + start,dcontents.size + dpcontents.size - c.length  + end,false,true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
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
            poffset = dcontents.length
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
            headingInfoOpt = headingInfos(i);
        	  headingInfo <- headingInfoOpt
          ) {
              r.sectionIDFields.setValue(headingInfo.sectionID)
              r.headingLevelFields.setValue(headingInfo.headingLevel)
              r.headingFields.setValue(headingInfo.heading)
              if (headingInfo.content.nonEmpty) {
            	  val contentS = headingInfo.content.toString
                val reuses: Seq[ReuseInterval] = documentFragments.overlap(new IntegerInterval(dcontents.size + dpcontents.size - contentS.length,dcontents.size + dpcontents.size,false,true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
                r.reusesFields.setValue(reuses.size)
                for (reuse <- reuses) {
                  val f = new StringSNDVFieldPair("reuseID").o(r.sd)
                  f.setValue(reuse.reuseID)
                }
            	  r.contentField.setStringValue(contentS)
                r.startOffsetFields.setValue(headingInfo.startOffset)
                r.endOffsetFields.setValue(headingInfo.startOffset + contentS.length)
            	  r.contentLengthFields.setValue(contentS.length)
            	  r.contentTokensFields.setValue(getNumberOfTokens(contentS))
              }
              siw.addDocument(r.sd)
              headingInfos(i) = None
            }
          headingInfos(level) = Some(new SectionInfo(sections.incrementAndGet, level, line.substring(level+2),dcontents.length))
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
        r.startOffsetFields.setValue(poffset)
        r.endOffsetFields.setValue(poffset + c.length)
        r.contentLengthFields.setValue(c.length)
        r.contentTokensFields.setValue(getNumberOfTokens(c))
        var hasHeadingInfos = false
        headingInfos.foreach(_.foreach(hi => {
          hi.addToDocument(r.pd)
          hi.addToDocument(r.send)
          hasHeadingInfos = true
        }))
        val reuses: Seq[ReuseInterval] = documentFragments.overlap(new IntegerInterval(dcontents.size + dpcontents.size - c.length,dcontents.size + dpcontents.size,false,true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
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
          val sentence = c.substring(start,end)
          r.sentenceIDField.setLongValue(sentences.incrementAndGet)
          r.contentField.setStringValue(sentence)
          r.startOffsetFields.setValue(poffset + start)
          r.endOffsetFields.setValue(poffset + end)
          r.contentLengthFields.setValue(sentence.length)
          r.contentTokensFields.setValue(getNumberOfTokens(sentence))
          val reuses: Seq[ReuseInterval] = documentFragments.overlap(new IntegerInterval(dcontents.size + dpcontents.size - c.length + start,dcontents.size + dpcontents.size - c.length  + end,false,true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
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
        poffset = dcontents.length
      }
      for (
          headingInfoOpt <- headingInfos;
          headingInfo <- headingInfoOpt
      ) { 
          r.sectionIDFields.setValue(headingInfo.sectionID)
          r.headingLevelFields.setValue(headingInfo.headingLevel)
          r.headingFields.setValue(headingInfo.heading)
          if (headingInfo.content.nonEmpty) {
        	  val contentS = headingInfo.content.toString.trim
        	  r.contentField.setStringValue(contentS)
            r.startOffsetFields.setValue(headingInfo.startOffset)
            r.endOffsetFields.setValue(headingInfo.startOffset + contentS.length)
        	  r.contentLengthFields.setValue(contentS.length)
        	  r.contentTokensFields.setValue(getNumberOfTokens(contentS))
            val reuses: Seq[ReuseInterval] = documentFragments.overlap(new IntegerInterval(dcontents.size + dpcontents.size - contentS.length,dcontents.size + dpcontents.size,false,true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
            r.reusesFields.setValue(reuses.size)
            for (reuse <- reuses) {
              val f = new StringSNDVFieldPair("reuseID").o(r.sd)
              f.setValue(reuse.reuseID)
            }
          }
          siw.addDocument(r.sd)
        }
      val dpcontentsS = dpcontents.toString
      if (dpcontentsS.length > 0) {
        r.contentField.setStringValue(dpcontentsS)
        r.contentLengthFields.setValue(dpcontentsS.length)
        r.contentTokensFields.setValue(getNumberOfTokens(dpcontentsS))
        r.startOffsetFields.setValue(dpoffset)
        r.endOffsetFields.setValue(dpoffset+dpcontentsS.length)
        val reuses: Seq[ReuseInterval] = documentFragments.overlap(new IntegerInterval(dcontents.size, dcontents.size + dpcontents.size, false, true)).asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
        r.reusesFields.setValue(reuses.size)
        for (reuse <- reuses) {
          val f = new StringSNDVFieldPair("reuseID").o(r.dpd)
          f.setValue(reuse.reuseID)
        }
        dpiw.addDocument(r.dpd)
      }
      dcontents.append(dpcontentsS)
    }
    val dcontentsS = dcontents.toString.trim
    val reuses: Seq[ReuseInterval] = documentFragments.asScala.toSeq.asInstanceOf[Seq[ReuseInterval]]
    r.reusesFields.setValue(reuses.size)
    for (reuse <- reuses) {
      val f = new StringSNDVFieldPair("reuseID").o(r.dd)
      f.setValue(reuse.reuseID)
    }
    r.contentField.setStringValue(dcontentsS)
    r.contentLengthFields.setValue(dcontentsS.length)
    r.contentTokensFields.setValue(getNumberOfTokens(dcontentsS))
    diw.addDocument(r.dd)
    logger.info("Processed: "+file.getPath.replace("_metadata.xml","*"))
  }
  
  var diw, dpiw, siw, piw, seniw = null.asInstanceOf[IndexWriter]
  
  val ds = new Sort(new SortField("documentID",SortField.Type.STRING))
  val dps = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("partID", SortField.Type.LONG))
  val ss = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("sectionID", SortField.Type.LONG))
  val ps = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG))
  val sens = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG), new SortField("sentenceID", SortField.Type.LONG))

  var bookToFragmentsDB: Database = _

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val dpostings = opt[String](default = Some("blocktree"))
      val dppostings = opt[String](default = Some("blocktree"))
      val spostings = opt[String](default = Some("blocktree"))
      val ppostings = opt[String](default = Some("fst"))
      val senpostings = opt[String](default = Some("blocktree"))
      val bookToFragmentDb = opt[String](required = true)
      verify()
    }
    val btcenvDir = new File(opts.bookToFragmentDb())
    btcenvDir.mkdirs()
    val btcenv = new Environment(btcenvDir,new EnvironmentConfig().setAllowCreate(true).setTransactional(false).setSharedCache(true).setConfigParam(EnvironmentConfig.LOG_FILE_MAX,"1073741824"))
    btcenv.setMutableConfig(btcenv.getMutableConfig.setCacheSize(opts.indexMemoryMb()*1024*1024/2))
    bookToFragmentsDB = btcenv.openDatabase(null, "bookToFragment", new DatabaseConfig().setAllowCreate(true).setDeferredWrite(true).setTransactional(false).setSortedDuplicates(true))
    if (!opts.onlyMerge()) {
      // document level
      diw = iw(opts.index()+"/dindex", ds, opts.indexMemoryMb()/5)
      // document part level
      dpiw = iw(opts.index()+"/dpindex", dps, opts.indexMemoryMb()/5)
      // section level
      siw = iw(opts.index()+"/sindex", ss, opts.indexMemoryMb()/5)
      // paragraph level
      piw = iw(opts.index()+"/pindex", ps, opts.indexMemoryMb()/5)
      // sentence level
      seniw = iw(opts.index()+"/senindex", sens, opts.indexMemoryMb()/5)
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
        }).filter(_._2.getName.endsWith("_metadata.xml")).foreach(pair => {
          val path = pair._2.getPath.replace("_metadata.xml","*")
          addTask(path, () => index(pair._1, pair._2))
        })
      })
    }
    waitForTasks(
      runSequenceInOtherThread(
        () => close(diw), 
        () => merge(opts.index()+"/dindex", ds, opts.indexMemoryMb()/5, toCodec(opts.dpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(dpiw), 
        () => merge(opts.index()+"/dpindex", dps, opts.indexMemoryMb()/5, toCodec(opts.dppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(siw), 
        () => merge(opts.index()+"/sindex", ss, opts.indexMemoryMb()/5, toCodec(opts.spostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(piw), 
        () => merge(opts.index()+"/pindex", ps, opts.indexMemoryMb()/5, toCodec(opts.ppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(seniw), 
        () => merge(opts.index()+"/senindex", sens, opts.indexMemoryMb()/5, toCodec(opts.senpostings(), termVectorFields))
      )      
    )
  }
}
