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
import org.apache.lucene.index.LogDocMergePolicy
import scala.util.Failure
import scala.util.Success

import org.rogach.scallop._
import scala.language.postfixOps
import org.apache.lucene.document.SortedNumericDocValuesField
import scala.xml.parsing.XhtmlEntities
import scala.xml.MetaData
import scala.collection.Searching

object ECCOIndexer extends OctavoIndexer {
  
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
  
  val fileRegex = ".*_(.*)\\.txt".r
  
  val headingRegex = "^# ".r

  private val paragraphs = new AtomicLong
  private val sections = new AtomicLong
  private val documentparts = new AtomicLong
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  class Reuse {
    val dd = new Document()
    val dpd = new Document()
    val sd = new Document()
    val pd = new Document()
    val collectionIDFields = new StringSDVFieldPair("collectionID", dd, dpd, sd, pd)
    val documentIDFields = new StringSDVFieldPair("documentID", dd, dpd, sd, pd)
    val estcIDFields = new StringSDVFieldPair("ESTCID", dd, dpd, sd, pd)
    val dateStartFields = new IntPointNDVFieldPair("dateStart", dd, dpd, sd, pd)
    val dateEndFields = new IntPointNDVFieldPair("dateEnd", dd, dpd, sd, pd)
    val totalPagesFields = new IntPointNDVFieldPair("totalPages", dd, dpd, sd, pd)
    val languageFields = new StringSDVFieldPair("language", dd, dpd, sd, pd)
    val moduleFields = new StringSDVFieldPair("module", dd, dpd, sd, pd)
    val fullTitleFields = new TextSDVFieldPair("fullTitle",dd,dpd,sd,pd)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength", dd, dpd, sd, pd)
    val documentLengthFields = new IntPointNDVFieldPair("documentLength", dd, dpd, sd, pd)
    val totalParagraphsFields = new IntPointNDVFieldPair("totalParagraphs", dd, dpd, sd, pd)
    def clearOptionalDocumentFields() {
      dateStartFields.setValue(0)
      dateEndFields.setValue(Int.MaxValue)
      totalPagesFields.setValue(0)
      languageFields.setValue("")
      moduleFields.setValue("")
      fullTitleFields.setValue("")
      dd.removeFields("containsGraphicOfType")
      dd.removeFields("containsGraphicCaption")
    }
    def clearOptionalDocumentPartFields() {
      dpd.removeFields("containsGraphicOfType")
      dpd.removeFields("containsGraphicCaption")
    }
    def clearOptionalParagraphFields() {
      pd.removeFields("sectionID")
      pd.removeFields("headingLevel")
      pd.removeFields("heading")
    }
    val documentPartIdFields = new StringNDVFieldPair("partID", dpd, sd, pd)
    val paragraphIDField = new NumericDocValuesField("paragraphID", 0)
    pd.add(paragraphIDField)
    val contentField = new Field("content","",contentFieldType)
    dd.add(contentField)
    dpd.add(contentField)
    sd.add(contentField)
    pd.add(contentField)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens", dd, dpd, sd, pd)
    val documentPartTypeFields = new StringSDVFieldPair("documentPartType", dpd, sd, pd)
    val sectionIDFields = new StringNDVFieldPair("sectionID", sd)
    val headingFields = new TextSDVFieldPair("heading",sd)
    val headingLevelFields = new IntPointNDVFieldPair("headingLevel", sd)
  }
  
  finalCodec.termVectorFields = Set("content","containsGraphicOfType")
  
  class SectionInfo(val sectionID: Long, val headingLevel: Int, val heading: String) {
    val paragraphSectionIDFields = new StringSNDVFieldPair("sectionID")
    paragraphSectionIDFields.setValue(sectionID)
    val paragraphHeadingLevelFields = new IntPointSNDVFieldPair("headingLevel")
    paragraphHeadingLevelFields.setValue(headingLevel)
    val paragraphHeadingFields = new TextSSDVFieldPair("heading")
	  val content = new StringBuilder()
    def addToParagraphDocument(pd: Document) {
      paragraphSectionIDFields.add(pd)
      paragraphHeadingLevelFields.add(pd)
      paragraphHeadingFields.add(pd)
    }
  }
  
  private def index(id: String, file: File): Unit = {
    val filePrefix = file.getName.replace("_metadata.xml","")
    logger.info("Processing: "+file.getPath.replace("_metadata.xml","*"))
    var totalPages = 0
    val xmls = Source.fromFile(file)
    implicit val xml = new XMLEventReader(xmls)
    val r = tld.get
    r.clearOptionalDocumentFields()
    r.collectionIDFields.setValue(id)
    var documentID: String = null
    var estcID: String = null
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"documentID",_,_) | EvElemStart(_,"PSMID",_,_) =>
        documentID = readContents
        r.documentIDFields.setValue(documentID)
      case EvElemStart(_,"ESTCID",_,_) =>
        estcID = readContents
        r.estcIDFields.setValue(estcID)
      case EvElemStart(_,"bibliographicID",attr,_) if (attr("type")(0).text == "ESTC") => // ECCO2
        estcID = readContents
        r.estcIDFields.setValue(estcID)
      case EvElemStart(_,"pubDate",_,_) => readContents match {
        case null => // ECCO2
          var break = false
          var endDateFound = false
          var startDate: String = null
          while (xml.hasNext && !break) {
            xml.next match {
              case EvElemStart(_,"pubDateStart",_,_) => readContents match {
                case any =>
                  startDate = any
                  r.dateStartFields.setValue(any.toInt)
              }
              case EvElemStart(_,"pubDateEnd",_,_) => readContents match {
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
      case EvElemStart(_,"totalPages",_,_) =>
        val tp = readContents
        if (!tp.isEmpty) {
          totalPages = tp.toInt
          r.totalPagesFields.setValue(totalPages)
        }
      case EvElemStart(_,"language",_,_) =>
        r.languageFields.setValue(readContents)
      case EvElemStart(_,"module",_,_) =>
        r.moduleFields.setValue(readContents)
      case EvElemStart(_,"fullTitle",_,_) => 
        r.fullTitleFields.setValue(readContents)
      case _ => 
    }
    xmls.close()
    if (documentID==null) logger.error("No document ID for "+file)
    if (estcID==null) logger.error("No ESTC ID for "+file)
    val dcontents = new StringBuilder
    var lastPage = 0
    val filesToProcess = new ArrayBuffer[File] 
    for (file <- getFileTree(file.getParentFile); if file.getName.endsWith(".txt") && file.getName.startsWith(filePrefix)) if (file.getName.contains("_page"))
      for (curPage <- "_page([0-9]+)".r.findFirstMatchIn(file.getName).map(_.group(1).toInt); if curPage>lastPage) lastPage = curPage
    else filesToProcess += file
    if (totalPages != lastPage) logger.warn(s"total pages $totalPages != actual $lastPage")
    var totalParagraphs = 0
    var totalLength = 0
    for (file <- filesToProcess;line <- Source.fromFile(file).getLines) {
      if (line.isEmpty) totalParagraphs += 1
      totalLength += line.length
    }
    r.documentLengthFields.setValue(totalLength)
    r.totalParagraphsFields.setValue(totalParagraphs)
    for (file <- filesToProcess.sortBy(x => x.getName.substring(x.getName.indexOf('_') + 1, x.getName.indexOf('_', x.getName.indexOf('_') + 1).toInt))) {
      val dpcontents = new StringBuilder
      r.clearOptionalDocumentPartFields()
      r.documentPartIdFields.setValue(documentparts.incrementAndGet)
      r.documentPartTypeFields.setValue(fileRegex.findFirstMatchIn(file.getName).get.group(1))
      if (new File(file.getPath.replace(".txt","-graphics.csv")).exists)
        for (row <- CSVReader(file.getPath.replace(".txt","-graphics.csv"))) {
          val gtype = if (row(1)=="") "unknown" else row(1)
          val f = new Field("containsGraphicOfType",gtype, notStoredStringFieldWithTermVectors)
          r.dpd.add(f)
          r.dd.add(f)
          if (row(3)!="") {
            val f = new Field("containsGraphicCaption", row(3), normsOmittingStoredTextField)
            r.dpd.add(f)
            r.dd.add(f)
          }
        }
      val headingInfos = Seq(1,2,3).map(w => None.asInstanceOf[Option[SectionInfo]]).toArray
      val pcontents = new StringBuilder
      val fl = Source.fromFile(file)
      for (line <- fl.getLines) {
        if (line.isEmpty) {
          if (!pcontents.isEmpty) {
        	  r.clearOptionalParagraphFields()
            r.paragraphIDField.setLongValue(paragraphs.incrementAndGet)
            r.contentField.setStringValue(pcontents.toString)
            val tokens = getNumberOfTokens(pcontents.toString)
            r.contentLengthFields.setValue(pcontents.length)
            r.contentTokensFields.setValue(getNumberOfTokens(pcontents.toString))
            var hasHeadingInfos = false
            headingInfos.foreach(_.foreach(hi => {
              hi.addToParagraphDocument(r.pd)
              hasHeadingInfos = true
            }))
            piw.addDocument(r.pd)
            pcontents.clear()
          }
        } else
          pcontents.append(line)
        if (line.startsWith("# ") || line.startsWith("## ") || line.startsWith("### ")) {
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
              if (!headingInfo.content.isEmpty) {
            	  val contentS = headingInfo.content.toString
            	  r.contentField.setStringValue(contentS)
            	  r.contentLengthFields.setValue(contentS.length)
            	  r.contentTokensFields.setValue(getNumberOfTokens(contentS))
              }
              siw.addDocument(r.sd)
              headingInfos(i) = None
            }
          headingInfos(level) = Some(new SectionInfo(sections.incrementAndGet, level, line.substring(level+2)))
          for (i <- 0 until level) headingInfos(i).foreach(_.content.append(line))
        } else
          headingInfos.foreach(_.foreach(_.content.append(line)))
        dpcontents.append(line+"\n")
      }
      fl.close()
      for (
          headingInfoOpt <- headingInfos;
          headingInfo <- headingInfoOpt
      ) { 
          r.sectionIDFields.setValue(headingInfo.sectionID)
          r.headingLevelFields.setValue(headingInfo.headingLevel)
          r.headingFields.setValue(headingInfo.heading)
          if (!headingInfo.content.isEmpty) {
        	  val contentS = headingInfo.content.toString.trim
        	  r.contentField.setStringValue(contentS)
        	  r.contentLengthFields.setValue(contentS.length)
        	  r.contentTokensFields.setValue(getNumberOfTokens(contentS))
          }
          siw.addDocument(r.sd)
        }
      val dpcontentsS = dpcontents.toString.trim
      r.contentField.setStringValue(dpcontentsS)
      r.contentLengthFields.setValue(dpcontentsS.length)
      r.contentTokensFields.setValue(getNumberOfTokens(dpcontentsS))
      dpiw.addDocument(r.dpd)
      dcontents.append(dpcontentsS)
      dcontents.append("\n\n")
    }
    val dcontentsS = dcontents.toString.trim
    r.contentField.setStringValue(dcontentsS)
    r.contentLengthFields.setValue(dcontentsS.length)
    r.contentTokensFields.setValue(getNumberOfTokens(dcontentsS))
    diw.addDocument(r.dd)
    logger.info("Processed: "+file.getPath.replace("_metadata.xml","*"))
  }
  
  var diw, dpiw, siw, piw = null.asInstanceOf[IndexWriter]
  
  def main(args: Array[String]): Unit = {
    val opts = new OctavoOpts(args)
    // document level
    diw = iw(opts.index()+"/dindex", new Sort(new SortField("documentID",SortField.Type.STRING)), opts.indexMemoryMb()/4)
    // document part level
    dpiw = iw(opts.index()+"/dpindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("partID", SortField.Type.LONG)), opts.indexMemoryMb()/4)
    // section level
    siw = iw(opts.index()+"/sindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("sectionID", SortField.Type.LONG)), opts.indexMemoryMb()/4)
    // paragraph level
    piw = iw(opts.index()+"/pindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)), opts.indexMemoryMb()/4)
    val writers = Seq(diw, dpiw, siw, piw)
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
    close(writers)
    mergeIndices(Seq(
     (opts.index()+"/dindex", new Sort(new SortField("documentID",SortField.Type.STRING)), opts.indexMemoryMb()/4),
     (opts.index()+"/dpindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("partID", SortField.Type.LONG)), opts.indexMemoryMb()/4),
     (opts.index()+"/sindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("sectionID", SortField.Type.LONG)), opts.indexMemoryMb()/4),
     (opts.index()+"/pindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)), opts.indexMemoryMb()/4)
    ))
  }
}
