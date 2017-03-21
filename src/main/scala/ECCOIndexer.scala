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

object ECCOIndexer extends LazyLogging {
  
  val numWorkers = sys.runtime.availableProcessors
  val queueCapacity = 1000
  val ec = ExecutionContext.fromExecutorService(
   new ThreadPoolExecutor(
     numWorkers, numWorkers,
     0L, TimeUnit.SECONDS,
     new ArrayBlockingQueue[Runnable](queueCapacity) {
       override def offer(e: Runnable) = {
         put(e)
         true
       }
     }
   )
  )
  
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().sorted.toStream.flatMap(getFileTree)
      else Stream.empty)
      
  def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_,_) => return null
      case EvText(text) => content.append(text)
      case er: EvEntityRef =>
        content.append('&')
        content.append(er.entity)
        content.append(';')
      case EvComment(_) => 
      case EvElemEnd(_,_) => break = true 
    }
    return content.toString.trim
  }
  
  val fileRegex = ".*_(.*)\\.txt".r
  
  val headingRegex = "^# ".r

  val analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET)
  
  val codec = new FSTOrdTermVectorsCodec()
  
  def iw(path: String): IndexWriter = {
    val iwc = new IndexWriterConfig(analyzer)
    iwc.setUseCompoundFile(false)
    new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
  }
  
  def getNumberOfTokens(text: String): Int = {
    val ts = analyzer.tokenStream("", text)
    val oa = ts.addAttribute(classOf[PositionIncrementAttribute])
    ts.reset()
    var length = 0
    while (ts.incrementToken())
      length += oa.getPositionIncrement
    ts.end()
    ts.close()
    return length
  }

  private val contentFieldType = new FieldType(TextField.TYPE_STORED)
  contentFieldType.setOmitNorms(true)

  contentFieldType.setStoreTermVectors(true)
  
  private val normsOmittingStoredTextField = new FieldType(TextField.TYPE_STORED)
  
  normsOmittingStoredTextField.setOmitNorms(true)
  
  private val notStoredStringFieldWithTermVectors = new FieldType(StringField.TYPE_NOT_STORED)
  notStoredStringFieldWithTermVectors.setStoreTermVectors(true)
  
  def merge(path: String, sort: Sort): Unit = {
    logger.info("Merging index at "+path)
    var size = getFileTreeSize(path) 
    // First, go to max two segments with general codec
    var iwc = new IndexWriterConfig(analyzer)
    iwc.setIndexSort(sort)
    iwc.setUseCompoundFile(false)
    var miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
    miw.forceMerge(2)
    miw.commit()
    miw.close()
    miw.getDirectory.close
    // Go to max one segment with custom codec
    iwc = new IndexWriterConfig(analyzer)
    iwc.setCodec(codec)
    iwc.setIndexSort(sort)
    iwc.setUseCompoundFile(false)
    miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
    miw.forceMerge(1)
    miw.commit()
    miw.close()
    miw.getDirectory.close()
    // Make sure the one segment we have is encoded with the custom codec (if we accidentally got to one segment in the first phase)
    iwc = new IndexWriterConfig(analyzer)
    iwc.setCodec(codec)
    iwc.setIndexSort(sort)
    iwc.setUseCompoundFile(false)
    var mp = new UpgradeIndexMergePolicy(iwc.getMergePolicy()) {
      override protected def shouldUpgradeSegment(si: SegmentCommitInfo): Boolean =  si.info.getCodec.getName != codec.getName
    }
    iwc.setMergePolicy(mp)
    miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
    miw.forceMerge(1)
    miw.commit()
    miw.close()
    miw.getDirectory.close()
    logger.info(f"Merged index ${path}%s. Went from ${size}%,d bytes to ${getFileTreeSize(path)}%,d bytes.")
  }
  
  def mergeIndices(path: String, single: Boolean)(implicit ec: ExecutionContext): Unit = {
    val df = Future { merge(path+"/dindex", new Sort(new SortField("documentID",SortField.Type.STRING))) }
    if (single) Await.result(df, Duration.Inf)
    val dpf = Future { merge(path+"/dpindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("partID", SortField.Type.LONG))) }
    if (single) Await.result(dpf, Duration.Inf)
    val sf = Future { merge(path+"/sindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("sectionID", SortField.Type.LONG))) }
    if (single) Await.result(sf, Duration.Inf)
    val pf = Future { merge(path+"/pindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG))) }
    if (single) Await.result(pf, Duration.Inf)
    Await.result(df, Duration.Inf)
    Await.result(dpf, Duration.Inf)
    Await.result(sf, Duration.Inf)
    Await.result(pf, Duration.Inf)
  }
  
  private val paragraphs = new AtomicLong
  private val sections = new AtomicLong
  private val documentparts = new AtomicLong
  
  private def index(id: String, file: File): Future[Unit] = Future({
    val filePrefix = file.getName.replace("_metadata.xml","")
    logger.info("Processing: "+file.getPath.replace("_metadata.xml","*"))
    var totalPages = 0
    val xmls = Source.fromFile(file)
    implicit val xml = new XMLEventReader(xmls)
    val md = new Document()
    md.add(new Field("collectionID",id,StringField.TYPE_NOT_STORED))
    md.add(new SortedDocValuesField("collectionID",new BytesRef(id)))
    var documentID: String = null
    var estcID: String = null
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"documentID",_,_) | EvElemStart(_,"PSMID",_,_) =>
        documentID = readContents
        val f = new Field("documentID",documentID,StringField.TYPE_NOT_STORED)
        md.add(f)
        md.add(new SortedDocValuesField("documentID", new BytesRef(documentID)))
      case EvElemStart(_,"ESTCID",_,_) =>
        estcID = readContents
        val f = new Field("ESTCID",estcID,StringField.TYPE_NOT_STORED)
        md.add(f)
        md.add(new SortedDocValuesField("ESTCID", new BytesRef(estcID)))
      case EvElemStart(_,"bibliographicID",attr,_) if (attr("type")(0).text == "ESTC") =>
        estcID = readContents
        val f = new Field("ESTCID",estcID,StringField.TYPE_NOT_STORED)
        md.add(f)
        md.add(new SortedDocValuesField("ESTCID", new BytesRef(estcID))) 
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
                  val ip = new IntPoint("pubDateStart",any.toInt) 
                  md.add(ip)
                  val f = new NumericDocValuesField("pubDateStart",any.toInt)
                  md.add(f)
              }
              case EvElemStart(_,"pubDateEnd",_,_) => readContents match {
                case any =>
                  endDateFound = true
                  val ip = new IntPoint("pubDateEnd",any.replaceAll("00","99").toInt) 
                  md.add(ip)
                  val f = new NumericDocValuesField("pubDateEnd",any.replaceAll("00","99").toInt)
                  md.add(f)
              }
              case EvElemEnd(_,"pubDate") => break = true
              case _ => 
            }
          }
          if (!endDateFound && startDate != null) {
            val ip = new IntPoint("pubDateEnd",startDate.replaceAll("00","99").toInt) 
            md.add(ip)
            val f = new NumericDocValuesField("pubDateEnd",startDate.replaceAll("00","99").toInt)
            md.add(f)
          }
        case "" =>
        case "1809" =>
          val ip = new IntPoint("pubDateStart",18090000)
          val ip2 = new IntPoint("pubDateEnd",18099999)
          md.add(ip)
          md.add(ip2)
          val f = new NumericDocValuesField("pubDateStart",18090000)
          val f2 = new NumericDocValuesField("pubDateEnd",18099999)
          md.add(f)
          md.add(f2)
        case any => 
          val ip = new IntPoint("pubDateStart", any.replaceAll("01","00").toInt) 
          val ip2 = new IntPoint("pubDateEnd", any.replaceAll("01", "99").toInt)
          md.add(ip)
          md.add(ip2)
          val f = new NumericDocValuesField("pubDateStart",any.replaceAll("01","00").toInt)
          val f2 = new NumericDocValuesField("pubDateEnd", any.replaceAll("01", "99").toInt)
          md.add(f)
          md.add(f2)
      }
      case EvElemStart(_,"totalPages",_,_) =>
        val tp = readContents
        if (!tp.isEmpty) {
          totalPages = tp.toInt
          val ip = new IntPoint("totalPages", totalPages) 
          md.add(ip)
          val f = new NumericDocValuesField("totalPages", totalPages)
          md.add(f)
        }
      case EvElemStart(_,"language",_,_) =>
        val c = readContents
        val f = new Field("language",c,StringField.TYPE_NOT_STORED)
        md.add(f)
        md.add(new SortedDocValuesField("language",new BytesRef(c)))
      case EvElemStart(_,"module",_,_) =>
        val c = readContents
        val f = new Field("module",c,StringField.TYPE_NOT_STORED)
        md.add(f)
        md.add(new SortedDocValuesField("module",new BytesRef(c)))
      case EvElemStart(_,"fullTitle",_,_) => 
        val f = new Field("fullTitle",readContents,contentFieldType)
        md.add(f)
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
    md.add(new IntPoint("documentLength", totalLength))
    md.add(new NumericDocValuesField("documentLength", totalLength))
    md.add(new IntPoint("totalParagraphs", totalParagraphs))
    md.add(new NumericDocValuesField("totalParagraphs", totalParagraphs))
    val d = new Document()
    for (f <- md.asScala) d.add(f)
    for (file <- filesToProcess.sortBy(x => x.getName.substring(x.getName.indexOf('_') + 1, x.getName.indexOf('_', x.getName.indexOf('_') + 1).toInt))) {
      val dpcontents = new StringBuilder
      val dpd = new Document()
      val documentPartNum = documentparts.getAndIncrement
      val dpf = new Field("partID", ""+documentPartNum, StringField.TYPE_NOT_STORED)
      dpd.add(new NumericDocValuesField("partID", documentPartNum))
      dpd.add(dpf)
      for (f <- md.asScala) dpd.add(f)
      if (new File(file.getPath.replace(".txt","-graphics.csv")).exists)
        for (r <- CSVReader(file.getPath.replace(".txt","-graphics.csv"))) {
          val gtype = if (r(1)=="") "unknown" else r(1)
          val f = new Field("containsGraphicOfType",gtype, notStoredStringFieldWithTermVectors)
          dpd.add(f)
          d.add(f)
          if (r(3)!="") {
            val f = new Field("containsGraphicCaption", r(3), normsOmittingStoredTextField)
            dpd.add(f)
            d.add(f)
          }
        }
      var hds = Seq(1,2,3).map(w => None.asInstanceOf[Option[Document]]).toBuffer
      var currentSectionFields = Seq(1,2,3).map(w => None.asInstanceOf[Option[Field]]).toBuffer
      val documentPartType = fileRegex.findFirstMatchIn(file.getName).get.group(1)
      val hcontents = hds.map(w => new StringBuilder)
      val pcontents = new StringBuilder
      val fl = Source.fromFile(file)
      for (line <- fl.getLines) {
        if (line.isEmpty) {
          if (!pcontents.isEmpty) {
            val d2 = new Document()
            for (f <- md.asScala) d2.add(f)
            val paragraphNum = paragraphs.getAndIncrement
            d2.add(new NumericDocValuesField("paragraphID", paragraphNum))
            d2.add(new Field("content",pcontents.toString,contentFieldType))
            val tokens = getNumberOfTokens(pcontents.toString)
            d2.add(new IntPoint("contentTokens",tokens))
            d2.add(new NumericDocValuesField("contentTokens",tokens))
            d2.add(new Field("documentPartType", documentPartType, StringField.TYPE_NOT_STORED))
            d2.add(new SortedDocValuesField("documentPartType", new BytesRef(documentPartType)))
            d2.add(dpf)
            currentSectionFields.foreach(_.foreach(d2.add(_)))
            pcontents.clear()
            piw.addDocument(d2)
          }
        } else
          pcontents.append(line)
        if (line.startsWith("# ") || line.startsWith("## ") || line.startsWith("### ")) {
          val level =
            if (line.startsWith("### ")) 2
            else if (line.startsWith("## ")) 1
            else 0
          for (
              i <- level until hds.length;
              contents = hcontents(i)
          ) {
            hds(i).foreach { d2 => 
              if (!contents.isEmpty) {
                d2.add(new Field("content",contents.toString,contentFieldType))
                val tokens = getNumberOfTokens(contents.toString)
                d2.add(new IntPoint("contentTokens",tokens))
                d2.add(new NumericDocValuesField("contentTokens",tokens))
                contents.clear()
              }
              d2.add(new Field("documentPartType", documentPartType, StringField.TYPE_NOT_STORED))
              d2.add(new SortedDocValuesField("documentPartType", new BytesRef(documentPartType)))
              siw.addDocument(d2)
            }
            hds(i) = None
          }
          val d2 = new Document()
          for (f <- md.asScala) d2.add(f)
          d2.add(new IntPoint("headingLevel", level))
          d2.add(new SortedDocValuesField("headingLevel", new BytesRef(level)))
          hds(level) = Some(d2)
          val f = new Field("heading",line.substring(level+2),contentFieldType)
          d2.add(f)
          val sectionNum = sections.getAndIncrement()
          d2.add(new NumericDocValuesField("sectionID", sectionNum))
          d2.add(dpf)
          val sid = new Field("sectionID", ""+sectionNum, StringField.TYPE_NOT_STORED)
          d2.add(sid)
          currentSectionFields(level) = Some(sid)
          for (i <- 0 until level) hcontents(i).append(line)
        } else
          for (i <- 0 until hds.length) hds(i).foreach(d=> hcontents(i).append(line))
        dpcontents.append(line)
      }
      fl.close()
      for (
          i <- 0 until hds.length;
          contents = hcontents(i)
      )
        hds(i).foreach { d2 => 
          if (!contents.isEmpty) {
            val scontents = contents.toString.trim
            d2.add(new Field("content",scontents,contentFieldType))
            val tokens = getNumberOfTokens(scontents)
            d2.add(new IntPoint("contentTokens",tokens))
            d2.add(new NumericDocValuesField("contentTokens",tokens))
          }
          d2.add(new Field("documentPartType", documentPartType, StringField.TYPE_NOT_STORED))
          d2.add(new SortedDocValuesField("documentPartType", new BytesRef(documentPartType)))
          siw.addDocument(d2)
        }
      dpd.add(new Field("documentPartType", documentPartType, StringField.TYPE_NOT_STORED))
      dpd.add(new SortedDocValuesField("documentPartType", new BytesRef(documentPartType)))
      val dpcontentsS = dpcontents.toString.trim
      dpd.add(new Field("content", dpcontentsS, contentFieldType))
      val not = getNumberOfTokens(dpcontentsS)
      dpd.add(new IntPoint("contentTokens", not))
      dpd.add(new NumericDocValuesField("contentTokens", not))
      dpiw.addDocument(dpd)
      dcontents.append(dpcontentsS)
      dcontents.append("\n\n")
    }
    val dcontentsS = dcontents.toString.trim
    d.add(new Field("content", dcontentsS, contentFieldType))
    val not = getNumberOfTokens(dcontentsS)
    d.add(new IntPoint("contentTokens", not))
    d.add(new NumericDocValuesField("contentTokens", not))
    diw.addDocument(d)
    logger.info("Processed: "+file.getPath.replace("_metadata.xml","*"))
  })(ec)
  
  var diw, dpiw, siw, piw = null.asInstanceOf[IndexWriter]
  
  def getFileTreeSize(path: String): Long = getFileTree(new File(path)).foldLeft(0l)((s,f) => s+f.length)    
  
  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
  
  def main(args: Array[String]): Unit = {
    // document level
    diw = iw(args.last+"/dindex")
    // document part level
    dpiw = iw(args.last+"/dpindex")
    // section level
    siw = iw(args.last+"/sindex") 
    // paragraph level
    piw = iw(args.last+"/pindex")
  
    diw.deleteAll()
    diw.commit()
    dpiw.deleteAll()
    dpiw.commit()
    siw.deleteAll()
    siw.commit()
    piw.deleteAll()
    piw.commit()
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
    implicit val iec = ExecutionContext.Implicits.global
    val all = Promise[Unit]()
    val poison = Future(())
    val bq = new ArrayBlockingQueue[Future[Unit]](queueCapacity)
    Future { 
      args.dropRight(1).toStream.flatMap(p => {
        val parts = p.split(':')
        getFileTree(new File(parts(0))).map((parts(1),_))
      }).filter(_._2.getName.endsWith("_metadata.xml")).foreach(pair => {
        val path = pair._2.getPath.replace("_metadata.xml","*")
        val f = index(pair._1, pair._2)
        f.recover { 
          case cause =>
            logger.error("An error has occured processing "+path+": " + getStackTraceAsString(cause))
            throw new Exception("An error has occured processing "+path, cause) 
        }
        bq.put(f)
      })
      bq.put(poison)
    }
    var f = bq.take()
    while (f ne poison) {
      Await.ready(f, Duration.Inf)
      f.onFailure { case t => all.failure(t) }
      f = bq.take()
    }
    if (!all.isCompleted) all.success(())
    all.future.onFailure { case t => logger.error("Processing of at least one file resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t)) }
    all.future.onSuccess { case _ => logger.info("Successfully processed all files.") }
    ec.shutdown()
    diw.close()
    diw.getDirectory.close()
    dpiw.close()
    dpiw.getDirectory.close()
    siw.close()
    siw.getDirectory.close()
    piw.close()
    piw.getDirectory.close()
    mergeIndices(args.last, false)
  }
}
