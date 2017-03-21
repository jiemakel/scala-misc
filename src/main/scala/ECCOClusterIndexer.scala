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

object ECCOClusterIndexer extends LazyLogging {
  
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
      
  val analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET)
  
  val codec = new FSTOrdTermVectorsCodec()
  
  def iw(path: String): IndexWriter = {
    val iwc = new IndexWriterConfig(analyzer)
    iwc.setUseCompoundFile(false)
    new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
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
    val df = Future { merge(path+"/dindex", new Sort(new SortField("clusterID",SortField.Type.INT))) }
    if (single) Await.result(df, Duration.Inf)
    Await.result(df, Duration.Inf)
  }
  
  private def index(id: String, cluster: Cluster): Future[Unit] = Future {
    for (m <- cluster.matches) {
      val d = new Document()
      d.add(new NumericDocValuesField("clusterID", cluster.id))
      d.add(new IntPoint("avgLength", cluster.avgLength))
      d.add(new NumericDocValuesField("avgLength", cluster.avgLength))
      d.add(new IntPoint("count", cluster.matches.size))
      d.add(new NumericDocValuesField("count", cluster.matches.size))
      d.add(new Field("ESTCID",m.estcID,StringField.TYPE_NOT_STORED))
      d.add(new SortedDocValuesField("ESTCID", new BytesRef(m.estcID)))
      d.add(new Field("title", m.title, contentFieldType))
      d.add(new Field("text", m.text, contentFieldType))
      if (!m.author.isEmpty) d.add(new Field("author", m.author, normsOmittingStoredTextField))
      d.add(new IntPoint("startIndex", m.startIndex))
      d.add(new NumericDocValuesField("startIndex", m.startIndex))
      d.add(new IntPoint("endIndex", m.endIndex))
      d.add(new NumericDocValuesField("endIndex", m.endIndex))
      d.add(new IntPoint("year", m.year))
      d.add(new NumericDocValuesField("year", m.year))
      diw.addDocument(d)
    }
  }(ec)
  
  var diw = null.asInstanceOf[IndexWriter]
  
  def getFileTreeSize(path: String): Long = getFileTree(new File(path)).foldLeft(0l)((s,f) => s+f.length)    
  
  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
  
  class Match {
    var estcID: String = null
    var title: String = null
    var author: String = null
    var startIndex: Int = -1
    var endIndex: Int = -1
    var year: Int = -1
    var text: String = null
  }
  
  class Cluster {
    var id: Int = -1
    var avgLength: Int = -1
    var count: Int = -1
    var matches: ArrayBuffer[Match] = new ArrayBuffer()
  }
  
  def main(args: Array[String]): Unit = {
    // document level
    diw = iw(args.last+"/dindex")
  
    diw.deleteAll()
    diw.commit()
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
    val sf = Future {
      args.dropRight(1).flatMap(n => getFileTree(new File(n))).parStream.filter(f => f.getName.endsWith(".gz")).forEach(file => {
        parse(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))), (p: Parser) => {
          val path = file.getParentFile.getName
          var cluster: Cluster = null
          var token = p.nextToken
          while (token != End) {
            token match {
              case FieldStart(field) if (field.startsWith("cluster_")) =>
                cluster = new Cluster
                cluster.id = field.substring(8).toInt
                while (token != CloseObj) {
                  token match {
                    case FieldStart("Avglength") => cluster.avgLength = p.nextToken.asInstanceOf[IntVal].value.toInt
                    case FieldStart("Count") => 
                      cluster.count = p.nextToken.asInstanceOf[IntVal].value.toInt
                    case FieldStart("Hits") => 
                      var cm: Match = null
                      while (token != CloseArr) {
                        token match {
                          case OpenObj => cm = new Match()
                          case FieldStart("end_index") => cm.endIndex = p.nextToken.asInstanceOf[IntVal].value.toInt
                          case FieldStart("start_index") => cm.startIndex = p.nextToken.asInstanceOf[IntVal].value.toInt
                          case FieldStart("title") => cm.title = p.nextToken.asInstanceOf[StringVal].value
                          case FieldStart("author") => cm.author = p.nextToken.asInstanceOf[StringVal].value
                          case FieldStart("book_id") => cm.estcID = p.nextToken.asInstanceOf[StringVal].value
                          case FieldStart("year") => cm.year = Try(p.nextToken.asInstanceOf[StringVal].value.toInt).getOrElse(-1)
                          case FieldStart("text") => cm.text = p.nextToken.asInstanceOf[StringVal].value
                          case CloseObj => cluster.matches += cm
                          case _ => 
                        }
                        token = p.nextToken
                      }
                    case _ =>
                  }
                  token = p.nextToken
                }
                val f = index(path, cluster)
                f.recover { 
                  case cause =>
                    logger.error("An error has occured processing "+path+": " + getStackTraceAsString(cause))
                    throw new Exception("An error has occured processing "+path, cause) 
                }
                bq.put(f)
              case _ =>
            }
            token = p.nextToken
          }
        })
        logger.info("File "+file+" processed.")
      })
      logger.info("All sources processed.")
      bq.put(poison)
    }
    sf.onComplete { 
      case Failure(t) => logger.error("Processing of at least one file resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t))
      case Success(_) =>
    }
    var f = bq.take()
    while (f ne poison) {
      Await.ready(f, Duration.Inf)
      f.onComplete { 
        case Failure(t) => all.failure(t)
        case Success(_) =>
      }
      f = bq.take()
    }
    if (!all.isCompleted) all.success(Unit)
    all.future.onComplete {
      case Success(_) => logger.info("Successfully processed all files.")
      case Failure(t) => logger.error("Processing of at least one file resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t))
    }
    ec.shutdown()
    diw.close()
    diw.getDirectory.close()
    mergeIndices(args.last, false)
  }
}
