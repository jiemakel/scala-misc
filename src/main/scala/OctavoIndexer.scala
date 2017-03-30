import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.ArrayBlockingQueue
import java.io.File
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.CharArraySet
import fi.seco.lucene.FSTOrdTermVectorsCodec
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.MMapDirectory
import java.nio.file.FileSystems
import org.apache.lucene.document.FieldType
import org.apache.lucene.document.TextField
import org.apache.lucene.document.StringField
import org.apache.lucene.search.Sort
import org.apache.lucene.index.LogDocMergePolicy
import java.io.StringWriter
import java.io.PrintWriter
import org.apache.lucene.index.UpgradeIndexMergePolicy
import org.apache.lucene.index.SegmentCommitInfo
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.json4s._
import org.json4s.JsonDSL._

class OctavoIndexer extends LazyLogging {
  
  private val numWorkers = sys.runtime.availableProcessors
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
      
  def getFileTreeSize(path: String): Long = getFileTree(new File(path)).foldLeft(0l)((s,f) => s+f.length)    
  
  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
      
  val analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET)
  
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
  
  val codec = new FSTOrdTermVectorsCodec()
  
  def iw(path: String): IndexWriter = {
    val iwc = new IndexWriterConfig(analyzer)
    iwc.setUseCompoundFile(false)
    iwc.setMergePolicy(new LogDocMergePolicy())
    new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
  }

  val contentFieldType = new FieldType(TextField.TYPE_STORED)
  contentFieldType.setOmitNorms(true)
  contentFieldType.setStoreTermVectors(true)
  
  val normsOmittingStoredTextField = new FieldType(TextField.TYPE_STORED)
  normsOmittingStoredTextField.setOmitNorms(true)
  
  val notStoredStringFieldWithTermVectors = new FieldType(StringField.TYPE_NOT_STORED)
  notStoredStringFieldWithTermVectors.setStoreTermVectors(true)
  
  def merge(path: String, sort: Sort): Unit = {
    logger.info("Merging index at "+path)
    var size = getFileTreeSize(path) 
    // First, go to max two segments with general codec
    var iwc = new IndexWriterConfig(analyzer)
    iwc.setIndexSort(sort)
    iwc.setUseCompoundFile(false)
    iwc.setMergePolicy(new LogDocMergePolicy())
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
    iwc.setMergePolicy(new LogDocMergePolicy())
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
    val mp = new UpgradeIndexMergePolicy(new LogDocMergePolicy()) {
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
  
  def mergeIndices(iws: Traversable[(String, Sort)], single: Boolean = false)(implicit ec: ExecutionContext): Unit = {
    iws.map(p => { 
      val f = Future { merge(p._1, p._2) }
      if (single) Await.result(f, Duration.Inf)
      f
     }).foreach(Await.result(_, Duration.Inf))
  }
  
  def clear(iw: IndexWriter) {
    iw.deleteAll()
    iw.commit()
  }
  
  def close(iw: IndexWriter) {
    iw.close()
    iw.getDirectory.close()
  }
    
}