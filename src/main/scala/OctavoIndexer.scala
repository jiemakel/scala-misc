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
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import org.apache.lucene.codecs.lucene62.Lucene62Codec
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode
import org.apache.lucene.index.ConcurrentMergeScheduler

class OctavoIndexer extends ParallelProcessor {
   
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
  
  def iw(path: String): IndexWriter = {
    val iwc = new IndexWriterConfig(analyzer)
    iwc.setUseCompoundFile(false)
    iwc.setMergePolicy(new LogDocMergePolicy())
    iwc.setCodec(new Lucene62Codec(Mode.BEST_SPEED))
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
    val mergePolicy = new LogDocMergePolicy()
    iwc.setMergePolicy(mergePolicy)
    val mergeScheduler = new ConcurrentMergeScheduler()
    mergeScheduler.setMaxMergesAndThreads(sys.runtime.availableProcessors + 5, sys.runtime.availableProcessors)
    mergeScheduler.disableAutoIOThrottle()
    iwc.setMergeScheduler(mergeScheduler)
    var miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
    miw.forceMerge(2)
    miw.commit()
    miw.close()
    miw.getDirectory.close
    // Go to max one segment with custom codec
    iwc = new IndexWriterConfig(analyzer)
    val finalCodec = new FSTOrdTermVectorsCodec()
    iwc.setCodec(finalCodec)
    iwc.setIndexSort(sort)
    iwc.setUseCompoundFile(false)
    iwc.setMergePolicy(mergePolicy)
    iwc.setMergeScheduler(mergeScheduler)
    miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
    miw.forceMerge(1)
    miw.commit()
    miw.close()
    miw.getDirectory.close()
    // Make sure the one segment we have is encoded with the custom codec (if we accidentally got to one segment in the first phase)
    iwc = new IndexWriterConfig(analyzer)
    iwc.setCodec(finalCodec)
    iwc.setIndexSort(sort)
    iwc.setUseCompoundFile(false)
    val mp = new UpgradeIndexMergePolicy(mergePolicy) {
      override protected def shouldUpgradeSegment(si: SegmentCommitInfo): Boolean =  si.info.getCodec.getName != finalCodec.getName
    }
    iwc.setMergePolicy(mp)
    iwc.setMergeScheduler(mergeScheduler)
    miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
    miw.forceMerge(1)
    miw.commit()
    miw.close()
    miw.getDirectory.close()
    logger.info(f"Merged index ${path}%s. Went from ${size}%,d bytes to ${getFileTreeSize(path)}%,d bytes.")
  }
  
  def mergeIndices(iws: Traversable[(String, Sort)], single: Boolean = false): Unit = {
    iws.map(p => { 
      val f = Future { merge(p._1, p._2) }(ExecutionContext.Implicits.global)
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