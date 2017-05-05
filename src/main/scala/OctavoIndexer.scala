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
import org.apache.lucene.index.IndexCommit
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.util.BytesRef
import org.apache.lucene.document.SortedDocValuesField
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.IntPoint
import org.apache.lucene.document.LongPoint
import org.apache.lucene.document.SortedNumericDocValuesField

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

  val indexingCodec = new TermVectorFilteringLucene62Codec()
  
  class FieldPair[F1 <: Field,F2 <: Field](val indexField: F1, val storedField: F2, docs: Document*) {
    docs.foreach(add)
    def add(d: Document) = {
      d.add(indexField)
      d.add(storedField)
    }
  }
  
  class StringSDVFieldPair(field: String, docs: Document*) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) = {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
    }
  }

  class StringNDVFieldPair(field: String, docs: Document*) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new NumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) = {
      indexField.setStringValue(""+v)
      storedField.setLongValue(v)
    }
  }
  
  class StringSNDVFieldPair(field: String, docs: Document*) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedNumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) = {
      indexField.setStringValue(""+v)
      storedField.setLongValue(v)
    }
  }
  
  class IntPointNDVFieldPair(field: String, docs: Document*) extends FieldPair(new IntPoint("field", 0), new NumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Int) = {
      indexField.setIntValue(v)
      storedField.setLongValue(v)
    }
  }

  class IntPointSNDVFieldPair(field: String, docs: Document*) extends FieldPair(new IntPoint("field", 0), new SortedNumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Int) = {
      indexField.setIntValue(v)
      storedField.setLongValue(v)
    }
  }

  class LongPointNDVFieldPair(field: String, docs: Document*) extends FieldPair(new LongPoint("field", 0l), new NumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) = {
      indexField.setLongValue(v)
      storedField.setLongValue(v)
    }
  }

  class LongPointSNDVFieldPair(field: String, docs: Document*) extends FieldPair(new LongPoint("field", 0l), new SortedNumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) = {
      indexField.setLongValue(v)
      storedField.setLongValue(v)
    }
  }

  def iwc(sort: Sort, bufferSizeInMB: Double): IndexWriterConfig = {
    val iwc = new IndexWriterConfig(analyzer)
    iwc.setUseCompoundFile(false)
    iwc.setMergePolicy(new LogDocMergePolicy())
    iwc.setCodec(indexingCodec)
    iwc.setRAMBufferSizeMB(bufferSizeInMB)
    iwc.setIndexSort(sort)
    val mergeScheduler = new ConcurrentMergeScheduler()
    mergeScheduler.setMaxMergesAndThreads(sys.runtime.availableProcessors + 5, sys.runtime.availableProcessors)
    mergeScheduler.disableAutoIOThrottle()
    iwc.setMergeScheduler(mergeScheduler)
    iwc.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    return iwc
  }
  
  def iw(path: String, sort: Sort, bufferSizeInMB: Double): IndexWriter = {
    new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc(sort, bufferSizeInMB))
  }

  val contentFieldType = new FieldType(TextField.TYPE_STORED)
  contentFieldType.setOmitNorms(true)
  contentFieldType.setStoreTermVectors(true)
  
  val normsOmittingStoredTextField = new FieldType(TextField.TYPE_STORED)
  normsOmittingStoredTextField.setOmitNorms(true)
  
  val notStoredStringFieldWithTermVectors = new FieldType(StringField.TYPE_NOT_STORED)
  notStoredStringFieldWithTermVectors.setStoreTermVectors(true)
  
  val finalCodec = new FSTOrdTermVectorsCodec()
  
  def merge(path: String, sort: Sort, bufferSizeInMB: Double): Unit = {
    logger.info("Merging index at "+path)
    var size = getFileTreeSize(path) 
    // First, go to max two segments with general codec
/*    var miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc(sort, bufferSizeInMB))
    miw.forceMerge(2)
    miw.commit()
    miw.close()
    miw.getDirectory.close */
    // Go to max one segment with custom codec
    var fiwc = iwc(sort, bufferSizeInMB)
    var miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), fiwc)
    miw.forceMerge(1)
    miw.commit()
    miw.close()
    miw.getDirectory.close()
    // Make sure the one segment we have is encoded with the custom codec (if we accidentally got to one segment before merging)
    fiwc = iwc(sort, bufferSizeInMB)
    fiwc.setCodec(finalCodec)
    fiwc.setMergePolicy(new UpgradeIndexMergePolicy(fiwc.getMergePolicy) {
      override protected def shouldUpgradeSegment(si: SegmentCommitInfo): Boolean = si.info.getCodec.getName != finalCodec.getName
    })
    miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), fiwc)
    miw.forceMerge(1)
    miw.commit()
    miw.close()
    miw.getDirectory.close()
    logger.info(f"Merged index ${path}%s. Went from ${size}%,d bytes to ${getFileTreeSize(path)}%,d bytes.")
  }
  
  def mergeIndices(iws: Traversable[(String, Sort, Double)], single: Boolean = false): Unit = {
    iws.map(p => { 
      val f = Future { merge(p._1, p._2, p._3) }(ExecutionContext.Implicits.global)
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