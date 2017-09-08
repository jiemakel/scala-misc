import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.ArrayBlockingQueue
import java.io.File
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.CharArraySet
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
import org.rogach.scallop.ScallopConf
import scala.language.postfixOps
import org.apache.lucene.document.SortedSetDocValuesField
import fi.seco.lucene.PerFieldPostingsFormatOrdTermVectorsCodec
import org.apache.lucene.codecs.Codec
import fi.seco.lucene.OrdExposingFSTOrdPostingsFormat
import org.apache.lucene.codecs.blocktreeords.BlockTreeOrdsPostingsFormat
import org.apache.lucene.codecs.PostingsFormat
import org.apache.lucene.document.DoublePoint
import org.apache.lucene.document.DoubleDocValuesField
import org.apache.lucene.document.FloatDocValuesField
import org.apache.lucene.document.FloatPoint
import scala.util.Try

class OctavoIndexer extends ParallelProcessor {
   
  val analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET)

  def lsplit[A](str: List[A],pos: List[Int]): List[List[A]] = {
    val (rest, result) = pos.foldRight((str, List[List[A]]())) {
      case (curr, (s, res)) =>
        val (rest, split) = s.splitAt(curr)
        (rest, split :: res)
    }
    rest :: result
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

  val indexingCodec = new TermVectorFilteringLucene62Codec()
  
  class FieldPair[F1 <: Field,F2 <: Field](val indexField: F1, val storedField: F2, docs: Document*) {
    docs.foreach(add)
    def add(d: Document) = {
      d.add(indexField)
      d.add(storedField)
    }
  }
  
  val normsOmittingNotStoredTextField = new FieldType(TextField.TYPE_NOT_STORED)
  normsOmittingNotStoredTextField.setOmitNorms(true)
  
  class TextSDVFieldPair(field: String, docs: Document*)  extends FieldPair(new Field(field, "", normsOmittingNotStoredTextField), new SortedDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) = {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
    }
  }
  
  class TextSSDVFieldPair(field: String, docs: Document*)  extends FieldPair(new Field(field, "", normsOmittingNotStoredTextField), new SortedSetDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) = {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
    }
  }

  class StringSSDVFieldPair(field: String, docs: Document*)  extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedSetDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) = {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
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

  class FloatPointFDVFieldPair(field: String, docs: Document*) extends FieldPair(new FloatPoint(field, 0.0f), new FloatDocValuesField(field, 0.0f), docs:_*) {
    def setValue(v: Float) = {
      indexField.setFloatValue(v)
      storedField.setFloatValue(v)
    }
  }

  
  class DoublePointDDVFieldPair(field: String, docs: Document*) extends FieldPair(new DoublePoint(field, 0.0), new DoubleDocValuesField(field, 0.0), docs:_*) {
    def setValue(v: Double) = {
      indexField.setDoubleValue(v)
      storedField.setDoubleValue(v)
    }
  }
  
  class IntPointNDVFieldPair(field: String, docs: Document*) extends FieldPair(new IntPoint(field, 0), new NumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Int) = {
      indexField.setIntValue(v)
      storedField.setLongValue(v)
    }
  }

  class IntPointSDVFieldPair(field: String, docs: Document*) extends FieldPair(new IntPoint(field, 0), new SortedDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: Int, sv: String) = {
      indexField.setIntValue(v)
      storedField.setBytesValue(new BytesRef(sv))
    }
  }

  
  class IntPointSNDVFieldPair(field: String, docs: Document*) extends FieldPair(new IntPoint(field, 0), new SortedNumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Int) = {
      indexField.setIntValue(v)
      storedField.setLongValue(v)
    }
  }

  class LongPointNDVFieldPair(field: String, docs: Document*) extends FieldPair(new LongPoint(field, 0l), new NumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) = {
      indexField.setLongValue(v)
      storedField.setLongValue(v)
    }
  }

  class LongPointSNDVFieldPair(field: String, docs: Document*) extends FieldPair(new LongPoint(field, 0l), new SortedNumericDocValuesField(field, 0), docs:_*) {
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
    if (sort!=null && sort.getSort.length!=0) iwc.setIndexSort(sort)
    val mergeScheduler = new ConcurrentMergeScheduler()
    mergeScheduler.setMaxMergesAndThreads(sys.runtime.availableProcessors + 5, sys.runtime.availableProcessors)
    mergeScheduler.disableAutoIOThrottle()
    iwc.setMergeScheduler(mergeScheduler)
    iwc.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    return iwc
  }
  
  def iw(path: String, bufferSizeInMB: Double): IndexWriter = {
    logger.info("Creating IndexWriter "+path+" with a memory buffer of "+bufferSizeInMB+"MB")
    val d = new MMapDirectory(FileSystems.getDefault().getPath(path))
    d.listAll().map(d.deleteFile(_))
    new IndexWriter(d, iwc(null, bufferSizeInMB))
  }
  

  val contentFieldType = new FieldType(TextField.TYPE_STORED)
  contentFieldType.setOmitNorms(true)
  contentFieldType.setStoreTermVectors(true)
  contentFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
  
  val normsOmittingStoredTextField = new FieldType(TextField.TYPE_STORED)
  normsOmittingStoredTextField.setOmitNorms(true)
  
  val notStoredStringFieldWithTermVectors = new FieldType(StringField.TYPE_NOT_STORED)
  notStoredStringFieldWithTermVectors.setStoreTermVectors(true)
  
  def merge(path: String, sort: Sort, bufferSizeInMB: Double, finalCodec: Codec): Unit = {
    logger.info("Merging index at "+path)
    var size = getFileTreeSize(path)
    var fiwc = iwc(sort, bufferSizeInMB)
    var miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), fiwc)
    if (finalCodec != null) {
      logger.info("Merging index at "+path+" to max two segments")
      miw.forceMerge(2)
    } else {
      logger.info("Merging index at "+path+" to max one segment")
      miw.forceMerge(1)
    }
    miw.commit()
    miw.close()
    miw.getDirectory.close()
    if (finalCodec != null) {
      logger.info("Merging index at "+path+" to max one segment using final codec")
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
    }
    logger.info(f"Merged index ${path}%s. Went from ${size}%,d bytes to ${getFileTreeSize(path)}%,d bytes.")
  }
  
  def toCodec(postingsFormatS: String, perFieldPostings: Seq[String]): Codec = {
    val finalCodec = new PerFieldPostingsFormatOrdTermVectorsCodec()
    val postingsFormat = postingsFormatS match {
      case "fst" => new OrdExposingFSTOrdPostingsFormat()
      case "blocktree" => new BlockTreeOrdsPostingsFormat()
      case any => throw new IllegalArgumentException("Unknown postings format "+any) 
    }
    finalCodec.perFieldPostingsFormat = perFieldPostings.map((_, postingsFormat)).toMap
    finalCodec
  }  
  
  def close(iw: IndexWriter) = if (iw != null) {
    logger.info("Closing index "+iw.getDirectory)
    iw.close()
    iw.getDirectory.close()
    logger.info("Closed index "+iw.getDirectory)
  }
  
  abstract class AOctavoOpts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val index = opt[String](required = true)
    val indexMemoryMb = opt[Long](default = Some(Runtime.getRuntime.maxMemory()/1024/1024/2), validate = _>0)
    val directories = trailArg[List[String]](required = false)
    val onlyMerge = opt[Boolean](default = Some(false))
  }
  
  class OctavoOpts(arguments: Seq[String]) extends AOctavoOpts(arguments) {
    verify()
  }
      
}