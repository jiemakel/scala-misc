import java.nio.file.FileSystems
import java.util.regex.Pattern

import fi.seco.lucene.{Lucene70PerFieldPostingsFormatOrdTermVectorsCodec, OrdExposingFSTOrdPostingsFormat}
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.WhitespaceTokenizer
import org.apache.lucene.analysis.miscellaneous.{HyphenatedWordsFilter, LengthFilter}
import org.apache.lucene.analysis.pattern.PatternReplaceFilter
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
import org.apache.lucene.analysis._
import org.apache.lucene.codecs.Codec
import org.apache.lucene.codecs.blocktreeords.BlockTreeOrdsPostingsFormat
import org.apache.lucene.document._
import org.apache.lucene.index._
import org.apache.lucene.search.Sort
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.util.BytesRef
import org.joda.time.format.DateTimeFormatter
import org.rogach.scallop.ScallopConf

import scala.language.postfixOps

class OctavoIndexer extends ParallelProcessor {

  def removeFields(field: String, docs: Document*): Unit = {
    for (doc <- docs) doc.removeFields(field)
  }

  val analyzer = new Analyzer() {
    override def createComponents(fieldName: String) = {
      val src = new WhitespaceTokenizer()
      var tok: TokenFilter = new HyphenatedWordsFilter(src)
      tok = new PatternReplaceFilter(tok,Pattern.compile("^\\p{Punct}*(.*?)\\p{Punct}*$"),"$1", false)
      tok = new LowerCaseFilter(tok)
      tok = new LengthFilter(tok, 1, Int.MaxValue)
      new TokenStreamComponents(src,tok)
    }
  }

  def lsplit[A](str: List[A],pos: List[Int]): List[List[A]] = {
    val (rest, result) = pos.foldRight((str, List[List[A]]())) {
      case (curr, (s, res)) =>
        val (rest, split) = s.splitAt(curr)
        (rest, split :: res)
    }
    rest :: result
  }

  def getNumberOfTokens(ts: TokenStream): Int = {
    val oa = ts.addAttribute(classOf[PositionIncrementAttribute])
    ts.reset()
    var length = 0
    while (ts.incrementToken())
      length += oa.getPositionIncrement
    ts.end()
    ts.close()
    length
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
    length
  }

  val indexingCodec = new TermVectorFilteringLucene70Codec()
  
  class FieldPair[F1 <: Field,F2 <: Field](val indexField: F1, val storedField: F2, docs: Document*) {
    docs.foreach(add)
    def add(d: Document) {
      d.add(indexField)
      d.add(storedField)
    }
  }
  
  val normsOmittingNotStoredTextField = new FieldType(TextField.TYPE_NOT_STORED)
  normsOmittingNotStoredTextField.setOmitNorms(true)
  
  class TextSDVFieldPair(field: String, docs: Document*)  extends FieldPair(new Field(field, "", normsOmittingNotStoredTextField), new SortedDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
    }
  }
  
  class TextSSDVFieldPair(field: String, docs: Document*)  extends FieldPair(new Field(field, "", normsOmittingNotStoredTextField), new SortedSetDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
    }
  }

  class StringSSDVFieldPair(field: String, docs: Document*)  extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedSetDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
    }
  }

  class StringSDVFieldPair(field: String, docs: Document*) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
    }
  }

  class StringNDVFieldPair(field: String, docs: Document*) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new NumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) {
      indexField.setStringValue(""+v)
      storedField.setLongValue(v)
    }
  }
  
  class StringSNDVFieldPair(field: String, docs: Document*) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedNumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) {
      indexField.setStringValue(""+v)
      storedField.setLongValue(v)
    }
  }

  class FloatPointFDVFieldPair(field: String, docs: Document*) extends FieldPair(new FloatPoint(field, 0.0f), new FloatDocValuesField(field, 0.0f), docs:_*) {
    def setValue(v: Float) {
      indexField.setFloatValue(v)
      storedField.setFloatValue(v)
    }
  }

  
  class DoublePointDDVFieldPair(field: String, docs: Document*) extends FieldPair(new DoublePoint(field, 0.0), new DoubleDocValuesField(field, 0.0), docs:_*) {
    def setValue(v: Double) {
      indexField.setDoubleValue(v)
      storedField.setDoubleValue(v)
    }
  }
  
  class IntPointNDVFieldPair(field: String, docs: Document*) extends FieldPair(new IntPoint(field, 0), new NumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Int) {
      indexField.setIntValue(v)
      storedField.setLongValue(v)
    }
  }

  class IntPointSDVFieldPair(field: String, docs: Document*) extends FieldPair(new IntPoint(field, 0), new SortedDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: Int, sv: String) {
      indexField.setIntValue(v)
      storedField.setBytesValue(new BytesRef(sv))
    }
  }

  class IntPointSNDVFieldPair(field: String, docs: Document*) extends FieldPair(new IntPoint(field, 0), new SortedNumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Int) {
      indexField.setIntValue(v)
      storedField.setLongValue(v)
    }
  }

  class LongPointNDVFieldPair(field: String, docs: Document*) extends FieldPair(new LongPoint(field, 0l), new NumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) {
      indexField.setLongValue(v)
      storedField.setLongValue(v)
    }
  }

  class LongPointSDVDateTimeFieldPair(field: String, df: DateTimeFormatter, docs: Document*) extends FieldPair(new LongPoint(field, 0l), new SortedDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) {
      indexField.setLongValue(df.parseMillis(v))
      storedField.setBytesValue(new BytesRef(v))
    }
  }

  class LongPointSSDVDateTimeFieldPair(field: String, df: DateTimeFormatter, docs: Document*) extends FieldPair(new LongPoint(field, 0l), new SortedSetDocValuesField(field, new BytesRef()), docs:_*) {
    def setValue(v: String) {
      indexField.setLongValue(df.parseMillis(v))
      storedField.setBytesValue(new BytesRef(v))
    }
  }

  class LongPointSNDVFieldPair(field: String, docs: Document*) extends FieldPair(new LongPoint(field, 0l), new SortedNumericDocValuesField(field, 0), docs:_*) {
    def setValue(v: Long) {
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
    iwc.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy())
    iwc
  }
  
  def iw(path: String, sort: Sort, bufferSizeInMB: Double): IndexWriter = {
    logger.info("Creating IndexWriter "+path+" with a memory buffer of "+bufferSizeInMB+"MB")
    val d = new MMapDirectory(FileSystems.getDefault.getPath(path))
    d.listAll().foreach(d.deleteFile)
    new IndexWriter(d, iwc(sort, bufferSizeInMB))
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
    val size = getFileTreeSize(path)
    logger.info(f"Merging index at $path%s of $size%,d bytes.")
    var fiwc = iwc(sort, bufferSizeInMB)
    var miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault.getPath(path)), fiwc)
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
      miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault.getPath(path)), fiwc)
      miw.forceMerge(1)
      miw.commit()
      miw.close()
      miw.getDirectory.close()
    }
    logger.info(f"Merged index $path%s. Went from $size%,d bytes to ${getFileTreeSize(path)}%,d bytes.")
  }
  
  def toCodec(postingsFormatS: String, perFieldPostings: Seq[String]): Codec = {
    val finalCodec = new Lucene70PerFieldPostingsFormatOrdTermVectorsCodec()
    val postingsFormat = postingsFormatS match {
      case "fst" => new OrdExposingFSTOrdPostingsFormat()
      case "blocktree" => new BlockTreeOrdsPostingsFormat()
      case any => throw new IllegalArgumentException("Unknown postings format "+any) 
    }
    finalCodec.perFieldPostingsFormat = perFieldPostings.map((_, postingsFormat)).toMap
    finalCodec
  }  
  
  def close(iw: IndexWriter) {
    if (iw != null) {
      logger.info("Closing index " + iw.getDirectory)
      iw.close()
      iw.getDirectory.close()
      logger.info("Closed index " + iw.getDirectory)
    }
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
