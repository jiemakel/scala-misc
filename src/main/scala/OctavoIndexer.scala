import java.nio.file.FileSystems
import java.util.regex.Pattern

import fi.seco.lucene.{Lucene80PerFieldPostingsFormatOrdTermVectorsCodec, OrdExposingFSTOrdPostingsFormat}
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis._
import org.apache.lucene.analysis.core.WhitespaceTokenizer
import org.apache.lucene.analysis.miscellaneous.{ASCIIFoldingFilter, HyphenatedWordsFilter, LengthFilter}
import org.apache.lucene.analysis.pattern.PatternReplaceFilter
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute
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

  def wrapTokenStream(src: TokenStream): TokenStream = {
    var tok: TokenFilter = new HyphenatedWordsFilter(src)
    tok = new PatternReplaceFilter(tok,Pattern.compile("^\\p{Punct}*(.*?)\\p{Punct}*$"),"$1", false)
    tok = new LowerCaseFilter(tok)
    new LengthFilter(tok, 1, Int.MaxValue)
  }

  def createAnalyser(tokeniser: (String) => Tokenizer, filters: ((String, TokenStream) => TokenStream)*): Analyzer = new Analyzer() {
    override def createComponents(fieldName: String) = {
      val t = tokeniser(fieldName)
      new TokenStreamComponents(t,normalize(fieldName,t))
    }

    override def normalize(fieldName: String, src: TokenStream): TokenStream =
      filters.foldLeft(src)((in,f) => f(fieldName,in))
  }

  val analyzer = createAnalyser((_) => new WhitespaceTokenizer(), (_,ts) => wrapTokenStream(ts))

  def lsplit[A](str: List[A],pos: List[Int]): List[List[A]] = {
    val (rest, result) = pos.foldRight((str, List[List[A]]())) {
      case (curr, (s, res)) =>
        val (rest, split) = s.splitAt(curr)
        (rest, split :: res)
    }
    rest :: result
  }

  def getNumberOfTokens(ts: TokenStream): Int = {
    val oa = ts.getAttribute(classOf[PositionIncrementAttribute])
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
    val oa = ts.getAttribute(classOf[PositionIncrementAttribute])
    ts.reset()
    var length = 0
    while (ts.incrementToken())
      length += oa.getPositionIncrement
    ts.end()
    ts.close()
    length
  }

  val indexingCodec = new TermVectorFilteringLucene80Codec()
  
  class FieldPair[F1 <: Field,F2 <: Field](val indexField: F1, val storedField: F2) {
    def r(docs: FluidDocument*): this.type = {
      for (d<-docs) {
        d.addRequired(indexField)
        d.addRequired(storedField)
      }
      this
    }
    var odocs: Seq[FluidDocument] = Seq.empty
    def o(docs: FluidDocument*): this.type = {
      if (docs.nonEmpty) {
        if (odocs.nonEmpty) throw new IllegalArgumentException("Setting optional documents when already set!")
        odocs = docs
      }
      for (d<-odocs) {
        d.addOptional(indexField)
        d.addOptional(storedField)
      }
      this
    }
  }

  class ContentField(field: String, fanalyzer: Analyzer = analyzer) {
    val f = new Field(field, "", contentFieldType)
    val tokenStream = new ReusableCachedTokenStream(fanalyzer.tokenStream(field,""))
    f.setTokenStream(tokenStream)
    def numberOfTokens: Int = OctavoIndexer.this.getNumberOfTokens(tokenStream)
    def setValue(v: String, t: TokenStream): Unit = {
      f.setTokenStream(t)
      f.setStringValue(v)
      //tokenStream.fill(t)
    }
    def setValue(v: String) {
      tokenStream.fill(fanalyzer.tokenStream(field,v))
      f.setStringValue(v)
    }
    def r(docs: FluidDocument*): this.type = {
      for (d<-docs)
        d.addRequired(f)
      this
    }
    var odocs: Seq[FluidDocument] = Seq.empty
    def o(docs: FluidDocument*): this.type = {
      if (docs.nonEmpty) {
        if (odocs.nonEmpty) throw new IllegalArgumentException("Setting optional documents when already set!")
        odocs = docs
      }
      for (d<-odocs)
        d.addOptional(f)
      this
    }
  }

  class SDVContentFields(field: String, fanalyzer: Analyzer = analyzer) extends FieldPair(new Field(field,"",notStoredContentFieldType), new SortedDocValuesField(field, new BytesRef())) {
    val tokenStream = new ReusableCachedTokenStream(fanalyzer.tokenStream(field,""))
    indexField.setTokenStream(tokenStream)
    def numberOfTokens: Int = OctavoIndexer.this.getNumberOfTokens(tokenStream)
    def setValue(v: String, t: TokenStream): Unit = {
      indexField.setTokenStream(t)
      storedField.setStringValue(v)
      o()
    }
    def setValue(v: String): Unit = {
      storedField.setStringValue(v)
      tokenStream.fill(fanalyzer.tokenStream(field,v))
      o()
    }
  }

  val normsOmittingNotStoredTextField = new FieldType(TextField.TYPE_NOT_STORED)
  normsOmittingNotStoredTextField.setOmitNorms(true)
  
  class TextSDVFieldPair(field: String) extends FieldPair(new Field(field, "", normsOmittingNotStoredTextField), new SortedDocValuesField(field, new BytesRef())) {
    val tokenStream = new ReusableCachedTokenStream(analyzer.tokenStream(field,""))
    indexField.setTokenStream(tokenStream)
    def setValue(v: String, t: TokenStream): Unit = {
      indexField.setTokenStream(t)
      storedField.setStringValue(v)
      o()
    }
    def setValue(v: String): Unit = {
      tokenStream.fill(analyzer.tokenStream(field,v))
      indexField.setTokenStream(tokenStream)
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
  }

  class TextSDVTVFieldPair(field: String) extends FieldPair(new Field(field, "", notStoredTextFieldTypeWithTermVectors), new SortedDocValuesField(field, new BytesRef())) {
    val tokenStream = new ReusableCachedTokenStream(analyzer.tokenStream(field,""))
    indexField.setTokenStream(tokenStream)
    def setValue(v: String, t: TokenStream): Unit = {
      indexField.setTokenStream(t)
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
    def setValue(v: String): Unit = {
      tokenStream.fill(analyzer.tokenStream(field,v))
      indexField.setTokenStream(tokenStream)
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
  }
  
  class TextSSDVFieldPair(field: String) extends FieldPair(new Field(field, "", normsOmittingNotStoredTextField), new SortedSetDocValuesField(field, new BytesRef())) {
    val tokenStream = new ReusableCachedTokenStream(analyzer.tokenStream(field,""))
    indexField.setTokenStream(tokenStream)
    def setValue(v: String, t: TokenStream): Unit = {
      indexField.setTokenStream(t)
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
    def setValue(v: String): Unit = {
      tokenStream.fill(analyzer.tokenStream(field,v))
      indexField.setTokenStream(tokenStream)
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
  }

  class StringSSDVFieldPair(field: String) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedSetDocValuesField(field, new BytesRef())) {
    def setValue(v: String): Unit = {
      setValue(v,v)
    }
    def setValue(i: String, v: String): Unit = {
      indexField.setStringValue(i)
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
  }

  class StringSDVFieldPair(field: String) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedDocValuesField(field, new BytesRef())) {
    def setValue(v: String) {
      indexField.setStringValue(v)
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
  }

  class StringNDVFieldPair(field: String) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new NumericDocValuesField(field, 0)) {
    def setValue(v: Long) {
      indexField.setStringValue(""+v)
      storedField.setLongValue(v)
      o()
    }
  }
  
  class StringSNDVFieldPair(field: String) extends FieldPair(new Field(field, "", StringField.TYPE_NOT_STORED), new SortedNumericDocValuesField(field, 0)) {
    def setValue(v: Long) {
      indexField.setStringValue(""+v)
      storedField.setLongValue(v)
      o()
    }
  }

  class LatLonFieldPair(field: String) extends FieldPair(new LatLonPoint(field,0,0), new LatLonDocValuesField(field, 0, 0)) {
    def setValue(lat: Double, lon: Double) {
      indexField.setLocationValue(lat, lon)
      storedField.setLocationValue(lat, lon)
      o()
    }
  }

  class FloatPointFDVFieldPair(field: String) extends FieldPair(new FloatPoint(field, 0.0f), new FloatDocValuesField(field, 0.0f)) {
    def setValue(v: Float) {
      indexField.setFloatValue(v)
      storedField.setFloatValue(v)
      o()
    }
  }

  
  class DoublePointDDVFieldPair(field: String) extends FieldPair(new DoublePoint(field, 0.0), new DoubleDocValuesField(field, 0.0)) {
    def setValue(v: Double) {
      indexField.setDoubleValue(v)
      storedField.setDoubleValue(v)
      o()
    }
  }
  
  class IntPointNDVFieldPair(field: String) extends FieldPair(new IntPoint(field, 0), new NumericDocValuesField(field, 0)) {
    def setValue(v: Int) {
      indexField.setIntValue(v)
      storedField.setLongValue(v)
      o()
    }
  }

  class IntPointSDVFieldPair(field: String) extends FieldPair(new IntPoint(field, 0), new SortedDocValuesField(field, new BytesRef())) {
    def setValue(v: Int, sv: String) {
      indexField.setIntValue(v)
      storedField.setBytesValue(new BytesRef(sv))
      o()
    }
  }

  class IntPointSNDVFieldPair(field: String) extends FieldPair(new IntPoint(field, 0), new SortedNumericDocValuesField(field, 0)) {
    def setValue(v: Int) {
      indexField.setIntValue(v)
      storedField.setLongValue(v)
      o()
    }
  }

  class LongPointNDVFieldPair(field: String) extends FieldPair(new LongPoint(field, 0l), new NumericDocValuesField(field, 0)) {
    def setValue(v: Long) {
      indexField.setLongValue(v)
      storedField.setLongValue(v)
      o()
    }
  }

  class LongPointSDVDateTimeFieldPair(field: String, df: DateTimeFormatter) extends FieldPair(new LongPoint(field, 0l), new SortedDocValuesField(field, new BytesRef())) {
    def setValue(v: String) {
      indexField.setLongValue(df.parseMillis(v))
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
  }

  class LongPointSSDVDateTimeFieldPair(field: String, df: DateTimeFormatter) extends FieldPair(new LongPoint(field, 0l), new SortedSetDocValuesField(field, new BytesRef())) {
    def setValue(v: String) {
      indexField.setLongValue(df.parseMillis(v))
      storedField.setBytesValue(new BytesRef(v))
      o()
    }
  }

  class LongPointSNDVFieldPair(field: String) extends FieldPair(new LongPoint(field, 0l), new SortedNumericDocValuesField(field, 0)) {
    def setValue(v: Long) {
      indexField.setLongValue(v)
      storedField.setLongValue(v)
      o()
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
  
  def iw(path: String, sort: Sort, bufferSizeInMB: Double, clear: Boolean = true): IndexWriter = {
    logger.info("Creating IndexWriter "+path+" with a memory buffer of "+bufferSizeInMB+"MB")
    val d = new MMapDirectory(FileSystems.getDefault.getPath(path))
    if (clear) d.listAll().foreach(d.deleteFile)
    new IndexWriter(d, iwc(sort, bufferSizeInMB))
  }

  val contentFieldType = new FieldType(TextField.TYPE_STORED)
  contentFieldType.setOmitNorms(true)
  contentFieldType.setStoreTermVectors(true)
  contentFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)

  val notStoredContentFieldType = new FieldType(TextField.TYPE_NOT_STORED)
  notStoredContentFieldType.setOmitNorms(true)
  notStoredContentFieldType.setStoreTermVectors(true)
  notStoredContentFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)

  val notStoredTextFieldTypeWithTermVectors = new FieldType(TextField.TYPE_NOT_STORED)
  notStoredTextFieldTypeWithTermVectors.setOmitNorms(true)
  notStoredTextFieldTypeWithTermVectors.setStoreTermVectors(true)

  val normsOmittingStoredTextField = new FieldType(TextField.TYPE_STORED)
  normsOmittingStoredTextField.setOmitNorms(true)
  
  val notStoredStringFieldWithTermVectors = new FieldType(StringField.TYPE_NOT_STORED)
  notStoredStringFieldWithTermVectors.setOmitNorms(true)
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
    val finalCodec = new Lucene80PerFieldPostingsFormatOrdTermVectorsCodec()
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
