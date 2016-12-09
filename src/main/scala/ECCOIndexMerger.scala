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
import org.apache.lucene.index.MergePolicy.OneMerge
import java.util.Collections
import org.apache.lucene.index.SegmentInfos
import org.apache.lucene.index.IndexUpgrader
import org.apache.lucene.index.UpgradeIndexMergePolicy
import fi.seco.lucene.FSTOrdTermVectorsCodec
import org.apache.lucene.index.SegmentCommitInfo

object ECCOIndexMerger {
  val analyzer = new StandardAnalyzer()
  
  val codec = new FSTOrdTermVectorsCodec()
  
 def merge(path: String): Unit = {
    println("Merging "+path)
    val iwc = new IndexWriterConfig(analyzer)
    iwc.setCodec(codec)
    iwc.setMergePolicy(new UpgradeIndexMergePolicy(iwc.getMergePolicy()) {
      override protected def shouldUpgradeSegment(si: SegmentCommitInfo): Boolean =  !si.info.getCodec.equals(codec)
    })
    iwc.setUseCompoundFile(false)
    val miw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath(path)), iwc)
    miw.forceMerge(1)
    miw.commit()
    miw.close()
    miw.getDirectory.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.length!=0) args.foreach(merge(_)) else {
      merge("/srv/ecco/dindex")
      merge("/srv/ecco/dpindex")
      merge("/srv/ecco/sindex")
      merge("/srv/ecco/pindex")      
    }
  }
}
