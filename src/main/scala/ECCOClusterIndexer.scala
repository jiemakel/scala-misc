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

import org.rogach.scallop._
import scala.language.postfixOps
import java.lang.ThreadLocal

object ECCOClusterIndexer extends OctavoIndexer {
  
  class Reuse {
    val d = new Document()
    val clusterIDSField = new Field("clusterID","",StringField.TYPE_NOT_STORED)
    d.add(clusterIDSField)
    val clusterIDNField = new NumericDocValuesField("clusterID", 0)
    d.add(clusterIDNField)
    val avgLengthIField = new IntPoint("avgLength", 0)
    d.add(avgLengthIField)
    val avgLengthNField = new NumericDocValuesField("avgLength", 0)
    d.add(avgLengthNField)
    val countIField = new IntPoint("count", 0)
    d.add(countIField)
    val countNField = new NumericDocValuesField("count", 0)
    d.add(countNField)
    val documentIDSField = new Field("documentID","",StringField.TYPE_NOT_STORED)
    d.add(documentIDSField)
    val documentIDSDVField = new SortedDocValuesField("documentID", new BytesRef)
    d.add(documentIDSDVField)
    val titleField = new Field("title", "", contentFieldType)
    d.add(titleField)
    val textField = new Field("text", "", contentFieldType)
    d.add(textField)
    val authorField = new Field("author", "", normsOmittingStoredTextField)
    d.add(authorField)
    val startIndexIField = new IntPoint("startIndex", 0)
    d.add(startIndexIField)
    val startIndexNField = new NumericDocValuesField("startIndex", 0)
    d.add(startIndexNField)
    val endIndexIField = new IntPoint("endIndex", 0)
    d.add(endIndexIField)
    val endIndexNField = new NumericDocValuesField("endIndex", 0)
    d.add(endIndexNField)
    val yearIField = new IntPoint("year", 0)
    d.add(yearIField)
    val yearNField = new NumericDocValuesField("year", 0)
    d.add(yearNField)
  }
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  private def index(cluster: Cluster): Unit = {
    val d = tld.get
    for (m <- cluster.matches) {
      d.clusterIDSField.setStringValue("" + cluster.id)
      d.clusterIDNField.setLongValue(cluster.id)
      d.avgLengthIField.setIntValue(cluster.avgLength)
      d.avgLengthNField.setLongValue(cluster.avgLength)
      d.countIField.setIntValue(cluster.matches.size)
      d.countNField.setLongValue(cluster.matches.size)
      d.documentIDSField.setStringValue("" + m.documentID)
      d.documentIDSDVField.setBytesValue(new BytesRef(m.documentID))
      d.titleField.setStringValue(m.title)
      d.textField.setStringValue(m.text)
      d.authorField.setStringValue(m.author)
      d.startIndexIField.setIntValue(m.startIndex)
      d.startIndexNField.setLongValue(m.startIndex)
      d.endIndexIField.setIntValue(m.endIndex)
      d.endIndexNField.setLongValue(m.endIndex)
      d.yearIField.setIntValue(m.year)
      d.yearNField.setLongValue(m.year)
      diw.addDocument(d.d)
    }
  }
  
  var diw = null.asInstanceOf[IndexWriter]
  
  class Match {
    var documentID: String = null
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
  
  class Opts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val index = opt[String](required = true)
    val indexMemoryMB = opt[Long](default = Some(availableMemory/1024/1024*3/4), validate = (0<))
    val directories = trailArg[List[String]]()
    verify()
  }
  
  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    diw = iw(opts.index()+"/dindex", new Sort(new SortField("clusterID",SortField.Type.INT)), opts.indexMemoryMB())
    clear(diw)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toArray.flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".gz")).forEach(file => {
        Try(parse(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))), (p: Parser) => {
          val path = file.getParentFile.getName
          var token = p.nextToken
          while (token != End) {
            token match {
              case FieldStart(field) if (field.startsWith("cluster_")) =>
                val cluster = new Cluster
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
                          case FieldStart("book_id") => cm.documentID = p.nextToken.asInstanceOf[StringVal].value
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
                addTask(file.getName+": "+cluster.id,() => index(cluster))
              case _ =>
            }
            token = p.nextToken
          }
        })) match {
          case Success(_) => logger.info("File "+file+" processed.")
          case Failure(f) => logger.error("An error has occurred in reading file "+file+".",f)
        }
      }))
    close(diw)
    mergeIndices(Seq(
     (opts.index()+"/dindex", new Sort(new SortField("clusterID",SortField.Type.INT)), opts.indexMemoryMB()))
    )
  }
}
