import java.io.{File, FileInputStream, InputStreamReader}
import java.util.zip.GZIPInputStream

import org.apache.lucene.document.{Document, Field}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.json4s.native.JsonParser._
import org.rogach.scallop._

import scala.collection.mutable.ArrayBuffer
import scala.language.{postfixOps, reflectiveCalls}
import scala.util.Try

object ECCOClusterIndexer extends OctavoIndexer {
  
  class Reuse {
    val d = new Document()
    val clusterIDFields = new StringNDVFieldPair("clusterID", d)
    val avgLengthFields = new IntPointNDVFieldPair("avgLength", d)
    val countFields = new IntPointNDVFieldPair("count", d)
    val documentIDFields = new StringSDVFieldPair("documentID", d)
    val titleFields = new TextSDVFieldPair("title", d)
    val textField = new Field("text", "", contentFieldType)
    d.add(textField)
    val lengthFields = new IntPointNDVFieldPair("length", d)
    val tokensFields = new IntPointNDVFieldPair("tokens", d)
    val authorFields = new TextSDVFieldPair("author", d)
    val startIndexFields = new IntPointNDVFieldPair("startIndex", d)
    val endIndexFields = new IntPointNDVFieldPair("endIndex", d)
    val yearFields = new IntPointNDVFieldPair("year", d)
  }
  
  val termVectorFields = Seq("text")
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  private def index(file: File): Unit = parse(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))), (p: Parser) => {
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
          index(cluster)
        case _ =>
      }
      token = p.nextToken
    }
    logger.info("File "+file+" processed.")
  })
  
  private def index(cluster: Cluster): Unit = {
    val d = tld.get
    d.clusterIDFields.setValue(cluster.id)
    d.avgLengthFields.setValue(cluster.avgLength)
    d.countFields.setValue(cluster.matches.size)
    for (m <- cluster.matches) {
      d.documentIDFields.setValue(m.documentID)
      d.titleFields.setValue(m.title)
      d.textField.setStringValue(m.text)
      d.lengthFields.setValue(m.text.length)
      d.tokensFields.setValue(getNumberOfTokens(m.text.toString))
      d.authorFields.setValue(m.author)
      d.startIndexFields.setValue(m.startIndex)
      d.endIndexFields.setValue(m.endIndex)
      d.yearFields.setValue(m.year)
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
  
  val cs = new Sort(new SortField("clusterID",SortField.Type.INT))
  
  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val postings = opt[String](default = Some("blocktree"))
      verify()
    }
    diw = iw(opts.index()+"/dindex", cs, opts.indexMemoryMb())
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".gz")).foreach(file => addTask(file.getPath, () => index(file)))
     )
    close(diw)
    merge(opts.index()+"/dindex", cs, opts.indexMemoryMb(), toCodec(opts.postings(), termVectorFields))
  }
}
