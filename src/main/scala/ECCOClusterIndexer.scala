import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import com.sleepycat.je._
import org.apache.lucene.document.{Document, Field}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.json4s.native.JsonParser._
import org.rogach.scallop._

import scala.collection.mutable.ArrayBuffer
import scala.language.{postfixOps, reflectiveCalls}

object ECCOClusterIndexer extends OctavoIndexer {
  
  class Reuse {
    val keya = new Array[Byte](java.lang.Long.BYTES+java.lang.Integer.BYTES*2)
    val kbb = ByteBuffer.wrap(keya)
    val dkey = new DatabaseEntry(keya)
    val dval = new DatabaseEntry
    val btckeya = new Array[Byte](java.lang.Long.BYTES)
    val btckbb = ByteBuffer.wrap(btckeya)
    val dbtckey = new DatabaseEntry(btckeya)
    val btcvala = new Array[Byte](java.lang.Integer.BYTES*3)
    val btcvbb = ByteBuffer.wrap(btcvala)
    val dbtcval = new DatabaseEntry(btcvala)
    val d = new Document()
    val clusterIDFields = new StringNDVFieldPair("clusterID", d)
    val avgLengthFields = new IntPointNDVFieldPair("avgLength", d)
    val countFields = new IntPointNDVFieldPair("count", d)
    val documentIDFields = new StringSDVFieldPair("documentID", d)
    val textField = new Field("text", "", contentFieldType)
    d.add(textField)
    val lengthFields = new IntPointNDVFieldPair("length", d)
    val tokensFields = new IntPointNDVFieldPair("tokens", d)
    val startIndexFields = new IntPointNDVFieldPair("startIndex", d)
    val endIndexFields = new IntPointNDVFieldPair("endIndex", d)
  }
  
  val termVectorFields = Seq("text")
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  private def index(file: File): Unit = parse(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))), (p: Parser) => {
    logger.info("Processing "+file+".")
    val path = file.getParentFile.getName
    var token = p.nextToken
    while (token != End) {
      token match {
        case FieldStart(field) if field.startsWith("cluster_") =>
          val cluster = new Cluster
          cluster.id = (if (field.startsWith("cluster_e_")) "1"+field.substring(10) else "2"+field.substring(8)).toInt
          while (token != CloseObj) {
            token match {
              case FieldStart("length") | FieldStart("Avglength") => cluster.avgLength = p.nextToken.asInstanceOf[IntVal].value.toInt
              case FieldStart("hits") | FieldStart("Hits") =>
                var cm: Match = null
                while (token != CloseArr) {
                  token match {
                    case OpenObj => cm = new Match()
                    case FieldStart("end_index") => cm.endIndex = p.nextToken.asInstanceOf[IntVal].value.toInt
                    case FieldStart("start_index") => cm.startIndex = p.nextToken.asInstanceOf[IntVal].value.toInt
                    case FieldStart("book_id") => cm.documentID = p.nextToken.asInstanceOf[StringVal].value
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
          val r = tld.get

          cluster.matches.foreach(cm => {
            var ldid = (if (cm.documentID.head == 'A') "10"+cm.documentID.tail
            else if (cm.documentID.head == 'B') "11"+cm.documentID.tail else "2" + cm.documentID).toLong // "0").toLong
            /* if (ldid == 0) {
              ldid = ("2" + cm.documentID).toLong
              r.kbb.putLong(0, cm.documentID.toLong)
              r.kbb.putInt(java.lang.Long.BYTES, cluster.id)
              r.kbb.putInt(java.lang.Long.BYTES + java.lang.Integer.BYTES, cm.startIndex)
              if (db.get(null, r.dkey, r.dval, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                val vbb = ByteBuffer.wrap(r.dval.getData)
                cm.startIndex = vbb.getInt
                cm.endIndex = vbb.getInt
              } else logger.warn("Did not find cluster mapping info for cluster " + cm.documentID + "/" + cm.startIndex + "/" + cm.endIndex)
            } */
            r.btckbb.putLong(0, ldid)
            r.btcvbb.putInt(0, cm.startIndex)
            r.btcvbb.putInt(java.lang.Integer.BYTES, cm.endIndex)
            r.btcvbb.putInt(2*java.lang.Integer.BYTES, cluster.id)
            bookToClusterDb.put(null, r.dbtckey, r.dbtcval)
          })
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
      d.textField.setStringValue(m.text)
      d.lengthFields.setValue(m.text.length)
      d.tokensFields.setValue(getNumberOfTokens(m.text.toString))
      d.startIndexFields.setValue(m.startIndex)
      d.endIndexFields.setValue(m.endIndex)
      diw.addDocument(d.d)
    }
  }
  
  var diw = null.asInstanceOf[IndexWriter]
  
  class Match {
    var documentID: String = _
    var startIndex: Int = -1
    var endIndex: Int = -1
    var text: String = _
  }
  
  class Cluster {
    var id: Int = -1
    var avgLength: Int = -1
    var matches: ArrayBuffer[Match] = new ArrayBuffer()
  }
  
  val cs = new Sort(new SortField("clusterID",SortField.Type.INT))

  var bookToClusterDb: Database = _
  var db: Database = _

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val postings = opt[String](default = Some("blocktree"))
      val mappingsDb = opt[String](required = true)
      val bookToClusterDb = opt[String](required = true)
      verify()
    }
    val envDir = new File(opts.mappingsDb())
    envDir.mkdirs()
    val env = new Environment(envDir,new EnvironmentConfig().setAllowCreate(false).setTransactional(false).setSharedCache(true))
    env.setMutableConfig(env.getMutableConfig.setCacheSize(opts.indexMemoryMb()*1024*1024/2))
    db = env.openDatabase(null, "sampleDatabase", new DatabaseConfig().setAllowCreate(false).setTransactional(false))
    val btcenvDir = new File(opts.bookToClusterDb())
    btcenvDir.mkdirs()
    for (file <- btcenvDir.listFiles) file.delete()
    val btcenv = new Environment(btcenvDir,new EnvironmentConfig().setAllowCreate(true).setTransactional(false).setSharedCache(true).setConfigParam(EnvironmentConfig.LOG_FILE_MAX,"1073741824"))
    btcenv.setMutableConfig(btcenv.getMutableConfig.setCacheSize(opts.indexMemoryMb()*1024*1024/2))
    bookToClusterDb = btcenv.openDatabase(null, "bookToCluster", new DatabaseConfig().setAllowCreate(true).setDeferredWrite(true).setTransactional(false).setSortedDuplicates(true))
    diw = iw(opts.index()+"/dindex", cs, opts.indexMemoryMb()/2)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".gz")).foreach(file => addTask(file.getPath, () => index(file)))
     )
    bookToClusterDb.close()
    btcenv.close()
    db.close()
    env.close()
    close(diw)
    merge(opts.index()+"/dindex", cs, opts.indexMemoryMb(), toCodec(opts.postings(), termVectorFields))
  }
}
