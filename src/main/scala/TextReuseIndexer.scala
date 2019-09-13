import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

import com.sleepycat.je._
import org.apache.lucene.document.Field
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.apache.lucene.store.ByteArrayDataOutput
import org.json4s.native.JsonParser._
import org.rogach.scallop._

import scala.collection.mutable.ArrayBuffer
import scala.language.{postfixOps, reflectiveCalls}

object TextReuseIndexer extends OctavoIndexer {
  
  class Reuse {
    val keya = new Array[Byte](java.lang.Long.BYTES+java.lang.Integer.BYTES*2)
    val kbb = ByteBuffer.wrap(keya)
    val dkey = new DatabaseEntry(keya)
    val dval = new DatabaseEntry
    val btckeya = new Array[Byte](java.lang.Long.BYTES)
    val btckbb = ByteBuffer.wrap(btckeya)
    val dbtckey = new DatabaseEntry(btckeya)
    var btcvala = new Array[Byte](0)
    val btcvaldo = new ByteArrayDataOutput(btcvala)
    val dbtcval = new DatabaseEntry(null)
    val d = new FluidDocument()
    val fragmentIDFields = new StringSDVFieldPair("fragmentID").r(d)
    val avgLengthFields = new IntPointNDVFieldPair("avgLength").r(d)
    val countFields = new IntPointNDVFieldPair("count").r(d)
    val documentIDFields = new StringSDVFieldPair("documentID").r(d)
    val textField = new Field("text", "", contentFieldType)
    d.addRequired(textField)
    val lengthFields = new IntPointNDVFieldPair("length").r(d)
    val tokensFields = new IntPointNDVFieldPair("tokens").r(d)
    val startIndexFields = new IntPointNDVFieldPair("startIndex").r(d)
    val endIndexFields = new IntPointNDVFieldPair("endIndex").r(d)
  }
  
  val termVectorFields = Seq("text")
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  private def index(file: File): Unit = parse(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))), (p: Parser) => {
    logger.info("Processing "+file+".")
    var token = p.nextToken
    val fragments = new collection.mutable.HashMap[String,Fragment]
    var indexOffset = 0
    var startIndex = 0
    var endIndex = 0
    while (token != End) {
      token match {
        case FieldStart(field) if field.startsWith("cluster_") =>
          fragments.clear()
          while (token != CloseObj) {
            token match {
              case FieldStart("hits") | FieldStart("Hits") =>
                var cm: Match = null
                while (token != CloseArr) {
                  token match {
                    case OpenObj => cm = new Match()
                    case FieldStart("doc_id") =>
                      var path = p.nextToken.asInstanceOf[StringVal].value
                      path = path.drop(path.lastIndexOf('/')+1)
                      val split = path.indexOf(".txt__")
                      cm.documentID = path.take(split).replaceAllLiterally(".headed","")
                      path = path.drop(split+6)
                      indexOffset = path.take(path.indexOf('_')).toInt
                    case FieldStart("original_indices") =>
                      p.nextToken
                      startIndex = p.nextToken.asInstanceOf[IntVal].value.toInt
                      endIndex = p.nextToken.asInstanceOf[IntVal].value.toInt
                      p.nextToken
                    case FieldStart("encoded_indices") =>
                      p.nextToken
                      p.nextToken
                      p.nextToken
                      p.nextToken
                    case FieldStart("text") => cm.text = p.nextToken.asInstanceOf[StringVal].value
                    case CloseObj =>
                      val fragmentId = field.substring(8)
                      cm.startIndex = indexOffset+startIndex
                      cm.endIndex = indexOffset+endIndex
                      fragments.getOrElseUpdate(fragmentId, new Fragment(fragmentId)).matches += cm
                    case _ => 
                  }
                  token = p.nextToken
                }
              case _ =>
            }
            token = p.nextToken
          }
          val r = tld.get
          for (fragment <- fragments.values) {
            fragment.matches.foreach(cm => {
              var did = cm.documentID
              val indexOffset = if (did.contains("_at_")) did.drop(did.indexOf("_at_")+4).takeWhile(_ != '_').toInt else 0
              did = did.takeWhile(_ != '_')
              val ldid = (if (did.head == 'A') "10" + did.tail // EEBO1,EEBO2,ECCO
              else if (did.head == 'B') "11" + did.tail else "2" + did).toLong
              r.btckbb.putLong(0, ldid)
              val fragmentIdBytes = fragment.id.getBytes("UTF-8")
              val alen = fragmentIdBytes.length + 2 * java.lang.Integer.BYTES
              if (r.btcvala.length < alen) {
                r.btcvala = new Array[Byte](alen)
                r.dbtcval.setData(r.btcvala)
              }
              r.btcvaldo.reset(r.btcvala)
              r.btcvaldo.writeInt(indexOffset + cm.startIndex)
              r.btcvaldo.writeInt(indexOffset + cm.endIndex)
              r.btcvaldo.writeBytes(fragmentIdBytes,fragmentIdBytes.length)
              r.dbtcval.setSize(alen)
              bookToFragmentDb.put(null, r.dbtckey, r.dbtcval)
            })
            index(fragment)
          }
        case _ =>
      }
      token = p.nextToken
    }
    logger.info("File "+file+" processed.")
  })
  
  private def index(fragment: Fragment): Unit = {
    val d = tld.get
    d.fragmentIDFields.setValue(fragment.id)
    d.avgLengthFields.setValue(fragment.matches.foldLeft(0)((l,r) => l+r.text.length)/fragment.matches.length)
    d.countFields.setValue(fragment.matches.size)
    for (m <- fragment.matches) {
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
  
  class Fragment(val id: String) {
    var matches: ArrayBuffer[Match] = new ArrayBuffer()
  }
  
  val cs = new Sort(new SortField("fragmentID",SortField.Type.STRING))

  var bookToFragmentDb: Database = _

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val postings = opt[String](default = Some("blocktree"))
      val bookToFragmentDb = opt[String](required = true)
      verify()
    }
    val btcenvDir = new File(opts.bookToFragmentDb())
    btcenvDir.mkdirs()
    for (file <- btcenvDir.listFiles) file.delete()
    val btcenv = new Environment(btcenvDir,new EnvironmentConfig().setAllowCreate(true).setTransactional(false).setSharedCache(true).setConfigParam(EnvironmentConfig.LOG_FILE_MAX,"1073741824"))
    btcenv.setMutableConfig(btcenv.getMutableConfig.setCacheSize(opts.indexMemoryMb()*1024*1024/2))
    bookToFragmentDb = btcenv.openDatabase(null, "bookToFragment", new DatabaseConfig().setAllowCreate(true).setDeferredWrite(true).setTransactional(false).setSortedDuplicates(true))
    diw = iw(opts.index()+"/dindex", cs, opts.indexMemoryMb()/2)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".gz")).foreach(file => addTask(file.getPath, () => index(file)))
     )
    bookToFragmentDb.close()
    btcenv.close()
    close(diw)
    merge(opts.index()+"/dindex", cs, opts.indexMemoryMb(), toCodec(opts.postings(), termVectorFields))
  }
}
