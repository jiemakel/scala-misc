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

import scala.collection.JavaConversions._
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

object ECCOIndexer {
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().sorted.toStream.flatMap(getFileTree)
      else Stream.empty)
      
  def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvText(text) => content.append(text)
      case er: EvEntityRef =>
        content.append('&')
        content.append(er.entity)
        content.append(';')
      case EvComment(_) => 
      case EvElemEnd(_,_) => break = true 
    }
    return content.toString.trim
  }
  
  val fileRegex = ".*_(.*)\\.txt".r
  
  val headingRegex = "^# ".r

  val analyzer = new StandardAnalyzer()
 
/*  val bdbec = new EnvironmentConfig().setAllowCreate(true)
  val bdbe = new Environment(FileSystems.getDefault().getPath("/srv/ecco/bdb").toFile,bdbec)
  bdbe.removeDatabase(null, "bdb")
  val bdbc = new DatabaseConfig().setAllowCreate(true).setExclusiveCreate(true).setDeferredWrite(true)
  val bdb = bdbe.openDatabase(null, "bdb", bdbc) */
  
  // document level, used for basic search, collocation
  val diw = new IndexWriter(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/dindex")), new IndexWriterConfig(analyzer))
  // heading level, search inside subpart
  val hiw = new IndexWriter(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/hindex")), new IndexWriterConfig(analyzer)) 
  // paragraph level, used only for collocation
  val piw = new IndexWriter(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/pindex")), new IndexWriterConfig(analyzer))

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

  val dcontentFieldType = new FieldType(TextField.TYPE_NOT_STORED)
  dcontentFieldType.setOmitNorms(true)
  val dstoredContentFieldType = new FieldType(dcontentFieldType)
  dstoredContentFieldType.setStored(true)

  dcontentFieldType.setStoreTermVectors(true)


  val phcontentFieldType = new FieldType(dcontentFieldType)
  phcontentFieldType.setIndexOptions(IndexOptions.DOCS)
  
  def main(args: Array[String]): Unit = {
    diw.deleteAll()
    diw.commit()
    hiw.deleteAll()
    hiw.commit()
    piw.deleteAll()
    piw.commit()
    /*
     *
   6919 article
  51226 backmatter
  34276 book
1377880 chapter
 147743 frontmatter
  13386 index
  25772 other
  38055 part
 290959 section
   3957 titlePage
  32836 TOC
   1481 volume
     */
    for (dir<-args.toSeq;file <- getFileTree(new File(dir))) if (file.getName.endsWith("_metadata.xml")) {
      println("Processing "+file.getParent)
      val xmls = Source.fromFile(file)
      implicit val xml = new XMLEventReader(xmls)
      val md = new Document()
      var documentId: Field = null
      var estcId: Field = null
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"documentID",_,_) => 
          documentId = new Field("documentID",readContents,StoredField.TYPE)
          md.add(documentId)
        case EvElemStart(_,"ESTCID",_,_) =>
          estcId = new Field("ESTCID",readContents,StoredField.TYPE)
          md.add(estcId)
        case EvElemStart(_,"pubDate",_,_) => readContents match {
          case "" =>
          case "1809" => 
            md.add(new IntPoint("pubDate",18090101))
            md.add(new StoredField("pubDate",18090101))
          case any => 
            md.add(new IntPoint("pubDate",any.toInt))
            md.add(new StoredField("pubDate",any.toInt))
        }
        case EvElemStart(_,"language",_,_) => md.add(new Field("language",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"module",_,_) => md.add(new Field("module",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"notes",_,_) => md.add(new Field("notes",readContents,dstoredContentFieldType))
        case EvElemStart(_,"fullTitle",_,_) => md.add(new Field("fullTitle",readContents,dstoredContentFieldType))
        case _ => 
      }
      xmls.close()
      for (file <- getFileTree(file.getParentFile)) if (file.getName.endsWith(".txt") && !file.getName.contains("_page")) {
        val dcontents = new StringBuilder
        val d = new Document()
        for (f <- md) d.add(f)
        var hds = Seq(1,2,3).map(w => None.asInstanceOf[Option[Document]]).toBuffer
        val field = fileRegex.findFirstMatchIn(file.getName).get.group(1)
        val hcontents = hds.map(w => new StringBuilder)
        val pcontents = new StringBuilder
        val fl = Source.fromFile(file)
        for (line <- fl.getLines) {
          if (line.isEmpty) {
            if (!pcontents.isEmpty) {
              val d2 = new Document()
              d2.add(new Field("content",pcontents.toString,phcontentFieldType))
              d2.add(new Field("type", field, StringField.TYPE_STORED))
              pcontents.clear()
              piw.addDocument(d2)
            }
          } else
            pcontents.append(line)
          if (line.startsWith("# ") || line.startsWith("## ") || line.startsWith("### ")) {
            val level =
              if (line.startsWith("### ")) 2
              else if (line.startsWith("## ")) 1
              else 0
            for (
                i <- level until hds.length;
                contents = hcontents(i)
            ) {
              hds(i).foreach { d2 => 
                if (!contents.isEmpty) {
                  d2.add(new Field("content",contents.toString,phcontentFieldType))
                  contents.clear()
                }
                d2.add(new Field("type", field, StringField.TYPE_STORED))
                hiw.addDocument(d2)
              }
              hds(i) = None
            }
            val d2 = new Document()
            hds(level) = Some(d2)
            val f = new Field("heading",line.substring(level+1),phcontentFieldType)
            d2.add(f)
            for (i <- 0 until level) hcontents(i).append(line)
          } else
            for (i <- 0 until hds.length) hds(i).foreach(d=> hcontents(i).append(line))
          dcontents.append(line)
        }
        fl.close()
        for (
            i <- 0 until hds.length;
            contents = hcontents(i)
        )
          hds(i).foreach { d2 => 
            if (!contents.isEmpty)
              d2.add(new Field("content",contents.toString,phcontentFieldType))
            d2.add(new Field("type", field, StringField.TYPE_STORED))
            hiw.addDocument(d2)
          }
        d.add(new Field("type", field, StringField.TYPE_STORED))
        d.add(new Field("content", dcontents.toString, dcontentFieldType))
        val not = getNumberOfTokens(dcontents.toString)
        d.add(new IntPoint("contentTokens", not))
        d.add(new StoredField("contentTokens", not))
        diw.addDocument(d)
      }
    }
    diw.forceMerge(1)
    diw.commit()
    diw.close()
    diw.getDirectory.close()
    hiw.forceMerge(1)
    hiw.commit()
    hiw.close()
    hiw.getDirectory.close()
    piw.forceMerge(1)
    piw.commit()
    piw.close()
    piw.getDirectory.close()
  }
}
