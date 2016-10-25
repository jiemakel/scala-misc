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
import scala.collection.mutable.ArrayBuffer

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
   
  // document level, used for basic search, collocation
  val diw = new IndexWriter(FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/dindex")), new IndexWriterConfig(analyzer))
  // heading level, search inside subpart, collocation
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

  val dhpcontentFieldType = new FieldType(TextField.TYPE_NOT_STORED)
  dhpcontentFieldType.setOmitNorms(true)
  val dhstoredContentFieldType = new FieldType(dhpcontentFieldType)
  dhstoredContentFieldType.setStored(true)

  dhpcontentFieldType.setStoreTermVectors(true)
  
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
      val dmd = new Document()
      val hmd = new Document()
      val pmd = new Document()
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"documentID",_,_) =>
          val c = readContents
          val f = new Field("documentID",c,StringField.TYPE_STORED)
          dmd.add(f)
          hmd.add(f)
        case EvElemStart(_,"ESTCID",_,_) =>
          val c = readContents
          val f = new Field("ESTCID",c,StringField.TYPE_STORED)
          dmd.add(f)
          hmd.add(f)
        case EvElemStart(_,"pubDate",_,_) => readContents match {
          case "" =>
          case "1809" =>
            val ip = new IntPoint("pubDate",18090101)
            dmd.add(ip)
            pmd.add(ip)
            hmd.add(ip)
            val f = new StoredField("pubDate",18090101)
            hmd.add(f)
            dmd.add(f)
          case any => 
            val ip = new IntPoint("pubDate",any.toInt) 
            dmd.add(ip)
            pmd.add(ip)
            hmd.add(ip)
            val f = new StoredField("pubDate",any.toInt)
            dmd.add(f)
            hmd.add(f)
        }
        case EvElemStart(_,"totalPages",_,_) =>
          val tp = readContents
          if (!tp.isEmpty) {
            val tpi = tp.toInt
            val ip = new IntPoint("totalPages",tpi) 
            dmd.add(ip)
            pmd.add(ip)
            hmd.add(ip)
            val f = new StoredField("totalPages",tpi)
            dmd.add(f)
            hmd.add(f)
          }
        case EvElemStart(_,"language",_,_) =>
          val c = readContents
          val f = new Field("language",c,StringField.TYPE_STORED)
          dmd.add(f)
          hmd.add(f)
          pmd.add(new Field("language",c,StringField.TYPE_NOT_STORED))
        case EvElemStart(_,"module",_,_) =>
          val c = readContents
          val f = new Field("module",c,StringField.TYPE_STORED)
          dmd.add(f)
          hmd.add(f)
          pmd.add(new Field("module",c,StringField.TYPE_NOT_STORED))
        case EvElemStart(_,"fullTitle",_,_) => 
          val f = new Field("fullTitle",readContents,dhstoredContentFieldType)
          dmd.add(f)
          hmd.add(f)
        case _ => 
      }
      xmls.close()
      var tlength = 0
      val pdocs = new ArrayBuffer[Document]
      val hdocs = new ArrayBuffer[Document]
      val ddocs = new ArrayBuffer[Document]
      for (file <- getFileTree(file.getParentFile)) if (file.getName.endsWith(".txt") && !file.getName.contains("_page")) {
        val dcontents = new StringBuilder
        val d = new Document()
        for (f <- dmd) d.add(f)
        var hds = Seq(1,2,3).map(w => None.asInstanceOf[Option[Document]]).toBuffer
        val field = fileRegex.findFirstMatchIn(file.getName).get.group(1)
        val hcontents = hds.map(w => new StringBuilder)
        val pcontents = new StringBuilder
        val fl = Source.fromFile(file)
        for (line <- fl.getLines) {
          if (line.isEmpty) {
            if (!pcontents.isEmpty) {
              val d2 = new Document()
              for (f <- pmd) d2.add(f)
              d2.add(new Field("content",pcontents.toString,dhpcontentFieldType))
              d2.add(new IntPoint("contentTokens",getNumberOfTokens(pcontents.toString)))
              d2.add(new Field("type", field, StringField.TYPE_NOT_STORED))
              pcontents.clear()
              pdocs += d2
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
                  d2.add(new Field("content",contents.toString,dhpcontentFieldType))
                  d2.add(new IntPoint("contentTokens",getNumberOfTokens(contents.toString)))
                  contents.clear()
                }
                d2.add(new Field("type", field, StringField.TYPE_STORED))
                hdocs += d2
              }
              hds(i) = None
            }
            val d2 = new Document()
            for (f <- hmd) d2.add(f)
            hds(level) = Some(d2)
            val f = new Field("heading",line.substring(level+2),dhstoredContentFieldType)
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
            if (!contents.isEmpty) {
              d2.add(new Field("content",contents.toString,dhpcontentFieldType))
              d2.add(new IntPoint("contentTokens",getNumberOfTokens(contents.toString)))
            }
            d2.add(new Field("type", field, StringField.TYPE_STORED))
            hdocs += d2
          }
        d.add(new Field("type", field, StringField.TYPE_STORED))
        d.add(new Field("content", dcontents.toString, dhpcontentFieldType))
        val not = getNumberOfTokens(dcontents.toString)
        tlength += dcontents.length
        d.add(new IntPoint("contentTokens", not))
        ddocs.add(d)
      }
      val f1 = new IntPoint("length", tlength)
      val f2 = new StoredField("length", tlength)
      for (d <- ddocs) {
        d.add(f1)
        d.add(f2)
        diw.addDocument(d)
      }
      for (d <- hdocs) {
        d.add(f1)
        d.add(f2)
        hiw.addDocument(d)
      }
      for (d <- pdocs) {
        d.add(f1)
        piw.addDocument(d)
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
