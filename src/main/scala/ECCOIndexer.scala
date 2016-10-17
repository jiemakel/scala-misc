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

object ECCOIndexer {
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
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
 
  val dir = FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/dindex"))
  val dir2 = FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/cindex"))
  val iwc = new IndexWriterConfig(analyzer)
  val iwc2 = new IndexWriterConfig(analyzer)
  val iw = new IndexWriter(dir, iwc)
  val iw2 = new IndexWriter(dir2, iwc2)
 

  def getIndexedLength(text: String): Int = {
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
  
  def main(args: Array[String]): Unit = {
    iw.deleteAll()
    iw2.deleteAll()
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
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"documentID",_,_) => md.add(new Field("metadata_documentID",readContents,StoredField.TYPE))
        case EvElemStart(_,"ESTCID",_,_) => md.add(new Field("metadata_ESTCID",readContents,StoredField.TYPE))
        case EvElemStart(_,"pubDate",_,_) => md.add(new Field("metadata_pubDate",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"language",_,_) => md.add(new Field("metadata_language",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"module",_,_) => md.add(new Field("metadata_module",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"notes",_,_) => md.add(new Field("metadata_notes",readContents,TextField.TYPE_STORED))
        case EvElemStart(_,"fullTitle",_,_) => md.add(new Field("metadata_fullTitle",readContents,TextField.TYPE_STORED))
        case _ => 
      }
      xmls.close()
      val d = new Document()
      for (f <- md) d.add(f)
      var tlength = 0
      val tlengths = new HashMap[String,Int]
      for (file <- getFileTree(file.getParentFile)) if (file.getName.endsWith(".txt") && !file.getName.contains("_page")) {
        var l1Heading: Seq[Field] = Seq.empty
        var l2Heading: Seq[Field] = Seq.empty
        var d2o: Option[Document] = None
        val field = fileRegex.findFirstMatchIn(file.getName).get.group(1)
        val contents = new StringBuilder
        val fl = Source.fromFile(file)
        for (line <- fl.getLines) {
          if (line.startsWith("# ") || line.startsWith("## ") || line.startsWith("### ")) {
            if (contents.length>0) {
              var f = new Field("contents_"+field, contents.toString, TextField.TYPE_NOT_STORED)
              d.add(f)
              val d2 = d2o.getOrElse({ 
                val d2 = new Document()
                for (f <- md) d2.add(f)
                d2
              })
              d2.add(f)
              val l = getIndexedLength(contents.toString)
              tlengths.put("tclength_"+field, tlengths.getOrElseUpdate("tclength_"+field, 0) + l)
              f = new StoredField("clength_"+field, l)
              d.add(f)
              d2.add(f)
              tlength += l
              iw2.addDocument(d2)
            }
            contents.clear()
            val d2 = new Document()
            for (f <- md) d2.add(f)
            d2o = Some(d2)
            val level =
              if (line.startsWith("### ")) 3
              else if (line.startsWith("## ")) 2
              else 1
            val f = new Field("heading"+level+"_"+field,line.substring(level+1),TextField.TYPE_STORED)
            d.add(f)
            d2.add(f)
            val l = getIndexedLength(line.substring(level+1))
            val f2 = new StoredField("h"+level+"length_"+field, l)
            d.add(f2)
            d2.add(f2)
            if (level==3) {
              l1Heading.foreach(d2.add(_))
              l2Heading.foreach(d2.add(_))
            } else if (level==2) {
              l1Heading.foreach(d2.add(_))
              l2Heading = Seq(f,f2)
            } else {
              l1Heading = Seq(f,f2)
              l2Heading = Seq.empty
            }
            tlengths.put("hc"+level+"length_"+field, tlengths.getOrElseUpdate("hc"+level+"length_"+field, 0) + l)
            tlengths.put("hclength_"+field, tlengths.getOrElseUpdate("hclength_"+field, 0) + l)
            tlength += l
          } else
            contents.append(line)
        }
        fl.close()
        var f = new Field("contents_"+field, contents.toString, TextField.TYPE_NOT_STORED)
        d.add(f)
        val d2 = d2o.getOrElse({ 
          val d2 = new Document()
          for (f <- md) d2.add(f)
          d2
        })
        d2.add(f)
        val l = getIndexedLength(contents.toString)
        tlengths.put("tclength_"+field, tlengths.getOrElseUpdate("tclength_"+field, 0) + l)
        f = new StoredField("clength_"+field, l)
        d.add(f)
        d2.add(f)
        tlength += l
        iw2.addDocument(d2)
      }
      for ((field,tlength) <- tlengths) d.add(new StoredField(field,tlength))
      d.add(new StoredField("tlength",tlength))
      iw.addDocument(d)
    }
    iw.forceMerge(1)
    iw2.forceMerge(1)
    iw.commit()
    iw2.commit()
    iw.close()
    iw2.close()
    dir.close()
    dir2.close()
  }
}
