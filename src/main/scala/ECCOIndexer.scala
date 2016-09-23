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

object EccoIndexer {
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
    return content.toString
  }
  
  val fileRegex = ".*_(.*)\\.txt".r
  
  val headingRegex = "^# ".r
      
  def main(args: Array[String]): Unit = {
    val dir = FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/index"))
    val analyzer = new StandardAnalyzer()
    val iwc = new IndexWriterConfig(analyzer)
    val iw = new IndexWriter(dir, iwc)
    iw.deleteAll()
    for (dir<-args.toSeq;file <- getFileTree(new File(dir))) if (file.getName.endsWith("_metadata.xml")) {
      println("Processing "+file.getParent)
      val xmls = Source.fromFile(file)
      implicit val xml = new XMLEventReader(xmls)
      val d = new Document()
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"documentID",_,_) => d.add(new Field("metadata_documentID",readContents,StoredField.TYPE))
        case EvElemStart(_,"ESTCID",_,_) => d.add(new Field("metadata_ESTCID",readContents,StoredField.TYPE))
        case EvElemStart(_,"pubDate",_,_) => d.add(new Field("metadata_pubDate",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"language",_,_) => d.add(new Field("metadata_language",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"module",_,_) => d.add(new Field("metadata_module",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"documentType",_,_) => d.add(new Field("metadata_documentType",readContents,StringField.TYPE_STORED))
        case EvElemStart(_,"notes",_,_) => d.add(new Field("metadata_notes",readContents,TextField.TYPE_STORED))
        case EvElemStart(_,"fullTitle",_,_) => d.add(new Field("metadata_fullTitle",readContents,TextField.TYPE_STORED))
        case _ => 
      }
      xmls.close()
      for (file <- getFileTree(file.getParentFile)) if (file.getName.endsWith(".txt") && !file.getName.contains("_page")) {
        val field = fileRegex.findFirstMatchIn(file.getName).get.group(1)
        val contents = new StringBuilder
        val f = Source.fromFile(file)
        for (line <- f.getLines) {
          if (line.startsWith("# "))
            d.add(new Field("heading_"+field,line.substring(2),TextField.TYPE_NOT_STORED))
          else {
            contents.append(line)
            contents.append('\n')
          }
        }
        f.close()
        d.add(new Field("contents_"+field, contents.toString, TextField.TYPE_NOT_STORED))
      }
      iw.addDocument(d)
    }
    iw.close()
    
/*    val ir = DirectoryReader.open(dir)
    val is = new IndexSearcher(ir)
    val qp = new QueryParser("text", analyzer)
    val q = qp.parse("test")
    is.search(q, new Collector() {
      
      def needsScores: Boolean = false
      
      def getLeafCollector(context: LeafReaderContext): LeafCollector = {
        val docBase = context.docBase;
        return new LeafCollector() {
          def setScorer(scorer: Scorer) {}
         
          def collect(doc: Int) {
            docBase+doc
          }
        }
      }
    })
    */
    dir.close()
  }
}