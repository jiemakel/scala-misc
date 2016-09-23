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


object EccoIndexer {
  def main(args: Array[String]): Unit = {
    val dir = FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/index"))
    val analyzer = new StandardAnalyzer()
    val iwc = new IndexWriterConfig(analyzer)
    val iw = new IndexWriter(dir, iwc)
    val d = new Document()
    d.add(new Field("id", "1", StoredField.TYPE))
    d.add(new Field("text", "content", TextField.TYPE_NOT_STORED))
    iw.addDocument(d)
    iw.close()
    
    val ir = DirectoryReader.open(dir)
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
    dir.close()
  }
}