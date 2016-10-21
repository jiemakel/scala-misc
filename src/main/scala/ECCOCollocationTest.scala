import java.nio.file.FileSystems
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.spans.SpanQuery
import org.apache.lucene.search.spans.SpanTermQuery
import org.apache.lucene.index.Term
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.SimpleCollector
import org.apache.lucene.index.LeafReaderContext
import scala.collection.JavaConversions._
import org.apache.lucene.util.BytesRef
import scala.collection.mutable.HashMap
import org.apache.lucene.search.FuzzyTermsEnum
import org.apache.lucene.util.AttributeSource
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer


object ECCOCollocationTest {
  
  val dir = FSDirectory.open(FileSystems.getDefault().getPath("/srv/ecco/pindex"))
  val ir = DirectoryReader.open(dir)
  val is = new IndexSearcher(ir)
  
  def main(args: Array[String]): Unit = {
    val q = new TermQuery(new Term("contents","statues"))
    val cv = new HashMap[String,Int].withDefaultValue(0)
    is.search(q, new SimpleCollector() {
      override def needsScores: Boolean = false
      var context: LeafReaderContext = null

      override def collect(doc: Int) {
        val tv = this.context.reader.getTermVector(doc, "contents").iterator()
        var term: BytesRef = tv.next()
        while (term!=null) {
          cv(term.utf8ToString) += tv.docFreq
          term = tv.next()
        }
      }

      override def doSetNextReader(context: LeafReaderContext) = {
        this.context = context
      }
    })
    val cv2 = new HashMap[String,Buffer[String]]
    val cv2DF = new HashMap[String,Long].withDefaultValue(0l)
    for (
        w <- cv.map(_._1);
        as = new AttributeSource();
        t = new Term("contents",w);
        lrc <- ir.leaves; terms = lrc.reader.terms("contents"); 
        if (terms!=null)) {
      val fte = new FuzzyTermsEnum(terms,as,t,1,1,false)
      var br = fte.next()
      while (br!=null) {
        cv2.getOrElseUpdate(w, new ArrayBuffer()) += br.utf8ToString
        cv2DF(br.utf8ToString) += fte.docFreq
        br = fte.next()
      }
    }
    val f = cv2.map(p => (p._2.maxBy(cv2DF(_)),(cv(p._1)-1).toDouble/p._2.foldLeft(0.0)((p,v) => p+cv2DF(v))))
    println(f.toSeq.sortWith{ case (p1,p2) => p1._2.compareTo(p2._2) > 0}.take(20))
  }
}