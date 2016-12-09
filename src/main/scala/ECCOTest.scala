import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store.MMapDirectory
import java.nio.file.FileSystems
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.FieldType
import org.apache.lucene.document.TextField
import org.apache.lucene.document.Field
import org.apache.lucene.util.NumericUtils
import org.apache.lucene.codecs.compressing.OrdTermVectorsReader.TVTermsEnum
import fi.seco.lucene.FSTOrdTermVectorsCodec
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.uhighlight.UnifiedHighlighter
import org.apache.lucene.search.TermQuery
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter
import org.apache.lucene.search.vectorhighlight.FieldQuery
import org.apache.lucene.index.IndexOptions

object ECCOTest {
  
   val analyzer = new StandardAnalyzer()
  
   val codec = new FSTOrdTermVectorsCodec()
  
   val contentFieldType = new FieldType(TextField.TYPE_STORED)
   contentFieldType.setOmitNorms(true)
   contentFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)

   contentFieldType.setStoreTermVectors(true)
   contentFieldType.setStoreTermVectorOffsets(true)
   contentFieldType.setStoreTermVectorPositions(true)
   
   def main(args: Array[String]): Unit = {
     var iw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath("/srv/ecco/testindex")), new IndexWriterConfig(analyzer))
     iw.deleteAll()
     var d = new Document
     d.add(new Field("content","c cdef eaaa eafaagjakjajsd",contentFieldType))
     for (i <- 0 to 10000) iw.addDocument(d)
     iw.commit()
     d = new Document
     d.add(new Field("content","b g h",contentFieldType))
     iw.addDocument(d)
     d = new Document
     d.add(new Field("content","i j k l m n",contentFieldType))
     iw.addDocument(d)
     iw.close  
     var ir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath("/srv/ecco/testindex")))
     println(ir.getTermVector(0, "content").iterator().next().utf8ToString())
     var it = ir.leaves().get(0).reader().terms("content").iterator()
     var is = new IndexSearcher(ir)
     var hl1 = new UnifiedHighlighter(is, analyzer)
     var hl2 = new FastVectorHighlighter()
     var q = new QueryParser("content",analyzer).parse("ea*")
     var td = is.search(q,100)
     println(td.totalHits)
     println(hl2.getBestFragment(hl2.getFieldQuery(q), ir, td.scoreDocs(0).doc, "content", 100))
     println(hl1.highlight("content",q,td).mkString(","))
     
     ir.close()
     iw = new IndexWriter(new MMapDirectory(FileSystems.getDefault().getPath("/srv/ecco/testindex")), new IndexWriterConfig(analyzer).setCodec(codec))
     iw.forceMerge(1)
     iw.close()
     ir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath("/srv/ecco/testindex")))
     it = ir.leaves().get(0).reader().terms("content").iterator()
     System.out.println("R3");
     println(it.next().utf8ToString()+":"+it.ord)
     println(it.next().utf8ToString()+":"+it.ord)
     println(it.next().utf8ToString()+":"+it.ord)
     it.seekExact(3l)
     println("3="+it.term().utf8ToString());
     it.seekExact(0l)
     println("0="+it.term().utf8ToString());
     var tv = ir.getTermVector(10001, "content").iterator
     var br = tv.next()
     print("TV:")
     while (br!=null) {
       print(br.utf8ToString+",")
       br = tv.next()
     }
     println
     tv = ir.getTermVector(10002, "content").iterator
     br = tv.next()
     print("TV:")
     while (br!=null) {
       print(br.utf8ToString+",")
       br = tv.next()
     }
     println     
     val tv2 = ir.getTermVector(10002, "content").iterator.asInstanceOf[TVTermsEnum]
     var ord = tv2.nextOrd()
     print("OV:")
     while (ord != -1l) {
       it.seekExact(ord)
       print(it.term().utf8ToString+",")
       ord = tv2.nextOrd()
     }
     println     
     is = new IndexSearcher(ir)
     hl1 = new UnifiedHighlighter(is, analyzer)
     hl2 = new FastVectorHighlighter()
     q = new QueryParser("content",analyzer).parse("ea*")
     td = is.search(q,100)
     println(td.totalHits)
     println(hl2.getBestFragment(hl2.getFieldQuery(q), ir, td.scoreDocs(0).doc, "content", 100))
     println(hl1.highlight("content",q,td).mkString(","))
  }
}