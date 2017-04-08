import org.apache.lucene.index.IndexWriter
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import org.json4s._
import org.json4s.native.JsonParser._
import org.json4s.JsonDSL._
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField


object FinnishNewspapersIndexer extends OctavoIndexer {
  
  // hierarchy: newspaper, article, paragraph
  
  /*
   * Needs to handle results from morphological analysis.
	 * Plan:
	 * Index for original data
	 * Parallel index for best guess lemma
	 * Parallel index for all guessed lemmas (with positionIncrementAttr set to zero, and weights [and/or morphological analysis] stored as payloads)
	 * Parallel index for additional metadata
   */
  
  var diw, aiw, piw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  def main(args: Array[String]): Unit = {
    diw = iw(args.last+"/dindex")
    aiw = iw(args.last+"/aindex")
    piw = iw(args.last+"/pindex")
    val writers = Seq(diw, aiw, piw)
    writers.foreach(clear(_))
    feedAndProcessFedTasksInParallel(() => {
      //addTask()
    })
    writers.foreach(close(_))
    mergeIndices(Seq(
     (args.last+"/dindex", new Sort(new SortField("newspaperID",SortField.Type.LONG))),
     (args.last+"/aindex", new Sort(new SortField("newspaperID",SortField.Type.LONG),new SortField("articleID",SortField.Type.LONG))),
     (args.last+"/pindex", new Sort(new SortField("newspaperID",SortField.Type.LONG),new SortField("paragraphID",SortField.Type.LONG)))))
  }
}
