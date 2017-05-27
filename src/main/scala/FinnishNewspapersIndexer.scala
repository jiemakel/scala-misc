import org.apache.lucene.index.IndexWriter
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import org.json4s._
import org.json4s.native.JsonParser._
import org.json4s.JsonDSL._
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField

import org.rogach.scallop._
import scala.language.postfixOps


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

  class Opts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val index = opt[String](required = true)
    val indexMemoryMB = opt[Long](default = Some(Runtime.getRuntime.maxMemory()/1024/1024*3/4), validate = (0<))
    val directories = trailArg[List[String]]()
    verify()
  }
  
  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    diw = iw(opts.index()+"/dindex", new Sort(new SortField("newspaperID",SortField.Type.LONG)), opts.indexMemoryMB() / 3)
    aiw = iw(opts.index()+"/aindex", new Sort(new SortField("newspaperID",SortField.Type.LONG),new SortField("articleID",SortField.Type.LONG)), opts.indexMemoryMB() / 3)
    piw = iw(opts.index()+"/pindex", new Sort(new SortField("newspaperID",SortField.Type.LONG),new SortField("paragraphID",SortField.Type.LONG)), opts.indexMemoryMB() / 3)
    val writers = Seq(diw, aiw, piw)
    feedAndProcessFedTasksInParallel(() => {
      //addTask()
    })
    close(writers)
    mergeIndices(Seq(
     (opts.index()+"/dindex", new Sort(new SortField("newspaperID",SortField.Type.LONG)), opts.indexMemoryMB() / 3),
     (opts.index()+"/aindex", new Sort(new SortField("newspaperID",SortField.Type.LONG),new SortField("articleID",SortField.Type.LONG)), opts.indexMemoryMB() / 3),
     (opts.index()+"/pindex", new Sort(new SortField("newspaperID",SortField.Type.LONG),new SortField("paragraphID",SortField.Type.LONG)), opts.indexMemoryMB() / 3)))
  }
}
