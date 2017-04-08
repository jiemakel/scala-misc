import fi.seco.lexical.combined.CombinedLexicalAnalysisService
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService
import play.api.libs.json.Writes
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import scala.collection.JavaConverters._
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService.WordToResults
import org.apache.lucene.index.IndexWriter
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.File
import org.json4s._
import org.json4s.native.JsonParser._
import org.json4s.JsonDSL._
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.StreamConverters._
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField

import org.rogach.scallop._
import scala.language.postfixOps


object YLEArticleIndexer extends OctavoIndexer {
  
  /*
   * Needs to handle results from morphological analysis.
	 * Plan:
	 * Index for original data
	 * Parallel index for best guess lemma
	 * Parallel index for all guessed lemmas (with positionIncrementAttr set to zero, and weights [and/or morphological analysis] stored as payloads)
	 * Parallel index for additional metadata
   */
  
  var diw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  class Article {
    
  }
   class Opts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val index = opt[String](required = true)
    val indexMemoryMB = opt[Long](default = Some(Runtime.getRuntime.maxMemory()/1024/1024*3/4), validate = (0<))
    val directories = trailArg[List[String]]()
    verify()
  }
  
  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    diw = iw(opts.index()+"/dindex",new Sort(new SortField("articleID",SortField.Type.INT)),opts.indexMemoryMB())
    clear(diw)
    feedAndProcessFedTasksInParallel(() =>
      args.dropRight(1).flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".json")).forEach(file => {
        parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
          val path = file.getParentFile.getName
          var token = p.nextToken
          while (token != End) {
            token match {
              case _ =>
            }
            token = p.nextToken
          }
        })
        logger.info("File "+file+" processed.")
      }))
    close(diw)
    mergeIndices(Seq(
     (args.last+"/dindex", new Sort(new SortField("articleID",SortField.Type.INT)),opts.indexMemoryMB())))
  }
}
