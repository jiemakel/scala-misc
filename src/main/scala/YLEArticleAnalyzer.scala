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
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import java.util.Locale
import java.io.PrintWriter
import java.util.Collections

object YLEArticleAnalyzer extends ParallelProcessor {
  
  val la = new CombinedLexicalAnalysisService()
  
  implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new CustomSerializer[HFSTLexicalAnalysisService.Result.WordPart](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.Result.WordPart => JObject(JField("lemma",r.getLemma) :: JField("tags",JArray(r.getTags.asScala.map(Extraction.decompose).toList)) :: Nil) })) +
   new CustomSerializer[HFSTLexicalAnalysisService.Result](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.Result => JObject(JField("weight",r.getWeight) :: JField("wordParts",JArray(r.getParts.asScala.map(Extraction.decompose).toList)) :: JField("globalTags",JArray(r.getGlobalTags.asScala.map(Extraction.decompose).toList)) :: Nil) })) +
   new CustomSerializer[HFSTLexicalAnalysisService.WordToResults](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.WordToResults => JObject(JField("word",r.getWord) :: JField("analysis", JArray(r.getAnalysis.asScala.map(Extraction.decompose).toList)) :: Nil) }))

  val fiLocale = new Locale("fi")
  
  def main(args: Array[String]): Unit = {
    val dest = args.last
    feedAndProcessFedTasksInParallel(() =>
      args.dropRight(1).flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".json")).forEach(file => {
        parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
          var token = p.nextToken
          while (token != FieldStart("data")) token = p.nextToken
          token = p.nextToken // OpenArr
          token = p.nextToken // OpenObj/CloseArr
          while (token != CloseArr) {
            //assert(token == OpenObj, token)
            val obj = ObjParser.parseObject(p, Some(token))
            val id = (obj \ "id").asInstanceOf[JString].values
            if ((obj \ "language").asInstanceOf[JString].values == "fi")
              addTask(file + "/" + id, () => {
                val json = org.json4s.native.JsonMethods.pretty(org.json4s.native.JsonMethods.render(obj transform {
                  case o: JObject => 
                    val text =  (o \ "text")
                    if (text.isInstanceOf[JString])
                      o merge JObject(JField("analyzedText",JArray(la.analyze(text.asInstanceOf[JString].values, fiLocale, Collections.EMPTY_LIST.asInstanceOf[java.util.List[String]], false, true, false, 0, 1).asScala.map(Extraction.decompose).toList)))
                    else o
                }))
                val writer = new PrintWriter(new File(dest+"/"+id+".analysis.json"))
                writer.write(json)
                writer.close()
              })
            token = p.nextToken // OpenObj/CloseArr
          }})
        logger.info("File "+file+" processed.")
      }))
  }
}