import java.io.{File, FileInputStream, InputStreamReader, PrintWriter}
import java.util
import java.util.{Collections, Locale}

import fi.seco.lexical.combined.CombinedLexicalAnalysisService
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonParser._

import scala.compat.java8.StreamConverters._
import scala.jdk.CollectionConverters._

object IltalehtiArticleAnalyzer extends ParallelProcessor {
  
  val la = new CombinedLexicalAnalysisService()
  
  implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new CustomSerializer[HFSTLexicalAnalysisService.Result.WordPart](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.Result.WordPart => JObject(JField("lemma",r.getLemma) :: JField("tags",JArray(r.getTags.asScala.toList.map(Extraction.decompose))) :: Nil) })) +
   new CustomSerializer[HFSTLexicalAnalysisService.Result](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.Result => JObject(JField("weight",r.getWeight) :: JField("wordParts",JArray(r.getParts.asScala.toList.map(Extraction.decompose))) :: JField("globalTags",JArray(r.getGlobalTags.asScala.toList.map(Extraction.decompose))) :: Nil) })) +
   new CustomSerializer[HFSTLexicalAnalysisService.WordToResults](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.WordToResults => JObject(JField("word",r.getWord) :: JField("analysis", JArray(r.getAnalysis.asScala.toList.map(Extraction.decompose))) :: Nil) }))

  val fiLocale = new Locale("fi")

  def analyze(str: String): collection.Seq[HFSTLexicalAnalysisService.WordToResults] =  {
    val ret = la.analyze(str, fiLocale, Collections.EMPTY_LIST.asInstanceOf[util.List[String]], false, true, false, 0, 1).asScala
    var inQuote = false
    for (wta <- ret) {
      if (wta.getAnalysis.get(0).getGlobalTags.containsKey("FIRST_IN_SENTENCE"))
        inQuote = wta.getWord == "-"
      if (inQuote) wta.getAnalysis.forEach(_.addGlobalTag("IS_QUOTATION","TRUE"))
    }
    ret
  }

  def main(args: Array[String]): Unit = {
    val dest = args.last
    feedAndProcessFedTasksInParallel(() =>
      args.dropRight(1).toIndexedSeq.flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".json")).forEach(file => {
        logger.info("Parsing "+file)
        parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
          var token = p.nextToken // OpenArr
          token = p.nextToken // OpenObj/CloseArr
          while (token != CloseArr) {
            assert(token == OpenObj, token)
            val obj = ObjParser.parseObject(p, Some(token))
            val id = (obj \ "article_id").asInstanceOf[JString].values
            addTask(file + "/" + id, () => {
              val json = org.json4s.native.JsonMethods.pretty(org.json4s.native.JsonMethods.render(obj transform {
                case o: JObject =>
                  var o2 = o \ "body" match {
                    case string: JString => o merge JObject(JField("analyzedBody", JArray(analyze(string.values).map(Extraction.decompose).toList)))
                    case _ => o
                  }
                  o2 = o2 \ "lead" match {
                    case string: JString => o2 merge JObject(JField("analyzedLead", JArray(analyze(string.values).map(Extraction.decompose).toList)))
                    case _ => o2
                  }
                  o2 \ "title" match {
                    case string: JString => o2 merge JObject(JField("analyzedTitle", JArray(analyze(string.values).map(Extraction.decompose).toList)))
                    case _ => o2
                  }
              }))
              val dir = dest+"/"+id.charAt(0)+"/"+id.charAt(1)+"/"
              new File(dir).mkdirs()
              val writer = new PrintWriter(new File(dir+id+".analysis.json"))
              writer.write(json)
              writer.close()
            })
            token = p.nextToken // OpenObj/CloseArr
          }})
        logger.info("File "+file+" processed.")
      }))
  }
}