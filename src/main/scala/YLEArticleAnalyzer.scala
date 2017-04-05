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
  
  private class ValStack(parser: Parser) {
    import java.util.LinkedList
    private[this] val stack = new LinkedList[Any]()

    def popAny = stack.poll
    def pop[A](expectedType: Class[A]) = convert(stack.poll, expectedType)
    def push(v: Any) = stack.addFirst(v)
    def peekAny = stack.peek
    def peek[A](expectedType: Class[A]) = convert(stack.peek, expectedType)
    def replace[A](newTop: Any) = stack.set(0, newTop)

    private def convert[A](x: Any, expectedType: Class[A]): A = {
      if (x == null) parser.fail("expected object or array")
      try { x.asInstanceOf[A] } catch { case _: ClassCastException => parser.fail(s"unexpected $x") }
    }

    def peekOption = if (stack isEmpty) None else Some(stack.peek)
  }
  
  private val objParser = (p: Parser, initialToken: Option[Token]) => {
    val vals = new ValStack(p)
    var token: Token = null
    var root: Option[JValue] = None

    // This is a slightly faster way to correct order of fields and arrays than using 'map'.
    def reverse(v: JValue): JValue = v match {
      case JObject(l) => JObject((l.map { case (n, v) => (n, reverse(v)) }).reverse)
      case JArray(l) => JArray(l.map(reverse).reverse)
      case x => x
    }

    def closeBlock(v: Any): Unit = {
      @inline def toJValue(x: Any) = x match {
        case json: JValue => json
        case scala.util.control.NonFatal(_) => p.fail(s"unexpected field $x")
      }

      vals.peekOption match {
        case Some((name: String, value)) =>
          vals.pop(classOf[JField])
          val obj = vals.peek(classOf[JObject])
          vals.replace(JObject((name, toJValue(v)) :: obj.obj))
        case Some(o: JObject) =>
          vals.replace(JObject(vals.peek(classOf[JField]) :: o.obj))
        case Some(a: JArray) => vals.replace(JArray(toJValue(v) :: a.arr))
        case Some(x) => p.fail(s"expected field, array or object but got $x")
        case None => root = Some(reverse(toJValue(v)))
      }
    }

    def newValue(v: JValue): Unit = {
      vals.peekAny match {
        case (name: String, value) =>
          vals.pop(classOf[JField])
          val obj = vals.peek(classOf[JObject])
          vals.replace(JObject((name, v) :: obj.obj))
        case a: JArray => vals.replace(JArray(v :: a.arr))
        case _ => p.fail("expected field or array")
      }
    }
    token = initialToken.getOrElse(p.nextToken)
    while (!root.isDefined && token != End) {
      token match {
        case OpenObj          => vals.push(JObject(Nil))
        case FieldStart(name) => vals.push(JField(name, null))
        case StringVal(x)     => newValue(JString(x))
        case IntVal(x)        => newValue(JInt(x))
        case LongVal(x)       => newValue(JLong(x))
        case DoubleVal(x)     => newValue(JDouble(x))
        case BigDecimalVal(x) => newValue(JDecimal(x))
        case BoolVal(x)       => newValue(JBool(x))
        case NullVal          => newValue(JNull)
        case CloseObj         => closeBlock(vals.popAny)
        case OpenArr          => vals.push(JArray(Nil))
        case CloseArr         => closeBlock(vals.pop(classOf[JArray]))
        case End              =>
      }
      if (!root.isDefined && token != End) token = p.nextToken
    }
    root getOrElse JNothing
  }

  
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
            val obj = objParser(p, Some(token))
            val id = (obj \ "id").asInstanceOf[JString].values
            if ((obj \ "language").asInstanceOf[JString].values == "fi")
              addTask(file + "/" + id, () => {
                val json = org.json4s.native.JsonMethods.pretty(org.json4s.native.JsonMethods.render(obj transform {
                  case o: JObject => 
                    val text =  (o \ "text")
                    if (text.isInstanceOf[JString])
                      o merge JObject(JField("analyzedText",JArray(la.analyze(text.asInstanceOf[JString].values, fiLocale, Collections.EMPTY_LIST.asInstanceOf[java.util.List[String]], false, true, false, 2).asScala.map(Extraction.decompose).toList)))
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