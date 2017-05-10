import fi.seco.lexical.combined.CombinedLexicalAnalysisService
import org.rogach.scallop.ScallopConf
import java.io.File
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.StreamConverters._
import scala.collection.JavaConverters._
import scala.xml.pull.XMLEventReader
import scala.io.Source
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvElemEnd
import scala.xml.MetaData
import scala.xml.pull.EvText
import scala.xml.pull.EvEntityRef
import scala.xml.pull.EvComment
import scala.xml.parsing.XhtmlEntities
import java.util.Locale
import fi.seco.lexical.LanguageRecognizer
import scala.util.Try
import com.typesafe.scalalogging.LazyLogging
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.text.CommonTextObjectFactories
import java.util.Collections
import org.json4s._
import org.json4s.JsonDSL._

import fi.seco.lexical.hfst.HFSTLexicalAnalysisService
import java.io.PrintWriter


object STTArticleAnalyzer extends ParallelProcessor {
  val la = new CombinedLexicalAnalysisService()
  val fiLocale = new Locale("fi")
  
  class Opts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val dest = opt[String](required = true)
    val directories = trailArg[List[String]]()
    verify()
  }
  
  /** helper function to turn XML attrs back into text */
  def attrsToString(attrs:MetaData) = {
    attrs.length match {
      case 0 => ""
      case _ => attrs.map( (m:MetaData) => " " + m.key + "='" + m.value +"'" ).reduceLeft(_+_)
    }
  }
  
  object LanguageDetector {
    lazy val languageProfiles = new LanguageProfileReader().readAllBuiltIn()
    lazy val supportedLanguages = languageProfiles.asScala.map(_.getLocale.toString())
    lazy val detector = LanguageDetectorBuilder.create(NgramExtractors.standard()).withProfiles(languageProfiles).build()
    lazy val textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText()
    def apply(text: String) = detector.getProbabilities(textObjectFactory.forText(text))
  }

  
   def getBestLang(text: String, locales: Seq[String] = Seq.empty): Option[String] = {
    if (locales.isEmpty) {
      val lrResult = Option(LanguageRecognizer.getLanguageAsObject(text)).map(r => Map(r.getLang() -> r.getIndex))
      val ldResult = Try(LanguageDetector(text).asScala.map(l => Map(l.getLocale.toString -> l.getProbability))).getOrElse(Seq.empty)
      val hfstResultTmp = la.getSupportedAnalyzeLocales.asScala.map(lang =>
            (lang.toString(),la.recognize(text, lang))).filter(_._2.getRate!=0.0).toSeq.sortBy(_._2.getRate).reverse.map(p => (p._1,p._2.getRate*p._2.getRate))
      val tc = hfstResultTmp.foldRight(0.0) { _._2 + _ }
      val hfstResult = hfstResultTmp.map(p => Map(p._1 -> p._2 / tc))
      Try(Some((ldResult ++ hfstResult ++ lrResult).groupBy(_.keysIterator.next).mapValues(_.foldRight(0.0) { (p, r) => r + p.valuesIterator.next } / 3.0).maxBy(_._2)._1)).getOrElse(None)
    } else {
      val lrResult = Option(LanguageRecognizer.getLanguageAsObject(text, locales: _*)).map(r => Map(r.getLang() -> r.getIndex))
      val ldResult = Try(LanguageDetector(text).asScala.filter(d => locales.contains(d.getLocale.toString)).map(l => Map(l.getLocale.toString -> l.getProbability))).getOrElse(Seq.empty)
      val hfstResultTmp = locales.map(new Locale(_)).intersect(la.getSupportedAnalyzeLocales.asScala.toSeq).map(lang =>
            (lang.toString(),la.recognize(text, lang))).filter(_._2.getRate!=0.0).toSeq.sortBy(_._2.getRate).reverse.map(p => (p._1,p._2.getRate*p._2.getRate))
      val tc = hfstResultTmp.foldRight(0.0) { _._2 + _ }
      val hfstResult = hfstResultTmp.map(p => Map(p._1 -> p._2 / tc))
      Try(Some((ldResult ++ hfstResult ++ lrResult).groupBy(_.keysIterator.next).mapValues(_.foldRight(0.0) { (p, r) => r + p.valuesIterator.next } / 3.0).maxBy(_._2)._1)).getOrElse(None)
    }
  }
   
  implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new CustomSerializer[HFSTLexicalAnalysisService.Result.WordPart](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.Result.WordPart => JObject(JField("lemma",r.getLemma) :: JField("tags",JArray(r.getTags.asScala.map(Extraction.decompose).toList)) :: Nil) })) +
   new CustomSerializer[HFSTLexicalAnalysisService.Result](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.Result => JObject(JField("weight",r.getWeight) :: JField("wordParts",JArray(r.getParts.asScala.map(Extraction.decompose).toList)) :: JField("globalTags",JArray(r.getGlobalTags.asScala.map(Extraction.decompose).toList)) :: Nil) })) +
   new CustomSerializer[HFSTLexicalAnalysisService.WordToResults](implicit format => (PartialFunction.empty,
      { case r: HFSTLexicalAnalysisService.WordToResults => JObject(JField("word",r.getWord) :: JField("analysis", JArray(r.getAnalysis.asScala.map(Extraction.decompose).toList)) :: Nil) }))
  
  private val langs = Seq("fi","sv","en")
      
  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val dest = opts.dest()
    new File(dest).mkdirs()
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toArray.flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".xml")).forEach(file => {
        val xml = new XMLEventReader(Source.fromFile(file, "UTF-8"))
        var skip = false 
        while (xml.hasNext && !skip) xml.next match {
          case EvElemStart(_,"genre",attrs,_) =>
            val qcode = attrs("qcode")(0).text
            skip = qcode.startsWith("sttversion") && qcode != "sttversion:5"
          case EvElemStart(_,"body",_,_) =>
            var break = false
            var content = new StringBuilder
            content.append("<body>")
            while (xml.hasNext && !break) xml.next match {
              case EvElemStart(_, elem, attrs, _) => content.append("<" + elem + attrsToString(attrs) + ">")
              case EvElemEnd(_,"body") => break = true
              case EvText(text) => 
                content.append(text)
              case er: EvEntityRef => 
                content.append(XhtmlEntities.entMap.get(er.entity) match {
                  case Some('&') => "&amp;"
                  case Some(chr) => chr
                  case _ => "&" + er.entity + ";"
                })
              case EvElemEnd(_, label) =>
                content.append("</"+label+">")
              case EvComment(_) =>
            }
            content.append("</body>")
            val contentXML = new XMLEventReader(Source.fromString(content.toString))
            val stringContent = new StringBuilder
            contentXML.next // body
            var inOrderedList = false
            while (contentXML.hasNext) contentXML.next match {
              case EvElemStart(_, "h1",_,_) => stringContent.append("\n # ") 
              case EvElemStart(_, "h2",_,_) => stringContent.append("\n ## ")
              case EvElemStart(_, "h3",_,_) => stringContent.append("\n ## ")
              case EvElemStart(_, "h4",_,_) => stringContent.append("\n ## ")
              case EvElemStart(_, "Company",_,_) => 
              case EvElemStart(_, "p",_,_) => stringContent.append("\n")
              case EvElemStart(_, "ul",_,_) =>
                inOrderedList = false
                stringContent.append('\n')
              case EvElemStart(_, "ol",_,_) =>
                inOrderedList = true
              case EvElemEnd(_, "li") => stringContent.append('\n')
              case EvElemStart(_, "li",_,_) =>
                stringContent.append(if (inOrderedList) "1." else "* ")
              case EvText(text) => stringContent.append(text)
              case er: EvEntityRef => 
                content.append(XhtmlEntities.entMap.get(er.entity) match {
                  case Some(chr) => chr
                  case _ => er.entity
                })
              case _ => 
            }
            val contentS = stringContent.toString
            val lang = getBestLang(contentS)
            if (lang != Some("sv") && lang != Some("en")) {
              if (lang != Some("fi")) logger.info("language of "+file+" detected as "+lang)
              val paragraphs = contentS.split("\\s*\n\n\\s*")
              val analysis = org.json4s.native.JsonMethods.pretty(org.json4s.native.JsonMethods.render(paragraphs.toList.filter(!_.isEmpty).map(la.analyze(_, fiLocale, Collections.EMPTY_LIST.asInstanceOf[java.util.List[String]], false, true, false, 0, 1).asScala.map(Extraction.decompose).toList)))
              val writer = new PrintWriter(new File(dest+"/"+file.getName+".analysis.json"))
              writer.write(analysis)
              writer.close()
            }
          case _ =>
        }
      })
    )
  }
}