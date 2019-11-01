import java.io.{File, PrintWriter}
import java.util.{Collections, Locale}

import au.com.bytecode.opencsv.CSVParser
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.CommonTextObjectFactories
import fi.seco.lexical.LanguageRecognizer
import fi.seco.lexical.combined.CombinedLexicalAnalysisService
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService
import org.json4s.JsonDSL._
import org.json4s._
import org.jsoup.Jsoup
import org.rogach.scallop.ScallopConf

import scala.collection.JavaConverters._
import scala.compat.java8.StreamConverters._
import scala.util.{Failure, Success, Try}
import scala.xml.MetaData


object HSArticleAnalyzer extends ParallelProcessor {
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
  
  def analyze(outputFile: File, article: String): Unit = {
    val contentDocument = Jsoup.parse("<article>"+article.replaceAllLiterally("&", "&amp;")+"</article>")
    val paragraphs = contentDocument.select("p").asScala.map(_.text)
    /*val contentXML = new XMLEventReader(Source.fromString("<article>"+article.replaceAllLiterally("&", "&amp;")+"</article>"))
    val stringContent = new StringBuilder
    while (contentXML.hasNext) contentXML.next match {
      case EvElemStart(_, "p",_,_) => stringContent.append("\n")
      case EvText(text) => stringContent.append(text)
      case er: EvEntityRef => 
        stringContent.append(XhtmlEntities.entMap.get(er.entity) match {
          case Some(chr) => chr
          case _ => er.entity
        })
      case _ => 
    }
    val contentS = stringContent.toString
    val paragraphs = contentS.split("\\s*\n\n\\s*") */
    val analysis = org.json4s.native.JsonMethods.pretty(org.json4s.native.JsonMethods.render(paragraphs.toList.filter(!_.isEmpty).map(la.analyze(_, fiLocale, Collections.EMPTY_LIST.asInstanceOf[java.util.List[String]], false, true, false, 0, 1).asScala.map(Extraction.decompose).toList)))
    val writer = new PrintWriter(outputFile)
    writer.write(analysis)
    writer.close()
    logger.info("Successfully processed "+outputFile)
  }
      
  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val dest = opts.dest()
    createHashDirectories(dest)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toIndexedSeq.flatMap(n => getFileTree(new File(n))).parStream.forEach(file => Try({
        val wr = CSVReader.open(file.getPath)(new DefaultCSVFormat{override val escapeChar =  CSVParser.NULL_CHARACTER})
        // articleId, nodeId, nodeTitle, startDate, modifiedDate, title, byLine, ingress, body
        for (r <- wr) {
          val outputFile = new File(dest + "/" + Math.abs(r(0).hashCode() % 10) + "/" + Math.abs(r(0).hashCode() % 100 / 10) + "/" + r(0) + ".analysis.json")
          if (!outputFile.exists) {
            if (r.length >= 9)
              addTask(file + ":" + r(0), () => analyze(outputFile, r(8)))
            else logger.warn(r.toSeq + " is not 9 columns.")
          }
        }
      }) match {
            case Success(_) => 
              logger.info("File "+file+" processed.")
            case Failure(e) => logger.error("Error processing file "+file, e)
          })
    )
  }
}