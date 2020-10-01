import java.io.{File, FileInputStream, PrintWriter}
import java.util.{Collections, Locale}

import ECCOXML2Text.attrsToString
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.CommonTextObjectFactories
import fi.seco.lexical.LanguageRecognizer
import fi.seco.lexical.combined.CombinedLexicalAnalysisService
import fi.seco.lexical.hfst.HFSTLexicalAnalysisService
import javax.xml.stream.XMLEventReader
import org.json4s.{JArray, JField, JObject}
import org.json4s.JsonDSL._
import org.json4s.{CustomSerializer, Extraction, NoTypeHints}
import org.rogach.scallop.ScallopConf

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.StreamConverters._
import scala.io.Source
import scala.util.Try
import scala.xml.MetaData
import scala.xml.parsing.XhtmlEntities
import XMLEventReaderSupport._
import com.github.tototoshi.csv.CSVWriter


object STTArticleAnalyzer extends ParallelProcessor {
  val la = new CombinedLexicalAnalysisService()
  val fiLocale = new Locale("fi")
  
  class Opts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val dest = opt[String](required = true)
    val directories = trailArg[List[String]]()
    verify()
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
      val lrResult = Option(LanguageRecognizer.getLanguageAsObject(text)).map(r => Map(r.getLang -> r.getIndex))
      val ldResult = Try(LanguageDetector(text).asScala.map(l => Map(l.getLocale.toString -> l.getProbability))).getOrElse(Seq.empty)
      val hfstResultTmp = la.getSupportedAnalyzeLocales.asScala.map(lang =>
            (lang.toString,la.recognize(text, lang))).filter(_._2.getRate!=0.0).toSeq.sortBy(_._2.getRate).reverse.map(p => (p._1,p._2.getRate*p._2.getRate))
      val tc = hfstResultTmp.foldRight(0.0) { _._2 + _ }
      val hfstResult = hfstResultTmp.map(p => Map(p._1 -> p._2 / tc))
      Try(Some((ldResult ++ hfstResult ++ lrResult).groupBy(_.keysIterator.next).mapValues(_.foldRight(0.0) { (p, r) => r + p.valuesIterator.next } / 3.0).maxBy(_._2)._1)).getOrElse(None)
    } else {
      val lrResult = Option(LanguageRecognizer.getLanguageAsObject(text, locales: _*)).map(r => Map(r.getLang() -> r.getIndex))
      val ldResult = Try(LanguageDetector(text).asScala.filter(d => locales.contains(d.getLocale.toString)).map(l => Map(l.getLocale.toString -> l.getProbability))).getOrElse(Seq.empty)
      val hfstResultTmp = locales.map(new Locale(_)).intersect(la.getSupportedAnalyzeLocales.asScala.toSeq).map(lang =>
            (lang.toString,la.recognize(text, lang))).filter(_._2.getRate!=0.0).toSeq.sortBy(_._2.getRate).reverse.map(p => (p._1,p._2.getRate*p._2.getRate))
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
  
  val aRegex = "<a([^>]*)>([^< ]*)(?!.*</a>)".r

  private def decodeEntity(entity: String): String = {
    XhtmlEntities.entMap.get(entity) match {
      case Some(chr) => chr.toString
      case None =>
        logger.warn("Encountered unknown entity "+entity)
        '〈' + entity + '〉'
    }
  }
  private def readContents(element: String)(implicit xml: Iterator[EvEvent]): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_) =>
      case EvText(text,_)  => content.append(text)
      case er: EvEntityRef => content.append(decodeEntity(er.entity))
      case EvComment(comment) if comment == " unknown entity apos; " => content.append('\'')
      case EvComment(comment) if comment.startsWith(" unknown entity") =>
        val entity = comment.substring(16, comment.length - 2)
        content.append(decodeEntity(entity))
      case EvComment(comment) =>
        logger.debug("Encountered comment: "+comment)
      case EvElemEnd(_,element) => break = true
    }
    content.toString.trim
  }
  
  def analyze(file: File, outputFile: File): Unit = {
    val fis = new FileInputStream(file)
    implicit val xml = getXMLEventReader(fis, "UTF-8")
    val id = file.getName.substring(0,file.getName.indexOf('.'))
    var version = ""
    var timePublished = ""
    var timeModified = ""
    var urgency=""
    var headline = ""
    var creditline = ""
    var byline = ""
    var department = ""
    var genre = ""
    val subjects = new ArrayBuffer[String]
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"contentCreated",_) =>
        val c = readContents("contentCreated")
        if (c.nonEmpty) timePublished = c
      case EvElemStart(_,"firstCreated",_) =>
        val c = readContents("firstCreated")
        if (c.nonEmpty) timePublished = c
      case EvElemStart(_,"contentModified",_) =>
        val c = readContents("contentModified")
        if (c.nonEmpty) timeModified = c
      case EvElemStart(_,"versionCreated",_) =>
        val c = readContents("versionCreated")
        if (c.nonEmpty) timeModified = c
      case EvElemStart(_,"urgency",_) => urgency=readContents("urgency")
      case EvElemStart(_,"located",_) =>
      case EvElemStart(_,"headline",_) => headline=readContents("headline")
      case EvElemStart(_,"creditline",_) => creditline=readContents("creditline")
      case EvElemStart(_,"by",_) => byline = readContents("by")
      case EvElemStart(_,"subject",attrs) =>
        val qcode = attrs("qcode")
        if (qcode.startsWith("sttdepartment:")) department = readContents("subject")
        else subjects += readContents("subject")
      case EvElemStart(_,"genre",attrs) =>
        val qcode = attrs("qcode")
        if (qcode.startsWith("sttversion:"))
          version = readContents("genre")
        else genre = readContents("genre")
      case EvElemStart(_,"body",_) =>
        var break = false
        val content = new StringBuilder
        content.append("<body>")
        while (xml.hasNext && !break) xml.next match {
          case EvElemStart(_, elem, attrs) => content.append("<" + elem + attrsToString(attrs) + ">")
          case EvElemEnd(_,"body") => break = true
          case EvText(text,_)  =>
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
        val contentXML = getXMLEventReader(aRegex.replaceAllIn(content.toString,"<a$1>$2</a>"))
        val stringContent = new StringBuilder
        contentXML.next // body
        var inOrderedList = false
        while (contentXML.hasNext) contentXML.next match {
          case EvElemStart(_, "h1",_) => stringContent.append("\n # ")
          case EvElemStart(_, "h2",_) => stringContent.append("\n ## ")
          case EvElemStart(_, "h3",_) => stringContent.append("\n ## ")
          case EvElemStart(_, "h4",_) => stringContent.append("\n ## ")
          case EvElemStart(_, "Company",_) =>
          case EvElemStart(_, "Person",_) =>
          case EvElemStart(_, "p",_) => stringContent.append("\n")
          case EvElemStart(_, "ul",_) =>
            inOrderedList = false
            stringContent.append('\n')
          case EvElemStart(_, "ol",_) =>
            inOrderedList = true
          case EvElemEnd(_, "li") => stringContent.append('\n')
          case EvElemStart(_, "li",_) =>
            stringContent.append(if (inOrderedList) "1." else "* ")
          case EvText(text,_)  => stringContent.append(text)
          case er: EvEntityRef => 
            stringContent.append(XhtmlEntities.entMap.get(er.entity) match {
              case Some(chr) => chr
              case _ => er.entity
            })
          case any =>
        }
        val contentS = stringContent.toString
        if (contentS.isEmpty) logger.warn("Couldn't extract any content from " + file)
        else {
          val lang = getBestLang(contentS)
          if (!lang.contains("sv") && !lang.contains("en")) {
            for (subject <- subjects) sw.synchronized { sw.writeRow(Seq(id,subject)) }
            mw.synchronized { mw.writeRow(Seq(id,version,urgency,department,genre,timePublished,timeModified,headline,creditline,byline)) }
            if (!lang.contains("fi")) logger.info("language of " + file + " detected as " + lang)
            if (!outputFile.exists) {
              val paragraphs = contentS.split("\\s*\n\n\\s*")
              val analysis = org.json4s.native.JsonMethods.pretty(org.json4s.native.JsonMethods.render(paragraphs.toList.filter(!_.isEmpty).map(la.analyze(_, fiLocale, Collections.EMPTY_LIST.asInstanceOf[java.util.List[String]], false, true, false, 0, 1).asScala.map(Extraction.decompose).toList)))
              val writer = new PrintWriter(outputFile)
              writer.write(analysis)
              writer.close()
            }
          }
        }
      case _ =>
    }
    fis.close()
    logger.info("Successfully processed "+file)
  }

  var sw: CSVWriter = _
  var mw: CSVWriter = _
      
  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val dest = opts.dest()
    mw = CSVWriter.open(dest+"/metadata.csv")
    mw.writeRow(Seq("id","version","urgency","department","genre","timePublished","timeModified","headline","creditline","byline"))
    sw = CSVWriter.open(dest+"/subjects.csv")
    createHashDirectories(dest)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toArray.flatMap(n => getFileTree(new File(n))).parStream.filter(_.getName.endsWith(".xml")).forEach(file => {
        val outputFile = new File(dest + "/" + Math.abs(file.getName.hashCode() % 10) + "/" + Math.abs(file.getName.hashCode() % 100 / 10) + "/" + file.getName + ".analysis.json")
        //if (!outputFile.exists)
          addTask(file.getPath, () => analyze(file,outputFile))
      })
    )
    mw.close()
    sw.close()
  }
}