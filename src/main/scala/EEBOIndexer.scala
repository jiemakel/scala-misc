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
import scala.util.Success
import scala.util.Failure


object EEBOIndexer extends OctavoIndexer {

  def index(file: File): Unit = {
   val xml = new XMLEventReader(Source.fromFile(file, "UTF-8"))
    var skip = false
    val content = new StringBuilder()
    while (xml.hasNext) xml.next match {
      case EvElemStart(_, "DIV1", attrs,_) => // TYPE, LANG, N
      case EvElemEnd(_, "DIV1") =>
      case EvElemStart(_, "DIV2", attrs,_) =>
      case EvElemEnd(_, "DIV2") =>        
      case EvElemStart(_, "DIV3", attrs,_) =>
      case EvElemEnd(_, "DIV3") =>        
      case EvElemStart(_, "DIV4", attrs,_) =>
      case EvElemEnd(_, "DIV4") =>        
      case EvElemStart(_, "DIV5", attrs,_) =>
      case EvElemEnd(_, "DIV5") =>        
      case EvElemStart(_, "DIV6", attrs,_) =>
      case EvElemEnd(_, "DIV6") =>        
      case EvElemStart(_, "DIV7", attrs,_) =>
      case EvElemEnd(_, "DIV7") =>        
      case EvElemStart(_, "HEAD",_,_) => content.append("\n # ") // DEPENDS on DIV-number
      case EvElemStart(_, "ARGUMENT",_,_) => content.append("\n ## ")
      case EvElemStart(_, "FRONT",_,_) =>
      case EvElemEnd(_, "FRONT") =>
      case EvElemEnd(_, "BODY") =>
      case EvElemEnd(_, "BACK") =>
      case EvElemEnd(_, "NOTE") => //PLACE
      case EvElemStart(_, "P",_,_) =>
      case EvElemEnd(_, "P") => 
      case EvElemEnd(_, "LETTER") => //quoted letter
      case EvElemEnd(_, "Q") => // inline quote
      case EvElemStart(_, "LG",_,_) => //line group of poems (L), not always inside LG
      case EvElemStart(_, "LIST",_,_) => //list of ITEMs
    }
    logger.info("Successfully processed "+file)
  }
  
  def main(args: Array[String]): Unit = {
    val opts = new OctavoOpts(args)
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".xml")).foreach(file => addTask(file.getPath, () => index(file)))
    )
  }
}