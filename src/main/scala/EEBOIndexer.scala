import java.io.File

import scala.io.Source
import scala.xml.pull.{EvElemEnd, EvElemStart, XMLEventReader}


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