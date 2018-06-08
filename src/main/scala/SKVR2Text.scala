import java.io.{File, PrintWriter}

import ECCOIndexer.getFileTree
import ECCOXML2Text.attrsToString
import com.bizo.mighty.csv.CSVWriter
import com.typesafe.scalalogging.LazyLogging
import org.rogach.scallop.ScallopConf

import scala.io.Source
import scala.xml.Utility
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._

object SKVR2Text extends LazyLogging {

  private def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_,_) =>
      case EvText(text) => content.append(text)
      case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
        case Some(chr) => content.append(chr)
        case _ => content.append(er.entity)
      }
      case EvComment(_) =>
      case EvElemEnd(_,"V") => break = true
      case EvElemEnd(_,_) =>
    }
    content.append('\n')
    content.toString.replaceFirst("^[0-9]* ","").replaceAll("#[0-9]*","").replaceAllLiterally("[","").replaceAllLiterally("]","")
  }

  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val directories = trailArg[List[String]](required = true)
      val dest = opt[String](required = true)
      verify()
    }
    new File(opts.dest()).mkdirs()
    val metadataCSV = CSVWriter(opts.dest()+"/"+"metadata.csv")
    opts.directories().toStream.flatMap(f => getFileTree(new File(f))
    ).filter(_.getName.endsWith(".xml")).foreach(f => {
      val s = Source.fromFile(f,"UTF-8")
      implicit val xml = new XMLEventReader(s)
      while (xml.hasNext) xml.next match {
        case EvElemStart(_, "ITEM", iattrs, _) =>
          var break = false
          var lastWasText = false
          val id = iattrs("nro").head.text
          val year = iattrs("y").head.text
          val place = iattrs("p").head.text
          val collector = iattrs("k").head.text
          metadataCSV.write(Seq(id,year,place,collector))
          val tsw: PrintWriter = new PrintWriter(opts.dest()+"/"+id+".txt")
          val msw: PrintWriter = new PrintWriter(opts.dest()+"/"+id+"_metadata.xml")
          while (xml.hasNext && !break) xml.next match {
            case EvElemStart(_, "V", _, _) => tsw.append(readContents)
            case EvElemStart(_, label, attrs, _) =>
              msw.append("<" + label + attrsToString(attrs) + ">\n")
            case EvText(text) =>
              if (text.length==1 && Utility.Escapes.escMap.contains(text(0))) msw.append(Utility.Escapes.escMap(text(0)))
              else msw.append(text)
            case er: EvEntityRef =>
              msw.append('&'); msw.append(er.entity); msw.append(';')
            case EvElemEnd(_, "ITEM") => break = true
            case EvElemEnd(_, label) =>
              msw.append("</"+label+">")
            case EvComment(_) =>
          }
          tsw.close()
          msw.close()
        case _ =>
      }
    })
    metadataCSV.close()
  }
}
