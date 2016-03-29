import com.bizo.mighty.csv.CSVReader
import java.net.URLEncoder
import org.apache.jena.riot.RDFFormat
import org.apache.jena.riot.RDFDataMgr
import java.io.FileOutputStream
import com.bizo.mighty.csv.CSVDictReader
import com.bizo.mighty.csv.CSVReaderSettings
import scala.io.Source
import scala.xml.pull.XMLEventReader
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvText
import scala.xml.pull.EvElemEnd
import scala.xml.MetaData
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import java.io.PrintWriter
import java.io.File
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.pull.EvEntityRef
import scala.xml.parsing.XhtmlEntities

object METSALTO2Text {
  
  def get(key: String)(implicit attrs: MetaData): Option[String] = {
    if (attrs(key)!=null && attrs(key)(0).text!="") Some(attrs(key)(0).text.trim)
    else None
  }
  
  def attrsToString(attrs:MetaData) = {
    attrs.length match {
      case 0 => ""
      case _ => attrs.map( (m:MetaData) => " " + m.key + "='" + m.value +"'" ).reduceLeft(_+_)
    }
  }
  
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree) 
      else Stream.empty)
      
  def process(metsFile: File): Future[Unit] = Future {
      val directory = metsFile.getParentFile
      println("Processing: "+directory)
      new File(directory.getPath+"/extracted").mkdir()
      val prefix = directory.getPath+"/extracted/"+directory.getName+'_'
      val textBlocks = new HashMap[String,String]
      val seenTextBlocks = new HashSet[String]
      val composedBlocks = new HashMap[String,HashSet[String]]
      val seenComposedBlocks = new HashSet[String]
      var break = false
      for (file <- new File(directory.getPath+"/alto").listFiles) {
        val s = Source.fromFile(file,"UTF-8")
        val xml = new XMLEventReader(s)
        var composedBlock: Option[HashSet[String]] = None
        while (xml.hasNext) xml.next match {
          case EvElemStart(_,"ComposedBlock",attrs,_) => 
            composedBlock = Some(new HashSet[String])
            composedBlocks.put(attrs("ID")(0).text,composedBlock.get)
          case EvElemEnd(_,"ComposedBlock") =>
            composedBlock = None 
          case EvElemStart(_,"TextBlock",attrs,_) => 
            var text = ""
            val textBlock = attrs("ID")(0).text 
            composedBlock.foreach(_+=textBlock)
            break = false
            while (xml.hasNext && !break) xml.next match {
              case EvElemStart(_,"String",attrs,_) if (attrs("SUBS_TYPE")!=null && attrs("SUBS_TYPE")(0).text=="HypPart1") => text+=attrs("SUBS_CONTENT")(0).text
              case EvElemStart(_,"String",attrs,_) if (attrs("SUBS_TYPE")!=null && attrs("SUBS_TYPE")(0).text=="HypPart2") => 
              case EvElemStart(_,"String",attrs,_) => text+=attrs("CONTENT")(0).text
              case EvElemStart(_,"SP",attrs,_) => text+=" "
              case EvElemEnd(_,"TextLine") => text+="\n"
              case EvElemEnd(_,"TextBlock") => break = true
              case _ =>
            }
            textBlocks.put(textBlock,text)
          case _ =>
        }
        s.close()
      }
      val s = Source.fromFile(metsFile,"UTF-8")
      val xml = new XMLEventReader(s)
      val articleMetadata: HashMap[String,String] = new HashMap
      var advertisements = 0
      var titleSections = 0
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"dmdSec", attrs, _) if attrs("ID")(0).text.startsWith("MODSMD_ARTICLE") || attrs("ID")(0).text.startsWith("MODSMD_CHAP") || attrs("ID")(0).text.startsWith("MODSMD_LIST") =>
          val article = attrs("ID")(0).text
          break = false
          while (xml.hasNext && !break) xml.next match {
            case EvElemStart(_,"mods",_,_) => break = true
            case _ => 
          }
          break = false
          var indent = ""
          var metadata = ""
          while (xml.hasNext && !break) xml.next match {
            case EvElemStart(pre, label, attrs, scope) =>
              metadata+= indent + "<" + label + attrsToString(attrs) + ">\n"
              indent += "  "
            case EvText(text) => if (text.trim!="") metadata += indent + text.trim + "\n"
            case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
              case Some(chr) => metadata += chr
              case _ => metadata += er.entity
            }
            case EvElemEnd(_,"mods") => break = true
            case EvElemEnd(_, label) => 
              indent = indent.substring(0,indent.length-2)
              metadata+=indent + "</"+label+">\n" 
          }
          articleMetadata.put(article,metadata)
        case EvElemStart(_,"div", attrs, _) if (attrs("TYPE")(0).text=="ARTICLE" || attrs("TYPE")(0).text=="TABLE_OF_CONTENTS" || attrs("TYPE")(0).text=="LIST" || attrs("TYPE")(0).text=="CONTRIBUTION" || attrs("TYPE")(0).text=="CHAPTER" || attrs("TYPE")(0).text=="ILLUSTRATION" || attrs("TYPE")(0).text=="ADVERTISEMENT" || attrs("TYPE")(0).text=="TABLE" || attrs("TYPE")(0).text=="TITLE_SECTION") => 
          val atype = attrs("TYPE")(0).text.toLowerCase
          val articleNumber = atype match {
            case "advertisement" => 
              advertisements += 1          
              ""+advertisements
            case "title_section" =>
              titleSections += 1          
              ""+titleSections
            case "article" => attrs("DMDID")(0).text.substring(14)
            case "chapter" | "list" | "table" | "illustration" | "contribution" | "table_of_contents" => attrs("DMDID")(0).text.substring(11)
          }
          val pw = new PrintWriter(new File(prefix+atype+'_'+articleNumber+".txt"))
          var depth = 1
          val pages = new HashSet[Int]
          while (xml.hasNext && depth!=0) xml.next match {
            case EvElemStart(_,"div",attrs,_) => 
              depth += 1
              attrs("TYPE")(0).text match {
                case "TITLE" => pw.append("# ")
                case "AUTHOR" => pw.append("## ")
                case "OVERLINE" => 
                  val breakDepth = depth - 1
                  while (xml.hasNext && depth!=breakDepth) xml.next match {
                    case EvElemEnd(_,"div") => depth -= 1
                    case _ => 
                  }
                case _ => 
              }
            case EvElemStart(_,"area",attrs,_) => {
              val areaId = attrs("BEGIN")(0).text
              pages.add(Integer.parseInt(areaId.substring(1,areaId.indexOf('_'))))
              if (textBlocks.contains(areaId)) {
                pw.println(textBlocks(areaId))
                seenTextBlocks.add(areaId)
              } else {
                for (block <- composedBlocks(areaId)) {
                  pages.add(Integer.parseInt(block.substring(1,block.indexOf('_'))))
                  pw.println(textBlocks(block))
                  seenTextBlocks.add(block)
                }
                seenComposedBlocks.add(areaId)
              }
            }
            case EvElemEnd(_,"div") => depth -= 1
            case _ => 
          }
          pw.close()
          if (atype == "article" && articleMetadata.contains(attrs("DMDID")(0).text)) {
            val pw = new PrintWriter(new File(prefix+atype+"_"+articleNumber+"_metadata.xml"))
            pw.append("<metadata>\n")
            pw.append(articleMetadata(attrs("DMDID")(0).text))
            pw.append("<pages>"+pages.toSeq.sorted.mkString(",")+"</pages>\n")
            pw.append("</metadata>\n")
            pw.close()
          } else {
            val pw = new PrintWriter(new File(prefix+atype+"_"+articleNumber+"_metadata.xml"))
            pw.append("<metadata>\n")
            pw.append("<pages>"+pages.toSeq.sorted.mkString(",")+"</pages>\n")
            pw.append("</metadata>\n")
            pw.close()
          }
        case _ =>  
      }
      var blocks = 0
      seenComposedBlocks.foreach(b=>composedBlocks.remove(b))
      for (block <- composedBlocks.values) {
        val pages = new HashSet[Int]
        blocks += 1
        var pw = new PrintWriter(new File(prefix+"other_texts_"+blocks+".txt"))
        for (textBlock <- block) {
          pages.add(Integer.parseInt(textBlock.substring(1,textBlock.indexOf('_'))))
          pw.println(textBlocks.remove(textBlock).get)
        }
        pw.close()
        pw = new PrintWriter(new File(prefix+"other_texts_"+blocks+"_metadata.xml"))
        pw.println("<metadata>\n<blocks>"+block.toSeq.sorted.mkString(",")+"</blocks>\n<pages>"+pages.toSeq.sorted.mkString(",")+"</pages>\n</metadata>")
        pw.close()
      }
      seenTextBlocks.foreach(b=>textBlocks.remove(b))
      if (!textBlocks.isEmpty) {
        val pageBlocks = new HashMap[Int,HashSet[String]]
        for (textBlock <- textBlocks.keys) pageBlocks.getOrElseUpdate(Integer.parseInt(textBlock.substring(1,textBlock.indexOf('_'))),new HashSet[String]).add(textBlock)
        for (page <- pageBlocks.keys) {
          var pw = new PrintWriter(new File(prefix+"other_texts_page_"+page+"_metadata.xml"))
          pw.println("<metadata>\n<blocks>"+pageBlocks(page).toSeq.sorted.mkString(",")+"</blocks>\n</metadata>")
          pw.close()
          pw = new PrintWriter(new File(prefix+"other_texts_page_"+page+".txt"))
          for (text <- pageBlocks(page).toSeq.sorted.map(textBlocks(_))) pw.println(text)
          pw.close()
        }
      }
      s.close()
  }
      
  def main(args: Array[String]): Unit = {
    val f = Future.sequence(for (dir<-args.toSeq;metsFile <- getFileTree(new File(dir)); if (metsFile.getName().endsWith("_mets.xml"))) yield {
      val f = process(metsFile)
      f.onFailure { case t => println("An error has occured processing "+metsFile.getParentFile+": " + t.getMessage) }
      f.onSuccess { case _ => println("Processed: "+metsFile.getParentFile) }
      f
    })
    f.onFailure { case t => println("Processing aborted due to an error.") }
    f.onSuccess { case _ => println("Successfully processed all files.") }
    Await.result(f, Duration.Inf)
  }
}
