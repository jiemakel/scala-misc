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
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

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
      
  case class Image(hpos: Int, vpos: Int, width: Int, height: Int)
  
  def serializeImage(filename: String, image: Image) {
    //FIXME: add image serialization
  }
  
  case class Level(element: String, id: String, fileNamePrefix: String, content: StringBuilder, serialize: Boolean) {
    val pages = new HashSet[Int]
  }
  
  def process(metsFile: File): Future[Unit] = Future {
      val directory = metsFile.getParentFile
      println("Processing: "+directory)
      new File(directory.getPath+"/extracted").mkdir()
      val prefix = directory.getPath+"/extracted/"+directory.getName+'_'
      val textBlocks = new HashMap[String,String]
      val imageBlocks = new HashMap[String,Image]
      val composedBlocks = new HashMap[String,Buffer[String]]
      val pageBlocks = new HashMap[String,Buffer[String]]
      val seenTextBlocks = new HashSet[String]
      val seenImageBlocks = new HashSet[String]
      val seenComposedBlocks = new HashSet[String]
      var blocks = 0
      val blockOrder = new HashMap[String,Int]
      val altoBlockOrder = new HashMap[String,Int]
      var break = false
      for (file <- new File(directory.getPath+"/alto").listFiles) {
        val s = Source.fromFile(file,"UTF-8")
        val xml = new XMLEventReader(s)
        var composedBlock: Option[Buffer[String]] = None
        var page: Option[Buffer[String]] = None
        while (xml.hasNext) xml.next match {
          case EvElemStart(_,"Page",attrs,_) =>
            page = Some(new ArrayBuffer[String])
            pageBlocks.put(attrs("ID")(0).text,page.get)
            altoBlockOrder.put(attrs("ID")(0).text,blocks)
            blocks+=1
          case EvElemStart(_,"ComposedBlock",attrs,_) => 
            val composedBlockId = attrs("ID")(0).text
            composedBlock = Some(new ArrayBuffer[String])
            page.foreach(_+=composedBlockId)
            composedBlocks.put(composedBlockId,composedBlock.get)
            altoBlockOrder.put(composedBlockId,blocks)
            blocks+=1
          case EvElemStart(_,"GraphicalElement",attrs,_) =>
            val imageBlock = attrs("ID")(0).text
            composedBlock.orElse(page).foreach(_+=imageBlock)
            imageBlocks.put(imageBlock,Image(Integer.parseInt(attrs("HPOS")(0).text),Integer.parseInt(attrs("VPOS")(0).text),Integer.parseInt(attrs("WIDTH")(0).text),Integer.parseInt(attrs("HEIGHT")(0).text)))
            altoBlockOrder.put(imageBlock,blocks)
            blocks+=1
          case EvElemEnd(_,"ComposedBlock") =>
            composedBlock = None 
          case EvElemEnd(_,"Page") =>
            page = None 
          case EvElemStart(_,"TextBlock",attrs,_) => 
            var text = ""
            val textBlock = attrs("ID")(0).text 
            composedBlock.orElse(page).foreach(_+=textBlock)
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
            altoBlockOrder.put(textBlock,blocks)
            blocks+=1
          case _ =>
        }
        s.close()
      }
      val s = Source.fromFile(metsFile,"UTF-8")
      val xml = new XMLEventReader(s)
      val articleMetadata: HashMap[String,String] = new HashMap
      var current = Level("","","",new StringBuilder(),false)
      blocks = 0
      def processArea: (String) => Unit = (areaId: String) => {
        blockOrder.put(areaId,blocks)
        blocks+=1
        current.pages.add(Integer.parseInt(areaId.substring(1).replaceFirst("_.*","")))
        if (textBlocks.contains(areaId)) {
          current.content.append(textBlocks(areaId))
          seenTextBlocks.add(areaId)
        } else if (imageBlocks.contains(areaId)) {
          serializeImage(prefix+current.fileNamePrefix+"_image_"+areaId,imageBlocks(areaId))
          seenImageBlocks.add(areaId)
        } else if (composedBlocks.contains(areaId)) {
          for (block <- composedBlocks(areaId)) {
            processArea(block)
            current.content.append("\n")
          }
          seenComposedBlocks.add(areaId)
        } else for (block <- pageBlocks(areaId)) {
          processArea(block)
          current.content.append("\n")
        }
      }
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"dmdSec", attrs, _) =>
          val entity = attrs("ID")(0).text
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
          articleMetadata.put(entity,metadata)
        case EvElemStart(_,"structMap", attrs, _) if (attrs("TYPE")(0).text=="LOGICAL") => 
          val counts = new HashMap[String,Int]
          var hierarchy = Seq(current)
          while (xml.hasNext) xml.next match {
            case EvElemStart(_,"div", attrs, _) =>
              val divType =  attrs("TYPE")(0).text.toLowerCase
              divType match {
                case "title" | "headline" | "title_of_work" | "continuation_headline" => 
                  current = Level(divType,"",current.fileNamePrefix,current.content,false)
                  current.content.append("# ")
                case "heading_text" | "subtitle" | "running_title" => 
                  current = Level(divType,"",current.fileNamePrefix,current.content,false)
                  current.content.append("## ")
                case "author" => 
                  current = Level(divType,"",current.fileNamePrefix,current.content,false)
                  current.content.append("### ")
                case "item" => 
                  current = Level(divType,"",current.fileNamePrefix,current.content,false)
                  current.content.append(" * ")
                case "metae_serial" | "data" | "statement" | "paragraph" | "image" | "main" | "caption" | "textblock" | "item_caption" | "heading" | "overline" | "footnote" | "content" | "body_content" | "text" | "newspaper" | "volume" | "issue" | "body" =>
                  current = Level(divType,"",current.fileNamePrefix,current.content,false)
                case _ => 
                  val articleNumber = divType match {
                    case "front" => ""
                    case "back" => ""
                    case _ if attrs("DMDID")!=null => "_"+attrs("DMDID")(0).text.replaceFirst("^[^\\d]*","")
                    case _ =>
                      val n = counts.get(divType).getOrElse(0)+1
                      counts.put(divType,n)
                      "_"+n
                  }
                  current = Level(divType,if (attrs("DMDID")!=null) attrs("DMDID")(0).text else "",(if (current.fileNamePrefix!="") current.fileNamePrefix+"_" else "")+divType+articleNumber, new StringBuilder(),true)
              }
              hierarchy = hierarchy :+ current 
            case EvElemStart(_,"area",attrs,_) => 
              processArea(attrs("BEGIN")(0).text)
            case EvElemEnd(_,"div") =>
              val hc = hierarchy.last
              hierarchy = hierarchy.dropRight(1)
              if (hc.element=="paragraph" || hc.element=="textblock") hc.content.append("\n")
              if (hc.serialize) {
                if (!hc.content.mkString.trim.isEmpty) {
                  val pw = new PrintWriter(new File(prefix+hc.fileNamePrefix+".txt"))
                  pw.append(hc.content)
                  pw.close()
                }
                val pw = new PrintWriter(new File(prefix+hc.fileNamePrefix+"_metadata.xml"))
                pw.append("<metadata>\n")
                if (articleMetadata.contains(hc.id))
                  pw.append(articleMetadata(hc.id))
                pw.append("<pages>"+hc.pages.toSeq.sorted.mkString(",")+"</pages>\n")
                pw.append("</metadata>\n")
                pw.close()
              }
              current=hierarchy.last
              current.pages++=hc.pages
            case _ =>  
          }
        case _ =>  
      }
      for (block <- composedBlocks.keys;if !seenComposedBlocks.contains(block)) {
        current = Level("","","",new StringBuilder(),false)
        processArea(block)
        var pw = new PrintWriter(new File(prefix+"other_texts_"+block+".txt"))
        pw.append(current.content)
        pw.close()
        pw = new PrintWriter(new File(prefix+"other_texts_"+block+"_metadata.xml"))
        pw.println("<metadata>\n<blocks>"+composedBlocks(block).toSeq.sorted.mkString(",")+"</blocks>\n<pages>"+current.pages.toSeq.sorted.mkString(",")+"</pages>\n</metadata>")
        pw.close()
      }
      for (page <- pageBlocks.keys) {
        current = Level("","","",new StringBuilder(),false)
        pageBlocks.put(page,pageBlocks(page).sortWith((a,b) => if (blockOrder.contains(a) && blockOrder.contains(b)) blockOrder(a)<blockOrder(b) else altoBlockOrder(a)<altoBlockOrder(b)))
        processArea(page)
        var pw = new PrintWriter(new File(prefix+"page_"+page+".txt"))
        pw.append(current.content)
        pw.close()
        pw = new PrintWriter(new File(prefix+"page_"+page+"_metadata.xml"))
        pw.println("<metadata>\n<blocks>"+pageBlocks(page).toSeq.sorted.mkString(",")+"</blocks>\n</metadata>")
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
      f.onFailure { case t => println("An error has occured processing "+metsFile.getParentFile+": " + t.printStackTrace()) }
      f.onSuccess { case _ => println("Processed: "+metsFile.getParentFile) }
      f
    })
    f.onFailure { case t => println("Processing aborted due to an error.") }
    f.onSuccess { case _ => println("Successfully processed all files.") }
    Await.result(f, Duration.Inf)
  }
}
