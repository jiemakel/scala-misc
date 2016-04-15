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
import javax.imageio.ImageIO
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.pull.EvEntityRef
import scala.xml.parsing.XhtmlEntities
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

object METSALTO2Text {

  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)

  /** helper function to turn XML attrs back into text */
  def attrsToString(attrs:MetaData) = {
    attrs.length match {
      case 0 => ""
      case _ => attrs.map( (m:MetaData) => " " + m.key + "='" + m.value +"'" ).reduceLeft(_+_)
    }
  }

  case class Image(parentDirectory: File, filenamePrefix: String, hpos: Int, vpos: Int, width: Int, height: Int)

  def serializeImage(outputFilename: String, page: Int, image: Image) {
    var imgFile = new File(image.parentDirectory.getPath+"/master_img/"+image.filenamePrefix+"-master.tif")
    if (!imgFile.exists) imgFile = new File(image.parentDirectory.getPath+"/preservation_img/pr-"+image.filenamePrefix+".tif")
    if (!imgFile.exists) imgFile = new File(image.parentDirectory.getPath+"/master_img/"+image.filenamePrefix+"-master.jp2")
    if (!imgFile.exists) imgFile = new File(image.parentDirectory.getPath+"/preservation_img/pr-"+image.filenamePrefix+".jp2")
    if (!imgFile.exists) imgFile = new File(image.parentDirectory.getAbsolutePath()+"/access_img/"+image.filenamePrefix+"-access.jpg")
    if (imgFile.exists) {
      val img = ImageIO.read(imgFile)
      val subImg = img.getSubimage(image.hpos,image.vpos,image.width,image.height)
      ImageIO.write(subImg,"png",new File(outputFilename+".png"))
    }
    val pw = new PrintWriter(outputFilename+"_metadata.xml")
    pw.append("<metadata>\n")
    pw.append("<pages>"+page+"</pages>\n<x>"+image.hpos+"</x>\n<y>"+image.vpos+"</y>\n<width>"+image.width+"</width>\n<height>"+image.height+"</height>\n")
    pw.append("</metadata>\n")
    pw.close()    
  }

  /** helper class to store information while parsing the METS logical hierarchy */
  case class Level(element: String, id: String, fileNamePrefix: String, content: StringBuilder, var serialize: Boolean) {
    val pages = new HashSet[Int]
  }

  /** Asynchronous method to process a particular METS file
    *
    * Churns out ~plaintexts (Markdown for titles and lists) for logical divisions defined in the METS file
    * as well as page plaintexts.
    *
    * Also splits the MODS metadata for those logical divisions into their own files
    */
  def process(metsFile: File): Future[Unit] = Future {
      val directory = metsFile.getParentFile
      println("Processing: "+directory)
      new File(directory.getPath+"/extracted").mkdir()
      val prefix = directory.getPath+"/extracted/"+directory.getName+'_'
      // first, load all page, composed, graphical and textblocks into hashes
      val textBlocks = new HashMap[String,String]
      val imageBlocks = new HashMap[String,Image]
      val composedBlocks = new HashMap[String,Buffer[String]]
      val pageBlocks = new HashMap[String,Buffer[String]]
      val seenTextBlocks = new HashSet[String]
      val seenImageBlocks = new HashSet[String]
      val seenComposedBlocks = new HashSet[String]
      // the following stores the order in which blocks were encountered in the ALTO file for later use in page-oriented serialization
      val altoBlockOrder = new HashMap[String,Int]
      var blocks = 0
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
            imageBlocks.put(imageBlock,Image(directory,file.getName.replace("-alto.xml",""),Integer.parseInt(attrs("HPOS")(0).text)*300/254,Integer.parseInt(attrs("VPOS")(0).text)*300/254,Integer.parseInt(attrs("WIDTH")(0).text)*300/254,Integer.parseInt(attrs("HEIGHT")(0).text)*300/254))
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
      // here, we start processing the actual METS file
      val s = Source.fromFile(metsFile,"UTF-8")
      val xml = new XMLEventReader(s)
      var current = Level("","","",new StringBuilder(),false)
      blocks = 0
      val blockOrder = new HashMap[String,Int]
      /** recursive function to serialize a (possibly composed) block */
      def processArea: (String) => Unit = (areaId: String) => {
        blockOrder.put(areaId,blocks)
        blocks+=1
        current.pages.add(Integer.parseInt(areaId.substring(1).replaceFirst("_.*","")))
        if (textBlocks.contains(areaId)) {
          current.content.append(textBlocks(areaId))
          seenTextBlocks.add(areaId)
        } else if (imageBlocks.contains(areaId)) {
          serializeImage(prefix+current.fileNamePrefix+"_image_"+areaId,Integer.parseInt(areaId.substring(1).replaceFirst("_.*","")),imageBlocks(areaId))
          current.serialize=true
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
      val articleMetadata: HashMap[String,String] = new HashMap
      while (xml.hasNext) xml.next match {
        // first extract article metadata into a hashmap
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
          if (!metadata.trim.isEmpty) articleMetadata.put(entity,metadata)
        case EvElemStart(_,"structMap", attrs, _) if (attrs("TYPE")(0).text=="LOGICAL") =>
          // process the logical structure hierarchically
          val counts = new HashMap[String,Int]
          var hierarchy = Seq(current)
          while (xml.hasNext) xml.next match {
            case EvElemStart(_,"div", attrs, _) =>
              val divType =  attrs("TYPE")(0).text.toLowerCase
              divType match { //handle various hierarchy levels differently. First, some Markdown
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
                case "metae_serial" | "data" | "statement" | "metae_monograph" | "paragraph" | "image" | "main" | "caption" | "textblock" | "item_caption" | "heading" | "overline" | "footnote" | "content" | "body_content" | "text" | "newspaper" | "volume" | "issue" | "body" =>
                  // these are just ignored completely
                  current = Level(divType,"",current.fileNamePrefix,current.content,false)
                case _ => // everything else results (eventually) in a new file for the logical structure (e.g. article, section, front matter, table_of_contents, ...)
                  val typeCount = divType match {
                    case "front" => ""
                    case "back" => ""
                    case _ if attrs("DMDID")!=null => "_"+attrs("DMDID")(0).text.replaceFirst("^[^\\d]*","")
                    case _ =>
                      val n = counts.get(divType).getOrElse(0)+1
                      counts.put(divType,n)
                      "_"+n
                  }
                  current = Level(divType,if (attrs("DMDID")!=null) attrs("DMDID")(0).text else "",(if (current.fileNamePrefix!="") current.fileNamePrefix+"_" else "")+divType+typeCount, new StringBuilder(),true)
              }
              hierarchy = hierarchy :+ current
            case EvElemStart(_,"area",attrs,_) =>
              processArea(attrs("BEGIN")(0).text)
            case EvElemEnd(_,"div") =>
              val hc = hierarchy.last
              hierarchy = hierarchy.dropRight(1)
              if (hc.element=="paragraph" || hc.element=="textblock") hc.content.append("\n") //paragraphs and textblocks result in plaintext paragraphs
              if (hc.serialize) {
                if (!hc.content.mkString.trim.isEmpty) {
                  val pw = new PrintWriter(new File(prefix+hc.fileNamePrefix+".txt"))
                  pw.append(hc.content)
                  pw.close()
                }
              }
              if (hc.serialize || articleMetadata.contains(hc.id)) {
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
      // sometimes (often), all blocks defined in the ALTO don't appear in the logical structure. Those are serialized here, first starting with unreferenced composed blocks
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
      // here, we then serialize unreferenced text blocks by page
      val unclaimedPageBlocks = new HashMap[Int,Buffer[String]]
      for (textBlock <- textBlocks.keys;if !seenTextBlocks.contains(textBlock))
        unclaimedPageBlocks.getOrElseUpdate(Integer.parseInt(textBlock.substring(1).replaceFirst("_.*","")),new ArrayBuffer[String]) += textBlock
      for (page <- unclaimedPageBlocks.keys) {
        var pw = new PrintWriter(new File(prefix+"other_texts_page_"+page+"_metadata.xml"))
        pw.println("<metadata>\n<blocks>"+unclaimedPageBlocks(page).toSeq.sorted.mkString(",")+"</blocks>\n</metadata>")
        pw.close()
        pw = new PrintWriter(new File(prefix+"other_texts_page_"+page+".txt"))
        for (text <- unclaimedPageBlocks(page).toSeq.sorted.map(textBlocks(_))) pw.println(text)
        pw.close()
      }
      // finally, we serialize also page plaintexts. The blocks are organized sorted first by how they appear in the logical order, then by inserting any blocks not appearing in the logical order after their ALTO predecessor.
      for (page <- pageBlocks.keys) {
        current = Level("","","",new StringBuilder(),false)
        var (refBlocks,unrefBlocks) = pageBlocks(page).partition(blockOrder.contains(_))
        refBlocks = refBlocks.sortWith(blockOrder(_)<blockOrder(_))
        if (refBlocks.isEmpty) refBlocks = unrefBlocks.sortWith(altoBlockOrder(_)<altoBlockOrder(_))
        else unrefBlocks.sortWith(altoBlockOrder(_)<altoBlockOrder(_)).foreach( b => {
          val myBlockOrder = altoBlockOrder(b)-1
          var i = 0
          while (i<refBlocks.length-1 && myBlockOrder!=altoBlockOrder(refBlocks(i))) i+=1
          refBlocks.insert(i+1,b)
        })
        pageBlocks.put(page,refBlocks)
        processArea(page)
        var pw = new PrintWriter(new File(prefix+"page_"+page+".txt"))
        pw.append(current.content)
        pw.close()
        pw = new PrintWriter(new File(prefix+"page_"+page+"_metadata.xml"))
        pw.println("<metadata>\n<blocks>"+pageBlocks(page).toSeq.sorted.mkString(",")+"</blocks>\n</metadata>")
        pw.close()
      }
      s.close()
  }

  def main(args: Array[String]): Unit = {
    val f = Future.sequence(for (dir<-args.toSeq;metsFile <- getFileTree(new File(dir)); if (metsFile.getName().endsWith("mets.xml"))) yield {
      val f = process(metsFile)
      f.onFailure { case t => println("An error has occured processing "+metsFile.getParentFile+": " + t.printStackTrace()) }
      f.onSuccess { case _ => println("Processed: "+metsFile.getParentFile) }
      f
    })
    f.onFailure { case t => println("Processing of at least one file resulted in an error.") }
    f.onSuccess { case _ => println("Successfully processed all files.") }
    Await.result(f, Duration.Inf)
  }
}
