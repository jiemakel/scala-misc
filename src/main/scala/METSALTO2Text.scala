import java.io.{File, FileInputStream, PrintWriter}
import java.net.URL

import javax.imageio.ImageIO
import org.rogach.scallop.ScallopConf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.MetaData
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._

object METSALTO2Text extends ParallelProcessor {

  /** helper function to turn XML attrs back into text */
  def attrsToString(attrs:MetaData) = {
    attrs.length match {
      case 0 => ""
      case _ => attrs.map( (m:MetaData) => " " + m.key + "='" + m.value +"'" ).reduceLeft(_+_)
    }
  }

  case class Image(parentDirectory: File, filenamePrefix: String, hpos: Int, vpos: Int, width: Int, height: Int)

  def serializeImage(outputFilename: String, page: Int, image: Image, fetchImages: Boolean) {
    if (fetchImages) {
      var imgFile = new File(image.parentDirectory.getPath + "/master_img/" + image.filenamePrefix + "-master.tif")
      if (!imgFile.exists) imgFile = new File(image.parentDirectory.getPath + "/preservation_img/pr-" + image.filenamePrefix + ".tif")
      if (!imgFile.exists) imgFile = new File(image.parentDirectory.getPath + "/master_img/" + image.filenamePrefix + "-master.jp2")
      if (!imgFile.exists) imgFile = new File(image.parentDirectory.getPath + "/preservation_img/pr-" + image.filenamePrefix + ".jp2")
      if (!imgFile.exists) imgFile = new File(image.parentDirectory.getAbsolutePath + "/access_img/" + image.filenamePrefix + "-access.jpg")
      val istream = if (imgFile.exists) new FileInputStream(imgFile) else new URL("").openStream() //FIXME https://digi.kansalliskirjasto.fi/sanomalehti/binding/414985/image/2
      val img = ImageIO.read(istream)
      istream.close()
      if (img != null) {
        val subImg = img.getSubimage(image.hpos, image.vpos, image.width, image.height)
        ImageIO.write(subImg, "png", new File(outputFilename + ".png"))
      } else logger.error(s"Image $image / $outputFilename came out null")
    }
    val pw = new PrintWriter(outputFilename+"_metadata.xml")
    pw.append("<metadata>\n")
    pw.append("<pages>"+page+"</pages>\n<x>"+image.hpos+"</x>\n<y>"+image.vpos+"</y>\n<width>"+image.width+"</width>\n<height>"+image.height+"</height>\n")
    pw.append("</metadata>\n")
    pw.close()    
  }

  /** helper class to store information while parsing the METS logical hierarchy */
  case class Level(element: String, id: String, fileNamePrefix: String, content: StringBuilder, var serialize: Boolean) {
    val pages = new mutable.HashSet[Int]
  }

  /** Asynchronous method to process a particular METS file
    *
    * Churns out ~plaintexts (Markdown for titles and lists) for logical divisions defined in the METS file
    * as well as page plaintexts.
    *
    * Also splits the MODS metadata for those logical divisions into their own files
    */
  def process(metsFile: File, dest: String, prefixLength: Int, omitStructure: Boolean, fetchImages: Boolean){
      val directory = metsFile.getParentFile
      val destdirectory = dest+directory.getAbsolutePath.substring(prefixLength) + "/"
      logger.info(s"Processing: $directory into $destdirectory")
      new File(destdirectory).mkdirs()
      //val prefix = directory.getPath+"/extracted/"+directory.getName+'_'
      // first, load all page, composed, graphical and textblocks into hashes
      val textBlocks = new mutable.HashMap[String,String]
      val imageBlocks = new mutable.HashMap[String,Image]
      val composedBlocks = new mutable.HashMap[String,mutable.Buffer[String]]
      val pageBlocks = new mutable.HashMap[String,mutable.Buffer[String]]
      val seenTextBlocks = new mutable.HashSet[String]
      val seenImageBlocks = new mutable.HashSet[String]
      val seenComposedBlocks = new mutable.HashSet[String]
      // the following stores the VPOS in which blocks were encountered in the ALTO file for later use in page-oriented serialization
      val altoBlockOrder = new mutable.HashMap[String,Int]
      val iw = new PrintWriter(new File(destdirectory+"issue.txt"))
      var break = false
      for (file <- new File(directory.getPath).listFiles;if file.getName.startsWith("alto_")) {
        val s = Source.fromFile(file,"UTF-8")
        val xml = new XMLEventReader(s)
        var composedBlock: Option[mutable.Buffer[String]] = None
        var page: Option[mutable.Buffer[String]] = None
        while (xml.hasNext) xml.next match {
          case EvElemStart(_,"Page",attrs,_) =>
            page = Some(new ArrayBuffer[String])
            pageBlocks.put(attrs("ID").head.text,page.get)
            altoBlockOrder.put(attrs("ID").head.text,0)
          case EvElemStart(_,"ComposedBlock",attrs,_) =>
            val composedBlockId = attrs("ID").head.text
            composedBlock = Some(new ArrayBuffer[String])
            page.foreach(_+=composedBlockId)
            composedBlocks.put(composedBlockId,composedBlock.get)
            altoBlockOrder.put(composedBlockId,attrs("VPOS").head.text.toInt)
          case EvElemStart(_,"GraphicalElement",attrs,_) =>
            val imageBlock = attrs("ID").head.text
            composedBlock.orElse(page).foreach(_+=imageBlock)
            imageBlocks.put(imageBlock,Image(directory,file.getName.replace("alto_","").replace(".xml",""),Integer.parseInt(attrs("HPOS").head.text)*30/254,Integer.parseInt(attrs("VPOS").head.text)*300/254,Integer.parseInt(attrs("WIDTH").head.text)*300/254,Integer.parseInt(attrs("HEIGHT").head.text)*300/254))
            altoBlockOrder.put(imageBlock,attrs("VPOS").head.text.toInt)
          case EvElemEnd(_,"ComposedBlock") =>
            composedBlock = None
          case EvElemEnd(_,"Page") =>
            page = None
          case EvElemStart(_,"TextBlock",attrs,_) =>
            var text = ""
            val textBlock = attrs("ID").head.text
            composedBlock.orElse(page).foreach(_+=textBlock)
            break = false
            while (xml.hasNext && !break) xml.next match {
              case EvElemStart(_,"String",sattrs,_) if sattrs("SUBS_TYPE")!=null && sattrs("SUBS_TYPE").head.text=="HypPart1" => text+=sattrs("SUBS_CONTENT").head.text
              case EvElemStart(_,"String",sattrs,_) if sattrs("SUBS_TYPE")!=null && sattrs("SUBS_TYPE").head.text=="HypPart2" =>
              case EvElemStart(_,"String",sattrs,_) => text+=sattrs("CONTENT").head.text
              case EvElemStart(_,"SP",_,_) => text+=" "
              case EvElemEnd(_,"TextLine") => text+="\n"
              case EvElemEnd(_,"TextBlock") => break = true
              case _ =>
            }
            textBlocks.put(textBlock,text)
            altoBlockOrder.put(textBlock,attrs("VPOS").head.text.toInt)
          case _ =>
        }
        s.close()
      }
      // here, we start processing the actual METS file
      val s = Source.fromFile(metsFile,"UTF-8")
      val xml = new XMLEventReader(s)
      var current = Level("","","",new StringBuilder(),serialize = false)
      var blocks = 0
      val blockOrder = new mutable.HashMap[String,Int]
      /** recursive function to serialize a (possibly composed) block */
      def processArea: String => Unit = (areaId: String) => {
        blockOrder.put(areaId,blocks)
        blocks+=1
        current.pages.add(Integer.parseInt(areaId.substring(1).replaceFirst("_.*","")))
        if (textBlocks.contains(areaId)) {
          current.content.append(textBlocks(areaId))
          seenTextBlocks.add(areaId)
        } else if (imageBlocks.contains(areaId)) {
          serializeImage(destdirectory+current.fileNamePrefix+"_image_"+areaId,Integer.parseInt(areaId.substring(1).replaceFirst("_.*","")),imageBlocks(areaId), fetchImages)
          current.serialize=true
          seenImageBlocks.add(areaId)
        } else if (composedBlocks.contains(areaId)) {
          for (block <- composedBlocks(areaId)) {
            processArea(block)
            current.content.append("\n")
          }
          seenComposedBlocks.add(areaId)
        } else if (pageBlocks.contains(areaId)) for (block <- pageBlocks(areaId)) {
          processArea(block)
          current.content.append("\n")
        } else logger.error(s"Unknown block $areaId in $metsFile.")
      }
      val articleMetadata: mutable.HashMap[String,String] = new mutable.HashMap
      while (xml.hasNext) xml.next match {
        // first extract article metadata into a hashmap
        case EvElemStart(_,"dmdSec", attrs, _) =>
          val entity = attrs("ID").head.text
          break = false
          while (xml.hasNext && !break) xml.next match {
            case EvElemStart(_,"mods",_,_) => break = true
            case _ =>
          }
          break = false
          var indent = ""
          var metadata = ""
          while (xml.hasNext && !break) xml.next match {
            case EvElemStart(_, label, sattrs, _) =>
              metadata+= indent + "<" + label + attrsToString(sattrs) + ">\n"
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
        case EvElemStart(_,"structMap", attrs, _) if attrs("TYPE").head.text=="LOGICAL" =>
          // process the logical structure hierarchically
          val counts = new mutable.HashMap[String,Int]
          var hierarchy = Seq(current)
          while (xml.hasNext) xml.next match {
            case EvElemStart(_,"div", sattrs, _) =>
              val divType =  sattrs("TYPE").head.text.toLowerCase
              divType match { //handle various hierarchy levels differently. First, some Markdown
                case "title" | "headline" | "title_of_work" | "continuation_headline" =>
                  current = Level(divType,"",current.fileNamePrefix,current.content,serialize = false)
                  if (!omitStructure) current.content.append("# ")
                case "heading_text" | "subtitle" | "running_title" =>
                  current = Level(divType,"",current.fileNamePrefix,current.content,serialize = false)
                  if (!omitStructure) current.content.append("## ")
                case "author" =>
                  current = Level(divType,"",current.fileNamePrefix,current.content,serialize = false)
                  if (!omitStructure) current.content.append("### ")
                case "item" =>
                  current = Level(divType,"",current.fileNamePrefix,current.content,serialize = false)
                  if (!omitStructure) current.content.append(" * ")
                case "metae_serial" | "data" | "statement" | "metae_monograph" | "paragraph" | "image" | "main" | "caption" | "textblock" | "item_caption" | "heading" | "overline" | "footnote" | "content" | "body_content" | "text" | "newspaper" | "volume" | "issue" | "body" =>
                  // these are just ignored completely
                  current = Level(divType,"",current.fileNamePrefix,current.content,serialize = false)
                case _ => // everything else results (eventually) in a new file for the logical structure (e.g. article, section, front matter, table_of_contents, ...)
                  val typeCount = divType match {
                    case "front" => ""
                    case "back" => ""
                    case _ if attrs("DMDID")!=null => "_"+attrs("DMDID").head.text.replaceFirst("^[^\\d]*","")
                    case _ =>
                      val n = counts.getOrElse(divType,0)+1
                      counts.put(divType,n)
                      "_"+n
                  }
                  current = Level(divType,if (attrs("DMDID")!=null) attrs("DMDID").head.text else "",(if (current.fileNamePrefix!="") current.fileNamePrefix+"_" else "")+divType+typeCount, new StringBuilder(),serialize=true)
              }
              hierarchy = hierarchy :+ current
            case EvElemStart(_,"area",sattrs,_) =>
              processArea(sattrs("BEGIN").head.text)
            case EvElemEnd(_,"div") =>
              val hc = hierarchy.last
              hierarchy = hierarchy.dropRight(1)
              if (hc.element=="paragraph" || hc.element=="textblock") hc.content.append("\n") //paragraphs and textblocks result in plaintext paragraphs
              if (hc.serialize) {
                if (!hc.content.mkString.trim.isEmpty) {
                  val pw = new PrintWriter(new File(destdirectory+hc.fileNamePrefix+".txt"))
                  pw.append(hc.content)
                  pw.close()
                  iw.append(hc.content)
                }
              }
              if (hc.serialize || articleMetadata.contains(hc.id)) {
                val pw = new PrintWriter(new File(destdirectory+hc.fileNamePrefix+"_metadata.xml"))
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
        current = Level("","","",new StringBuilder(),serialize = false)
        processArea(block)
        var pw = new PrintWriter(new File(destdirectory+"other_texts_"+block+".txt"))
        pw.append(current.content)
        iw.append(current.content)
        pw.close()
        pw = new PrintWriter(new File(destdirectory+"other_texts_"+block+"_metadata.xml"))
        pw.println("<metadata>\n<blocks>"+composedBlocks(block).sorted.mkString(",")+"</blocks>\n<pages>"+current.pages.toSeq.sorted.mkString(",")+"</pages>\n</metadata>")
        pw.close()
      }
      // here, we then serialize unreferenced text blocks by page
      val unclaimedPageBlocks = new mutable.HashMap[Int,mutable.Buffer[String]]
      for (textBlock <- textBlocks.keys;if !seenTextBlocks.contains(textBlock))
        unclaimedPageBlocks.getOrElseUpdate(Integer.parseInt(textBlock.substring(1).replaceFirst("_.*","")),new ArrayBuffer[String]) += textBlock
      for (page <- unclaimedPageBlocks.keys.toSeq.sorted) {
        var pw = new PrintWriter(new File(destdirectory+"other_texts_page_"+page+"_metadata.xml"))
        pw.println("<metadata>\n<blocks>"+unclaimedPageBlocks(page).sorted.mkString(",")+"</blocks>\n</metadata>")
        pw.close()
        pw = new PrintWriter(new File(destdirectory+"other_texts_page_"+page+".txt"))
        for (text <- unclaimedPageBlocks(page).sortWith(altoBlockOrder(_)<altoBlockOrder(_)).map(textBlocks(_))) {
          pw.println(text)
          iw.println(text)
        }
        pw.close()
      }
      iw.close()
      val iwm = new PrintWriter(new File(destdirectory+"issue_metadata.xml"))
      iwm.println("<metadata>")
      // finally, we serialize also page and issue plaintexts. The blocks are organized sorted first by how they appear in the logical order, then by inserting any blocks not appearing in the logical order after their ALTO predecessor.
      for (page <- pageBlocks.keys.toSeq.sortWith(_.substring(1).toInt<_.substring(1).toInt)) {
        current = Level("","","",new StringBuilder(),serialize = false)
        var (refBlocks,unrefBlocks) = pageBlocks(page).partition(blockOrder.contains)
        refBlocks = refBlocks.sortWith(blockOrder(_)<blockOrder(_))
        if (refBlocks.isEmpty) refBlocks = unrefBlocks.sortWith(altoBlockOrder(_)<altoBlockOrder(_))
        else unrefBlocks.sortWith(altoBlockOrder(_)<altoBlockOrder(_)).foreach( b => {
          val myBlockOrder = altoBlockOrder(b)
          var i = 0
          while (i<refBlocks.length && myBlockOrder>altoBlockOrder(refBlocks(i))) i+=1
          refBlocks.insert(i,b)
        })
        pageBlocks.put(page,refBlocks)
        processArea(page)
        var pw = new PrintWriter(new File(destdirectory+"page_"+page+".txt"))
        pw.append(current.content)
        pw.close()
        pw = new PrintWriter(new File(destdirectory+"page_"+page+"_metadata.xml"))
        val ps = "<blocks>"+pageBlocks(page).sorted.mkString(",")+"</blocks>"
        pw.println("<metadata>\n"+ps+"\n</metadata>")
        iwm.println("<page id=\""+page+"\">\n"+ps+"\n</page>")
        pw.close()
      }
      iwm.println("</metadata>")
      iw.close()
      iwm.close()
      s.close()
  }

  class Opts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val dest = opt[String](required = true)
    val directories = trailArg[List[String]]()
    val serializeImages = opt[Boolean]()
    val omitStructure = opt[Boolean]()
    verify()
  }


  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val dest = opts.dest()
    val serializeImages = opts.serializeImages()
    val omitStructure = opts.omitStructure()
    feedAndProcessFedTasksInParallel(() =>
      for (dir <- opts.directories(); f = new File(dir); prefixLength = f.getAbsolutePath.length)
        getFileTree(f).filter(_.getName == "mets.xml").foreach(file => addTask(file.getPath, () =>
          process(file, dest, prefixLength, omitStructure, serializeImages)
        ))
    )
  }
}
