import com.bizo.mighty.csv.CSVReader
import java.net.URLEncoder
import org.apache.jena.riot.RDFFormat
import org.apache.jena.riot.RDFDataMgr
import java.io.FileOutputStream
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.vocabulary.OWL
import com.hp.hpl.jena.vocabulary.DC
import com.hp.hpl.jena.vocabulary.DC_11
import com.hp.hpl.jena.vocabulary.RDFS
import com.bizo.mighty.csv.CSVDictReader
import com.bizo.mighty.csv.CSVReaderSettings
import scala.io.Source
import com.hp.hpl.jena.sparql.vocabulary.FOAF
import scala.xml.pull.XMLEventReader
import scala.xml.pull.EvElemStart
import scala.xml.pull.EvText
import scala.xml.pull.EvElemEnd
import scala.xml.MetaData
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import java.io.PrintWriter
import java.io.File

object MODSALTO2Text {
  
      
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
      
  def main(args: Array[String]): Unit = {
    val texts = new HashMap[String,String]
    val composedBlocks = new HashMap[String,HashSet[String]]
    var break = false
    for (file <- new java.io.File("1457-4721_1874-07-17_82/alto").listFiles) {
      println("Processing: "+file)
      val xml = new XMLEventReader(Source.fromFile(file,"UTF-8"))
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
          texts.put(textBlock,text)
        case _ =>
      }
    }
    val xml = new XMLEventReader(Source.fromFile("1457-4721_1874-07-17_82/1457-4721_1874-07-17_82_mets.xml","UTF-8"))
    val articleMetadata: HashMap[String,String] = new HashMap
    var advertisements = 0
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"dmdSec", attrs, _) if attrs("ID")(0).text.startsWith("MODSMD_ARTICLE") =>
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
          case EvElemEnd(_,"mods") => break = true
          case EvElemEnd(_, label) => 
            indent = indent.substring(0,indent.length-2)
            metadata+=indent + "</"+label+">\n" 
        }
        articleMetadata.put(article,metadata)
      // case EvElemStart(_,"structMap", attrs, _) if attrs("TYPE")(0).text=="LOGICAL" => println("logical")
      case EvElemStart(_,"div", attrs, _) if (attrs("TYPE")(0).text=="ARTICLE" || attrs("TYPE")(0).text=="ADVERTISEMENT") => 
        val atype = if (attrs("TYPE")(0).text=="ADVERTISEMENT") "advertisement" else "article"
        val articleNumber = if (atype=="advertisement") {
          advertisements += 1          
          ""+advertisements
        }
        else attrs("DMDID")(0).text.substring(14)
        if (atype == "article" && articleMetadata.contains(attrs("DMDID")(0).text)) {
          val pw = new PrintWriter(new File("article_"+articleNumber+"_metadata.txt"))
          pw.append(articleMetadata(attrs("DMDID")(0).text))
          pw.close()
        }
        val pw = new PrintWriter(new File(atype+'_'+articleNumber+".txt"))
        //if (attrs("label")!=null) pw.print("# "+attrs("label")(0).text+"\n\n")
        var depth = 1
        while (xml.hasNext && depth!=0) xml.next match {
          case EvElemStart(_,"div",attrs,_) => 
            depth += 1
            attrs("TYPE")(0).text match {
              case "TITLE" => pw.append("# ")
//              case "PARAGRAPH" => pw.println()
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
            if (texts.contains(areaId))
              pw.println(texts(areaId))
            else for (block <- composedBlocks(areaId))
              pw.println(texts(block))
          }
          case EvElemEnd(_,"div") => depth -= 1
          case _ => 
        }
        pw.close()
      case EvElemStart(_,"div", attrs, _) => println("Unhandled type: "+attrs("TYPE")(0).text)
      case _ =>  
    }
  }
}
