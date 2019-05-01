import java.nio.file.FileSystems

import com.bizo.mighty.csv.CSVReader
import org.apache.lucene.index.{DirectoryReader, DocValues, IndexWriter}
import org.apache.lucene.store.MMapDirectory

import scala.language.reflectiveCalls

object ECCOMetadataIndexer extends OctavoIndexer {
  
  class Reuse {
    val d = new FluidDocument()
    val intPointFields = Set("pagecount.orig","document.items","publication_year","publication_decade","publication_frequency_annual","publication_interval_from","publication_interval_till","author_birth", "author_death", "publication_year_from", "publication_year_to", "publication_year_from","publication_year_till","volnumber","volcount","parts","publication_time")
    val floatPointFields = Set("pagecount","latitude","longitude","paper","width","height","area")
    val fields = Seq("control_number","language.English","language.French","language.Latin","language.German","language.Scottish Gaelic","language.Italian","language.Greek Ancient to 1453 ","language.Welsh","language.Portuguese","language.Dutch","language.Greek Modern 1453- ","language.Hebrew","language.Spanish","language.Pahlavi","language.Swedish","language.Irish","language.Manx","language.Romance Other ","language.Algonquian Other ","language.Lithuanian","language.Turkish","language.English Old ca. 450-1100 ","language.Scots","language.Arabic","language.North American Indian Other ","language.Persian","language.French Middle ca. 1300-1600 ","language.Newari","language.Armenian","language.Tamil","language.Icelandic","language.Bengali","language.Russian","language.Malayalam","language.Danish","language.English Middle 1100-1500 ","language.Coptic","language.Mongolian","language.Gujarati","language.Malay","language.Sanskrit","language.Gothic","language.Mohawk","language.Delaware","language.Iroquoian Other ","language.Palauan","language.Arawak","language.Scottish Gaelix","multilingual","language","system_control_number","author_name","author_birth","author_death","title","publication_year_from","publication_year_till","pagecount","volnumber","volcount","parts","gatherings.original","width.original","height.original","obl.original","publication_frequency_annual","publication_frequency_text","publication_interval_from","publication_interval_till","subject_topic","publication_topic","publication_geography","original_row","publisher","latitude","longitude","publication_place","country","author_pseudonyme","author","author_gender","publication_year","publication_decade","first_edition","self_published","gatherings","width","height","obl","area","pagecount.orig","singlevol","multivol","issue","paper","paper.check","document.items","id","document_type","publication_time").map(field =>
      (field , field match {
        case "title" => new TextSDVFieldPair("title").r(d)
        case "original_row" | "paper.check" => null
        case "publication_geography" | "publication_topic" | "subject" => null
        case field if field.startsWith("language") => null
        case field if intPointFields.contains(field) => new IntPointNDVFieldPair(field).r(d)
        case field if floatPointFields.contains(field) => new FloatPointFDVFieldPair(field).r(d)
        case field => new StringSDVFieldPair(field).r(d)
      }))
    def clean() {
      d.clearOptional()
/*      d.removeFields("estc_language")
      d.removeFields("publication_geography")
      d.removeFields("publication_topic")
      d.removeFields("subject") */
    }
  }
  
  var mdiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var mwiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var mdpiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var msiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var mseniw: IndexWriter = null.asInstanceOf[IndexWriter]
  var mpiw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  def process(path: String, metadata: Map[String, Array[String]], iw: IndexWriter): Unit = {
    val r = new Reuse()
    val ir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault.getPath(path))).leaves().get(0).reader
    logger.info("Going to process "+ir.maxDoc+" documents in "+path+".")
    val dv = DocValues.getSorted(ir, "ESTCID")
    var lastESTCID: String = null
    for (d <- 0 until ir.maxDoc) {
      dv.advance(d)
      val estcID = "^([A-Z])0*".r.replaceAllIn(dv.binaryValue.utf8ToString,m => m.group(1))
      if (estcID!=lastESTCID) {
        lastESTCID = estcID
        r.clean()
        if (!metadata.contains(estcID)) {
          logger.warn("Unknown ESTCID "+estcID)
          for ((fieldName,field)<- r.fields) if (fieldName != "original_row" && fieldName != "paper.check")
            fieldName match {
              case "title" => field.asInstanceOf[TextSDVFieldPair].setValue("")
              case "publication_geography" | "publication_topic" | "subject" => 
              case "language" => 
              case _ if fieldName.startsWith("language") =>
              case _ if r.intPointFields.contains(fieldName) => field.asInstanceOf[IntPointNDVFieldPair].setValue(0)
              case _ if r.floatPointFields.contains(fieldName) => field.asInstanceOf[FloatPointFDVFieldPair].setValue(0.0f)
              case _ => field.asInstanceOf[StringSDVFieldPair].setValue("")
            }
        } else {
          val mrow = r.fields.zip(metadata(estcID))
          for (((fieldName,field),fieldValue) <- mrow) if (fieldName != "original_row" && fieldName != "paper.check")
            fieldValue match {
              case "NA" | "" => fieldName match {
                  case "title" => field.asInstanceOf[TextSDVFieldPair].setValue("")
                  case "publication_geography" | "publication_topic" | "subject" => 
                  case "language" => 
                  case fieldName if fieldName.startsWith("language") =>
                  case fieldName if r.intPointFields.contains(fieldName) => field.asInstanceOf[IntPointNDVFieldPair].setValue(0)
                  case fieldName if r.floatPointFields.contains(fieldName) => field.asInstanceOf[FloatPointFDVFieldPair].setValue(0.0f)
                  case fieldName => field.asInstanceOf[StringSDVFieldPair].setValue("")
              }
              case fieldValue => 
                fieldName match {
                  case "title" => field.asInstanceOf[TextSDVFieldPair].setValue(fieldValue)
                  case "publication_geography" | "publication_topic" | "subject" => for (value <- fieldValue.split(";"))
                    new StringSSDVFieldPair(fieldName).o(r.d).setValue(value.trim)
                  case "language" => new StringSSDVFieldPair("estc_language").o(r.d).setValue(fieldValue)
                  case "obl" => field.asInstanceOf[StringSDVFieldPair].setValue(if (fieldValue == "0") "FALSE" else "TRUE")
                  case fieldName if fieldName.startsWith("language") => if (fieldValue == "TRUE") new StringSSDVFieldPair("estc_language").o(r.d).setValue(fieldName.substring(9))
                  case fieldName if r.intPointFields.contains(fieldName) => try { field.asInstanceOf[IntPointNDVFieldPair].setValue(fieldValue.toInt) } catch {
                    case e: NumberFormatException => logger.error(fieldName, fieldValue); throw e              
                  }
                  case fieldName if r.floatPointFields.contains(fieldName) => field.asInstanceOf[FloatPointFDVFieldPair].setValue(fieldValue.toFloat)
                  case fieldName => field.asInstanceOf[StringSDVFieldPair].setValue(fieldValue)
                }
            }
        }
      }
      iw.addDocument(r.d)
    }
  }
  
  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val metadata = opt[String](required = true)
      val hasWorkIndex = opt[Boolean]()
      verify()
    }
    val r = CSVReader(opts.metadata())
    val fields = r.next()
    val NA = "NA"
    val TRUE = "TRUE"
    val FALSE = "FALSE"
    val metadata = r.map(row => (row(98),row.map(_ match {
      case "NA" => NA
      case "TRUE" => TRUE
      case "FALSE" => FALSE
      case any => any
    }))).toMap
    logger.info("Metadata loaded for "+metadata.size+" ids")
    val parts = if (opts.hasWorkIndex()) 6 else 5
    mdiw = iw(opts.index()+"/mdindex", null, opts.indexMemoryMb()/parts)
    if (opts.hasWorkIndex()) mwiw = iw(opts.index()+"/mwindex", null, opts.indexMemoryMb()/parts)
    mdpiw = iw(opts.index()+"/mdpindex", null, opts.indexMemoryMb()/parts)
    msiw = iw(opts.index()+"/msindex", null, opts.indexMemoryMb()/parts)
    mseniw = iw(opts.index()+"/msenindex", null, opts.indexMemoryMb()/parts)
    mpiw = iw(opts.index()+"/mpindex", null, opts.indexMemoryMb()/parts)
    var tasks = Seq(runSequenceInOtherThread(
      () => process(opts.index()+"/dindex", metadata, mdiw),
      () => close(mdiw),
      () => merge(opts.index()+"/mdindex", null, opts.indexMemoryMb()/parts, null)
    ),
      runSequenceInOtherThread(
        () => process(opts.index()+"/dpindex", metadata, mdpiw),
        () => close(mdpiw),
        () => merge(opts.index()+"/mdpindex", null, opts.indexMemoryMb()/parts, null)
      ),
      runSequenceInOtherThread(
        () => process(opts.index()+"/sindex", metadata, msiw),
        () => close(msiw),
        () => merge(opts.index()+"/msindex", null, opts.indexMemoryMb()/parts, null)
      ),
      runSequenceInOtherThread(
        () => process(opts.index()+"/senindex", metadata, mseniw),
        () => close(mseniw),
        () => merge(opts.index()+"/msenindex", null, opts.indexMemoryMb()/parts, null)
      ),
      runSequenceInOtherThread(
        () => process(opts.index()+"/pindex", metadata, mpiw),
        () => close(mpiw),
        () => merge(opts.index()+"/mpindex", null, opts.indexMemoryMb()/parts, null)
      ))
    if (opts.hasWorkIndex()) tasks = tasks :+ runSequenceInOtherThread(
      () => process(opts.index()+"/windex", metadata, mwiw),
      () => close(mwiw),
      () => merge(opts.index()+"/mwindex", null, opts.indexMemoryMb()/parts, null)
    )
    waitForTasks(tasks:_*)
  }
}
