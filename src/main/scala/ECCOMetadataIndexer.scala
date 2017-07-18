import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import org.apache.lucene.store.MMapDirectory
import java.io.File
import java.nio.file.FileSystems
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.SortedDocValues
import org.apache.lucene.document.SortedDocValuesField
import org.apache.lucene.index.DocValues
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import com.bizo.mighty.csv.CSVReader
import scala.language.reflectiveCalls
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import scala.collection.mutable.HashMap

object ECCOMetadataIndexer extends OctavoIndexer {
  
  class Reuse {
    val d = new Document()
    val intPointFields = Set("pagecount.orig","document.items","obl","publication_year","publication_decade","publication_frequency_annual","publication_interval_from","publication_interval_till","author_birth", "author_death", "publication_year_from", "publication_year_to", "publication_year_from","publication_year_till","volnumber","volcount","parts","publication_time")
    val floatPointFields = Set("pagecount","latitude","longitude","paper","paper.check","width","height","area")
    val fields = Seq("control_number","language.English","language.French","language.Latin","language.German","language.Scottish Gaelic","language.Italian","language.Greek Ancient to 1453 ","language.Welsh","language.Portuguese","language.Dutch","language.Greek Modern 1453- ","language.Hebrew","language.Spanish","language.Pahlavi","language.Swedish","language.Irish","language.Manx","language.Romance Other ","language.Algonquian Other ","language.Lithuanian","language.Turkish","language.English Old ca. 450-1100 ","language.Scots","language.Arabic","language.North American Indian Other ","language.Persian","language.French Middle ca. 1300-1600 ","language.Newari","language.Armenian","language.Tamil","language.Icelandic","language.Bengali","language.Russian","language.Malayalam","language.Danish","language.English Middle 1100-1500 ","language.Coptic","language.Mongolian","language.Gujarati","language.Malay","language.Sanskrit","language.Gothic","language.Mohawk","language.Delaware","language.Iroquoian Other ","language.Palauan","language.Arawak","language.Scottish Gaelix","multilingual","language","system_control_number","author_name","author_birth","author_death","title","publication_year_from","publication_year_till","pagecount","volnumber","volcount","parts","gatherings.original","width.original","height.original","obl.original","publication_frequency_annual","publication_frequency_text","publication_interval_from","publication_interval_till","subject_topic","publication_topic","publication_geography","original_row","publisher","latitude","longitude","publication_place","country","author_pseudonyme","author","author_gender","publication_year","publication_decade","first_edition","self_published","gatherings","width","height","obl","area","pagecount.orig","singlevol","multivol","issue","paper","paper.check","document.items","id","document_type","publication_time").map(field => 
      (field , field match {
        case "title" => new TextSDVFieldPair("title", d)
        case "publication_geography" | "publication_topic" | "subject" => null
        case field if (field.startsWith("language")) => null
        case field if (intPointFields.contains(field)) => new IntPointNDVFieldPair(field, d)
        case field if (floatPointFields.contains(field)) => new FloatPointFDVFieldPair(field, d)
        case field => new StringSDVFieldPair(field, d)
      }))
    def clean() {
      d.removeFields("languages")
      d.removeFields("publication_geographies")
      d.removeFields("publication_topics")
      d.removeFields("subjects")
    }
  }
  
  var mdiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var mdpiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var msiw: IndexWriter = null.asInstanceOf[IndexWriter]
  var mpiw: IndexWriter = null.asInstanceOf[IndexWriter]
  
  def process(path: String, metadata: Map[String, Array[String]], iw: IndexWriter): Future[Unit] = Future {
    val r = new Reuse()
    val ir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault().getPath(path))).leaves().get(0).reader
    logger.info("Going to process "+ir.maxDoc+" douments in "+path+".")
    val dv = DocValues.getSorted(ir, "ESTCID")
    var lastESTCID: String = null
    for (d <- 0 until ir.maxDoc) {
      val estcID = "^([A-Z])0*".r.replaceAllIn(dv.get(d).utf8ToString(),m => m.group(1))
      if (estcID!=lastESTCID) {
        lastESTCID = estcID
        r.clean()
        if (!metadata.contains(estcID)) {
          logger.warn("Unknown ESTCID "+estcID)
          for ((fieldName,field)<- r.fields)
            fieldName match {
              case "title" => field.asInstanceOf[TextSDVFieldPair].setValue("")
              case "publication_geography" | "publication_topic" | "subject" => 
              case "language" => 
              case fieldName if (fieldName.startsWith("language")) => 
              case fieldName if (r.intPointFields.contains(fieldName)) => field.asInstanceOf[IntPointNDVFieldPair].setValue(0)
              case fieldName if (r.floatPointFields.contains(fieldName)) => field.asInstanceOf[FloatPointFDVFieldPair].setValue(0.0f)
              case fieldName => field.asInstanceOf[StringSDVFieldPair].setValue("")
            }
        } else {
          val mrow = r.fields.zip(metadata(estcID))
          for (((fieldName,field),fieldValue) <- mrow)
            fieldValue match {
              case "NA" | "" => fieldName match {
                  case "title" => field.asInstanceOf[TextSDVFieldPair].setValue("")
                  case "publication_geography" | "publication_topic" | "subject" => 
                  case "language" => 
                  case fieldName if (fieldName.startsWith("language")) => 
                  case fieldName if (r.intPointFields.contains(fieldName)) => field.asInstanceOf[IntPointNDVFieldPair].setValue(0)
                  case fieldName if (r.floatPointFields.contains(fieldName)) => field.asInstanceOf[FloatPointFDVFieldPair].setValue(0.0f)
                  case fieldName => field.asInstanceOf[StringSDVFieldPair].setValue("")
              }
              case fieldValue => 
                fieldName match {
                  case "title" => field.asInstanceOf[TextSDVFieldPair].setValue(fieldValue)
                  case "publication_geography" | "publication_topic" | "subject" => for (value <- fieldValue.split(";"))
                    new StringSSDVFieldPair(fieldName, r.d).setValue(value.trim)
                  case "language" => new StringSSDVFieldPair("languages", r.d).setValue(fieldValue)
                  case fieldName if (fieldName.startsWith("language")) => if (fieldValue == "TRUE") new StringSSDVFieldPair("languages", r.d).setValue(fieldName.substring(9))
                  case fieldName if (r.intPointFields.contains(fieldName)) => try { field.asInstanceOf[IntPointNDVFieldPair].setValue(fieldValue.toInt) } catch {
                    case e: NumberFormatException => logger.error(fieldName, fieldValue); throw e              
                  }
                  case fieldName if (r.floatPointFields.contains(fieldName)) => field.asInstanceOf[FloatPointFDVFieldPair].setValue(fieldValue.toFloat)
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
    mdiw = iw(opts.index()+"/mdindex",null, opts.indexMemoryMb()/4)
    mdpiw = iw(opts.index()+"/mdpindex",null, opts.indexMemoryMb()/4)
    msiw = iw(opts.index()+"/msindex",null, opts.indexMemoryMb()/4)
    mpiw = iw(opts.index()+"/mpindex",null, opts.indexMemoryMb()/4)
    val futures = new ArrayBuffer[Future[Unit]]
    for (path <- opts.directories()) {
      futures += process(path+"/dindex", metadata, mdiw)
      futures += process(path+"/dpindex", metadata, mdpiw)
      futures += process(path+"/sindex", metadata, msiw)
      futures += process(path+"/pindex", metadata, mpiw)
    }
    Await.result(Future.sequence(futures),Duration.Inf)
    waitForTasks(
      runSequenceInOtherThread(
        () => close(mdiw), 
        () => merge(opts.index()+"/mdindex", null, opts.indexMemoryMb()/4, null)
      ),
      runSequenceInOtherThread(
        () => close(mdpiw), 
        () => merge(opts.index()+"/mdpindex", null, opts.indexMemoryMb()/4, null)
      ),
      runSequenceInOtherThread(
        () => close(msiw), 
        () => merge(opts.index()+"/msindex", null, opts.indexMemoryMb()/4, null)
      ),
      runSequenceInOtherThread(
        () => close(mpiw), 
        () => merge(opts.index()+"/mpindex", null, opts.indexMemoryMb()/4, null)
      )
    )
  }
}