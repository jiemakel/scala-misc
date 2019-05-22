import java.text.BreakIterator
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import com.bizo.mighty.csv.{CSVReader, Row}
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.rogach.scallop._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.{postfixOps, reflectiveCalls}


object ToEIndexer extends OctavoIndexer {

  private val sentences = new AtomicLong

  class Reuse {
    val send = new FluidDocument
    val sbi = BreakIterator.getSentenceInstance(new Locale("en_GB"))
    val spd = new FluidDocument
    val ad = new FluidDocument
    //val agendaIDFields = new StringSDVFieldPair("agendaID").r(ad,spd,send)
    val speechIDFields = new StringSDVFieldPair("speechID").r(spd,send)
    val sentenceIDField = new NumericDocValuesField("sentenceID", 0)
    send.addRequired(sentenceIDField)
    val dateFields = new IntPointNDVFieldPair("date").r(ad,spd,send)
    val speechField = new ContentField("speech").r(ad,spd,send)
    val contentLengthFields = new IntPointNDVFieldPair("speechLength").r(ad,spd,send)
    val contentTokensFields = new IntPointNDVFieldPair("speechTokens").r(ad,spd,send)
    val languageFields = new StringSDVFieldPair("language").o(spd,send)
    val agendaItemFields = new TextSDVFieldPair("agendaItem").r(ad,spd,send)
    val videoURL = new StringSDVFieldPair("videoURL").o(spd)
    val sentencesFields = new IntPointNDVFieldPair("sentences").r(spd, ad)
    val speakerCountry = new StringSDVFieldPair("speakerCountry").o(spd,send)
    val speakerName = new StringSDVFieldPair("speakerName").o(spd,send)
    val speakerSex = new StringSDVFieldPair("speakerSex").o(spd,send)
    val speakerDateOfBirth = new IntPointNDVFieldPair("speakerDateOfBirth").o(spd,send)
    val speakerPlaceOfBirth = new StringSDVFieldPair("speakerPlaceOfBirth").o(spd,send)
    val speakerFunction = new TextSSDVFieldPair("speakerFunction").o(spd,send) // institution + institution type + role
  }
  
  val tld = new java.lang.ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  private val twitterDateTimeFormat = DateTimeFormat.forPattern( "EE MMM dd HH:mm:ss Z yyyy" ).withLocale(Locale.US)
  private val isoDateTimeFormat = ISODateTimeFormat.dateTimeNoMillis()
 
  private def index(speechId: String, speech: String): Unit = {
    val r = tld.get
    val m = speechMetadata(speechId)
    r.spd.clearOptional()
    r.send.clearOptional()
    r.speechIDFields.setValue(speechId)
    r.dateFields.setValue((m(2).substring(0,4)+m(2).substring(5,7)+m(2).substring(8,10)).toInt)
    r.agendaItemFields.setValue(agendaItems(m(4)))
    if (m(5)!="") r.videoURL.setValue(m(5))
    if (m(1)!="") r.languageFields.setValue(m(1))
    if (m(3)!="") {
      val sm = speakers(m(3))
      r.speakerName.setValue(sm(1))
      if (sm(2)!="" && sm(2)!="NA") r.speakerCountry.setValue(sm(2))
      if (sm(5)!="" && sm(5)!="NA") r.speakerSex.setValue(sm(5))
      if (sm(3)!="" && sm(3)!="NA") r.speakerPlaceOfBirth.setValue(sm(3))
      if (sm(4)!="" && sm(4)!="NA") r.speakerDateOfBirth.setValue((sm(4).substring(0,4)+sm(4).substring(5,7)+sm(4).substring(8,10)).toInt)
    }
    spokenAs.get(speechId).foreach(f => for (pf <- f) {
      val (institutionId,role) = politicalFunctions(pf)
      val (institution,itype) = institutions(institutionId)
      val value = role + ", " + institution + (if (itype!="") " ("+itype+")" else "")
      new TextSSDVFieldPair("speakerFunction").o(r.spd).setValue(value)
    })
    r.sbi.setText(speech)
    var start = r.sbi.first()
    var end = r.sbi.next()
    var csentences = 0
    while (end != BreakIterator.DONE) {
      val sentence = speech.substring(start,end)
      if (seniw != null) {
        r.sentenceIDField.setLongValue(sentences.incrementAndGet)
        r.speechField.setValue(sentence)
        r.contentLengthFields.setValue(sentence.length)
        r.contentTokensFields.setValue(r.speechField.numberOfTokens)
        seniw.addDocument(r.send)
      }
      start = end
      end = r.sbi.next()
      csentences += 1
    }
    r.sentencesFields.setValue(csentences)
    r.speechField.setValue(speech)
    r.contentLengthFields.setValue(speech.length)
    r.contentTokensFields.setValue(r.speechField.numberOfTokens)
    siw.addDocument(r.spd)
  }

  var siw: IndexWriter = null.asInstanceOf[IndexWriter]
  var seniw: IndexWriter = null.asInstanceOf[IndexWriter]

  val ts = new Sort(new SortField("id",SortField.Type.STRING))

  var agendaItems: Map[String, String] = _
  var institutions: Map[String, (String, String)] = _
  var politicalFunctions: Map[String, (String, String)] = _
  var speakers: Map[String, Row] = _
  var speechMetadata: Map[String, Row] = _
  val spokenAs: mutable.HashMap[String, ArrayBuffer[String]] = new mutable.HashMap[String,ArrayBuffer[String]]

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val spostings = opt[String](default = Some("fst"))
      val senpostings = opt[String](default = Some("fst"))
      verify()
    }
    var r = CSVReader(opts.directories().head+"/agendaItems.csv")
    r.next()
    agendaItems = r.map(r => r(0)->r(1)).toMap
    r = CSVReader(opts.directories().head+"/institutions.csv")
    r.next()
    institutions = r.map(r => r(0)->(r(1),r(2))).toMap
    r = CSVReader(opts.directories().head+"/politicalFunctions.csv")
    r.next()
    politicalFunctions = r.map(r => r(0)->(r(1),r(2).charAt(36).toUpper + r(2).substring(37))).toMap
    r = CSVReader(opts.directories().head+"/speakers.csv")
    var fields = r.next()
    speakers = r.map(r => r(0)->r).toMap
    r = CSVReader(opts.directories().head+"/spokenAs.csv")
    r.next()
    for (row <- r) spokenAs.getOrElseUpdate(row(0),new ArrayBuffer[String]) += row(1)
    r = CSVReader(opts.directories().head+"/speechMetadata.csv")
    r.next()
    speechMetadata = r.map(r => r(0)->r).toMap
    if (!opts.onlyMerge()) {
      siw = iw(opts.index()+"/sindex",ts,opts.indexMemoryMb() / 2)
      seniw = iw(opts.index()+"/senindex",ts,opts.indexMemoryMb() / 2)
      feedAndProcessFedTasksInParallel(() => {
        r = CSVReader(opts.directories().head + "/English-speeches.csv")
        r.next()
        for (row <- r)
          addTask(row(0), () => index(row(0),row(1)))
      })
    }
    val termVectorFields = Seq("speech")
    waitForTasks(
      runSequenceInOtherThread(
        () => close(siw),
        () => merge(opts.index()+"/sindex", ts,opts.indexMemoryMb() / 2, toCodec(opts.spostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(seniw),
        () => merge(opts.index()+"/senindex", ts,opts.indexMemoryMb() / 2, toCodec(opts.spostings(), termVectorFields))
      )
    )
  }
}
