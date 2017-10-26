import java.io.File
import java.text.BreakIterator
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import com.bizo.mighty.csv.CSVReader
import org.apache.lucene.document.{Document, Field, NumericDocValuesField}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.rogach.scallop._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.language.{postfixOps, reflectiveCalls}
import scala.xml.parsing.XhtmlEntities
import scala.xml.pull._

object ECCOIndexer extends OctavoIndexer {
  
  private def readContents(implicit xml: XMLEventReader): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_,_) => return null
      case EvText(text) => content.append(text)
      case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
        case Some(chr) => content.append(chr)
        case _ => content.append(er.entity)
      }
      case EvComment(_) => 
      case EvElemEnd(_,_) => break = true 
    }
    return content.toString.trim
  }
  
  val fileRegex = ".*_(.*)\\.txt".r
  
  val headingRegex = "^# ".r

  private val sentences = new AtomicLong
  private val paragraphs = new AtomicLong
  private val sections = new AtomicLong
  private val documentparts = new AtomicLong
  
  val tld = new ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  class Reuse {
    val sbi = BreakIterator.getSentenceInstance(new Locale("en_GB"))
    val dd = new Document()
    val dpd = new Document()
    val sd = new Document()
    val pd = new Document()
    val send = new Document()
    val collectionIDFields = new StringSDVFieldPair("collectionID", dd, dpd, sd, pd, send)
    val documentIDFields = new StringSDVFieldPair("documentID", dd, dpd, sd, pd, send)
    val estcIDFields = new StringSDVFieldPair("ESTCID", dd, dpd, sd, pd, send)
    val dateStartFields = new IntPointNDVFieldPair("dateStart", dd, dpd, sd, pd, send)
    val dateEndFields = new IntPointNDVFieldPair("dateEnd", dd, dpd, sd, pd, send)
    val totalPagesFields = new IntPointNDVFieldPair("totalPages", dd, dpd, sd, pd, send)
    val languageFields = new StringSDVFieldPair("language", dd, dpd, sd, pd, send)
    val moduleFields = new StringSDVFieldPair("module", dd, dpd, sd, pd, send)
    val fullTitleFields = new TextSDVFieldPair("fullTitle",dd,dpd,sd,pd, send)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength", dd, dpd, sd, pd, send)
    val documentLengthFields = new IntPointNDVFieldPair("documentLength", dd, dpd, sd, pd, send)
    val totalParagraphsFields = new IntPointNDVFieldPair("totalParagraphs", dd, dpd, sd, pd, send)
    def clearOptionalDocumentFields() {
      dateStartFields.setValue(0)
      dateEndFields.setValue(Int.MaxValue)
      totalPagesFields.setValue(0)
      languageFields.setValue("")
      moduleFields.setValue("")
      fullTitleFields.setValue("")
      dd.removeFields("containsGraphicOfType")
      dd.removeFields("containsGraphicCaption")
    }
    def clearOptionalDocumentPartFields() {
      dpd.removeFields("containsGraphicOfType")
      dpd.removeFields("containsGraphicCaption")
    }
    def clearOptionalParagraphFields() {
      pd.removeFields("sectionID")
      pd.removeFields("headingLevel")
      pd.removeFields("heading")
    }
    def clearOptionalSentenceFields() {
      send.removeFields("sectionID")
      send.removeFields("headingLevel")
      send.removeFields("heading")
    }
    val documentPartIdFields = new StringNDVFieldPair("partID", dpd, sd, pd)
    val paragraphIDFields = new StringNDVFieldPair("paragraphID", pd, send)
    val sentenceIDField = new NumericDocValuesField("sentenceID", 0)
    send.add(sentenceIDField)
    val contentField = new Field("content","",contentFieldType)
    dd.add(contentField)
    dpd.add(contentField)
    sd.add(contentField)
    pd.add(contentField)
    send.add(contentField)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens", dd, dpd, sd, pd, send)
    val documentPartTypeFields = new StringSDVFieldPair("documentPartType", dpd, sd, pd, send)
    val sectionIDFields = new StringNDVFieldPair("sectionID", sd)
    val headingFields = new TextSDVFieldPair("heading",sd)
    val headingLevelFields = new IntPointNDVFieldPair("headingLevel", sd)
  }
  
  val termVectorFields = Seq("content", "containsGraphicOfType")
  
  class SectionInfo(val sectionID: Long, val headingLevel: Int, val heading: String) {
    val paragraphSectionIDFields = new StringSNDVFieldPair("sectionID")
    paragraphSectionIDFields.setValue(sectionID)
    val paragraphHeadingLevelFields = new IntPointSNDVFieldPair("headingLevel")
    paragraphHeadingLevelFields.setValue(headingLevel)
    val paragraphHeadingFields = new TextSSDVFieldPair("heading")
	  val content = new StringBuilder()
    def addToDocument(pd: Document) {
      paragraphSectionIDFields.add(pd)
      paragraphHeadingLevelFields.add(pd)
      paragraphHeadingFields.add(pd)
    }
  }
  
  private def index(id: String, file: File): Unit = {
    val filePrefix = file.getName.replace("_metadata.xml","")
    logger.info("Processing: "+file.getPath.replace("_metadata.xml","*"))
    var totalPages = 0
    val xmls = Source.fromFile(file)
    implicit val xml = new XMLEventReader(xmls)
    val r = tld.get
    r.clearOptionalDocumentFields()
    r.collectionIDFields.setValue(id)
    var documentID: String = null
    var estcID: String = null
    while (xml.hasNext) xml.next match {
      case EvElemStart(_,"documentID",_,_) | EvElemStart(_,"PSMID",_,_) =>
        documentID = readContents
        r.documentIDFields.setValue(documentID)
      case EvElemStart(_,"ESTCID",_,_) =>
        estcID = readContents
        r.estcIDFields.setValue(estcID)
      case EvElemStart(_,"bibliographicID",attr,_) if (attr("type")(0).text == "ESTC") => // ECCO2
        estcID = readContents
        r.estcIDFields.setValue(estcID)
      case EvElemStart(_,"pubDate",_,_) => readContents match {
        case null => // ECCO2
          var break = false
          var endDateFound = false
          var startDate: String = null
          while (xml.hasNext && !break) {
            xml.next match {
              case EvElemStart(_,"pubDateStart",_,_) => readContents match {
                case any =>
                  startDate = any
                  r.dateStartFields.setValue(any.toInt)
              }
              case EvElemStart(_,"pubDateEnd",_,_) => readContents match {
                case any =>
                  endDateFound = true
                  r.dateEndFields.setValue(any.replaceAll("00","99").toInt)
              }
              case EvElemEnd(_,"pubDate") => break = true
              case _ => 
            }
          }
          if (!endDateFound && startDate != null) {
            r.dateEndFields.setValue(startDate.replaceAll("00","99").toInt)
          }
        case "" =>
        case "1809" =>
          r.dateStartFields.setValue(18090000)
          r.dateEndFields.setValue(18099999)
        case any => 
          r.dateStartFields.setValue(any.toInt)
          r.dateEndFields.setValue(any.replaceAll("01","99").toInt)
      }
      case EvElemStart(_,"totalPages",_,_) =>
        val tp = readContents
        if (!tp.isEmpty) {
          totalPages = tp.toInt
          r.totalPagesFields.setValue(totalPages)
        }
      case EvElemStart(_,"language",_,_) =>
        r.languageFields.setValue(readContents)
      case EvElemStart(_,"module",_,_) =>
        r.moduleFields.setValue(readContents)
      case EvElemStart(_,"fullTitle",_,_) => 
        r.fullTitleFields.setValue(readContents)
      case _ => 
    }
    xmls.close()
    if (documentID==null) logger.error("No document ID for "+file)
    if (estcID==null) logger.error("No ESTC ID for "+file)
    val dcontents = new StringBuilder
    var lastPage = 0
    val filesToProcess = new ArrayBuffer[File] 
    for (file <- getFileTree(file.getParentFile); if file.getName.endsWith(".txt") && file.getName.startsWith(filePrefix)) if (file.getName.contains("_page"))
      for (curPage <- "_page([0-9]+)".r.findFirstMatchIn(file.getName).map(_.group(1).toInt); if curPage>lastPage) lastPage = curPage
    else filesToProcess += file
    if (totalPages != lastPage) logger.warn(s"total pages $totalPages != actual $lastPage")
    var totalParagraphs = 0
    var totalLength = 0
    for (file <- filesToProcess;line <- Source.fromFile(file).getLines) {
      if (line.isEmpty) totalParagraphs += 1
      totalLength += line.length
    }
    r.documentLengthFields.setValue(totalLength)
    r.totalParagraphsFields.setValue(totalParagraphs)
    for (file <- filesToProcess.sortBy(x => x.getName.substring(x.getName.indexOf('_') + 1, x.getName.indexOf('_', x.getName.indexOf('_') + 1)).toInt)) {
      val dpcontents = new StringBuilder
      r.clearOptionalDocumentPartFields()
      r.documentPartIdFields.setValue(documentparts.incrementAndGet)
      r.documentPartTypeFields.setValue(fileRegex.findFirstMatchIn(file.getName).get.group(1))
      if (new File(file.getPath.replace(".txt","-graphics.csv")).exists)
        for (row <- CSVReader(file.getPath.replace(".txt","-graphics.csv"))) {
          val gtype = if (row(1)=="") "unknown" else row(1)
          val f = new Field("containsGraphicOfType",gtype, notStoredStringFieldWithTermVectors)
          r.dpd.add(f)
          r.dd.add(f)
          if (row(3)!="") {
            val f = new Field("containsGraphicCaption", row(3), normsOmittingStoredTextField)
            r.dpd.add(f)
            r.dd.add(f)
          }
        }
      val headingInfos = Seq(1,2,3).map(w => None.asInstanceOf[Option[SectionInfo]]).toArray
      val pcontents = new StringBuilder
      val fl = Source.fromFile(file)
      for (line <- fl.getLines) {
        if (line.isEmpty) {
          if (!pcontents.isEmpty) {
            val c = pcontents.toString
            r.clearOptionalParagraphFields()
            r.clearOptionalSentenceFields()
            r.paragraphIDFields.setValue(paragraphs.incrementAndGet)
            r.contentField.setStringValue(c)
            r.contentLengthFields.setValue(c.length)
            r.contentTokensFields.setValue(getNumberOfTokens(c))
            var hasHeadingInfos = false
            headingInfos.foreach(_.foreach(hi => {
              hi.addToDocument(r.pd)
              hi.addToDocument(r.send)
              hasHeadingInfos = true
            }))
            piw.addDocument(r.pd)
            r.sbi.setText(c)
            var start = r.sbi.first()
            var end = r.sbi.next()
            while (end != BreakIterator.DONE) {
              val sentence = c.substring(start,end)
              r.sentenceIDField.setLongValue(sentences.incrementAndGet)
              r.contentField.setStringValue(sentence)
              val tokens = getNumberOfTokens(sentence)
              r.contentLengthFields.setValue(sentence.length)
              r.contentTokensFields.setValue(getNumberOfTokens(sentence))
              seniw.addDocument(r.send)
              start = end
              end = r.sbi.next()
            }
            pcontents.clear()
          }
        } else
          pcontents.append(line)
        if (line.startsWith("# ") || line.startsWith("## ") || line.startsWith("### ")) {
          val level =
            if (line.startsWith("### ")) 2
            else if (line.startsWith("## ")) 1
            else 0
          for (
            i <- level until headingInfos.length;
            headingInfoOpt = headingInfos(i);
        	  headingInfo <- headingInfoOpt
          ) {
              r.sectionIDFields.setValue(headingInfo.sectionID)
              r.headingLevelFields.setValue(headingInfo.headingLevel)
              r.headingFields.setValue(headingInfo.heading)
              if (!headingInfo.content.isEmpty) {
            	  val contentS = headingInfo.content.toString
            	  r.contentField.setStringValue(contentS)
            	  r.contentLengthFields.setValue(contentS.length)
            	  r.contentTokensFields.setValue(getNumberOfTokens(contentS))
              }
              siw.addDocument(r.sd)
              headingInfos(i) = None
            }
          headingInfos(level) = Some(new SectionInfo(sections.incrementAndGet, level, line.substring(level+2)))
          for (i <- 0 until level) headingInfos(i).foreach(_.content.append(line))
        } else
          headingInfos.foreach(_.foreach(_.content.append(line)))
        dpcontents.append(line+"\n")
      }
      fl.close()
      if (!pcontents.isEmpty) {
        val c = pcontents.toString
        r.clearOptionalParagraphFields()
        r.clearOptionalSentenceFields()
        r.paragraphIDFields.setValue(paragraphs.incrementAndGet)
        r.contentField.setStringValue(c)
        r.contentLengthFields.setValue(c.length)
        r.contentTokensFields.setValue(getNumberOfTokens(c))
        var hasHeadingInfos = false
        headingInfos.foreach(_.foreach(hi => {
          hi.addToDocument(r.pd)
          hi.addToDocument(r.send)
          hasHeadingInfos = true
        }))
        piw.addDocument(r.pd)
        r.sbi.setText(c)
        var start = r.sbi.first()
        var end = r.sbi.next()
        while (end != BreakIterator.DONE) {
          val sentence = c.substring(start,end)
          r.sentenceIDField.setLongValue(sentences.incrementAndGet)
          r.contentField.setStringValue(sentence)
          val tokens = getNumberOfTokens(sentence)
          r.contentLengthFields.setValue(sentence.length)
          r.contentTokensFields.setValue(getNumberOfTokens(sentence))
          seniw.addDocument(r.send)
          start = end
          end = r.sbi.next()
        }
      }
      for (
          headingInfoOpt <- headingInfos;
          headingInfo <- headingInfoOpt
      ) { 
          r.sectionIDFields.setValue(headingInfo.sectionID)
          r.headingLevelFields.setValue(headingInfo.headingLevel)
          r.headingFields.setValue(headingInfo.heading)
          if (!headingInfo.content.isEmpty) {
        	  val contentS = headingInfo.content.toString.trim
        	  r.contentField.setStringValue(contentS)
        	  r.contentLengthFields.setValue(contentS.length)
        	  r.contentTokensFields.setValue(getNumberOfTokens(contentS))
          }
          siw.addDocument(r.sd)
        }
      val dpcontentsS = dpcontents.toString.trim
      r.contentField.setStringValue(dpcontentsS)
      r.contentLengthFields.setValue(dpcontentsS.length)
      r.contentTokensFields.setValue(getNumberOfTokens(dpcontentsS))
      dpiw.addDocument(r.dpd)
      dcontents.append(dpcontentsS)
      dcontents.append("\n\n")
    }
    val dcontentsS = dcontents.toString.trim
    r.contentField.setStringValue(dcontentsS)
    r.contentLengthFields.setValue(dcontentsS.length)
    r.contentTokensFields.setValue(getNumberOfTokens(dcontentsS))
    diw.addDocument(r.dd)
    logger.info("Processed: "+file.getPath.replace("_metadata.xml","*"))
  }
  
  var diw, dpiw, siw, piw, seniw = null.asInstanceOf[IndexWriter]
  
  val ds = new Sort(new SortField("documentID",SortField.Type.STRING))
  val dps = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("partID", SortField.Type.LONG))
  val ss = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("sectionID", SortField.Type.LONG))
  val ps = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG))
  val sens = new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG), new SortField("sentenceID", SortField.Type.LONG))
  
  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val dpostings = opt[String](default = Some("blocktree"))
      val dppostings = opt[String](default = Some("blocktree"))
      val spostings = opt[String](default = Some("blocktree"))
      val ppostings = opt[String](default = Some("fst"))
      val senpostings = opt[String](default = Some("fst"))
      verify()
    }
    if (!opts.onlyMerge()) {
      // document level
      diw = iw(opts.index()+"/dindex", ds, opts.indexMemoryMb()/5)
      // document part level
      dpiw = iw(opts.index()+"/dpindex", dps, opts.indexMemoryMb()/5)
      // section level
      siw = iw(opts.index()+"/sindex", ss, opts.indexMemoryMb()/5)
      // paragraph level
      piw = iw(opts.index()+"/pindex", ps, opts.indexMemoryMb()/5)
      // sentence level
      seniw = iw(opts.index()+"/senindex", sens, opts.indexMemoryMb()/5)
      /*
       *
     6919 article
    51226 backmatter
    34276 book
  1377880 chapter
   147743 frontmatter
    13386 index
    25772 other
    38055 part
   290959 section
     3957 titlePage
    32836 TOC
     1481 volume
       */
      feedAndProcessFedTasksInParallel(() => {
        opts.directories().toStream.flatMap(p => {
          val parts = p.split(':')
          getFileTree(new File(parts(0))).map((parts(1),_))
        }).filter(_._2.getName.endsWith("_metadata.xml")).foreach(pair => {
          val path = pair._2.getPath.replace("_metadata.xml","*")
          addTask(path, () => index(pair._1, pair._2))
        })
      })
    }
    waitForTasks(
      runSequenceInOtherThread(
        () => close(diw), 
        () => merge(opts.index()+"/dindex", ds, opts.indexMemoryMb()/5, toCodec(opts.dpostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(dpiw), 
        () => merge(opts.index()+"/dpindex", dps, opts.indexMemoryMb()/5, toCodec(opts.dppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(siw), 
        () => merge(opts.index()+"/sindex", ss, opts.indexMemoryMb()/5, toCodec(opts.spostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(piw), 
        () => merge(opts.index()+"/pindex", ps, opts.indexMemoryMb()/5, toCodec(opts.ppostings(), termVectorFields))
      ),
      runSequenceInOtherThread(
        () => close(seniw), 
        () => merge(opts.index()+"/senindex", sens, opts.indexMemoryMb()/5, toCodec(opts.senpostings(), termVectorFields))
      )      
    )
  }
}
