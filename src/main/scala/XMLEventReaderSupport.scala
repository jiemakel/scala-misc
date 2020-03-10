import java.io.{InputStream, StringReader}

import javax.xml.stream.events._
import javax.xml.stream.{XMLEventReader, XMLInputFactory}

import scala.jdk.CollectionConverters._

object XMLEventReaderSupport {
  abstract class EvEvent
  case class EvDocumentStart(encoding: String) extends EvEvent
  case class EvDocumentEnd() extends EvEvent
  case class EvDTD() extends EvEvent
  case class EvProcessingInstruction() extends EvEvent
  case class EvElemStart(pre: String, name: String, attrs: Map[String,String]) extends EvEvent
  case class EvElemEnd(pre: String, name: String) extends EvEvent
  case class EvText(text: String) extends EvEvent
  case class EvEntityRef(entity: String) extends EvEvent
  case class EvComment(comment: String) extends EvEvent
  private def toEvEvent(e: XMLEvent): EvEvent = e match {
    case startElement: StartElement => EvElemStart(startElement.getName.getPrefix,startElement.getName.getLocalPart,startElement.getAttributes.asScala.map(a => (if (a.getName.getPrefix!="") a.getName.getPrefix+":" else "") + a.getName.getLocalPart -> a.getValue).toMap)
    case endElement: EndElement => EvElemEnd(endElement.getName.getPrefix, endElement.getName.getLocalPart)
    case text: Characters => EvText(text.getData)
    case comment: Comment => EvComment(comment.getText)
    case entityRef: EntityReference => EvEntityRef(entityRef.getName)
    case startDocument: StartDocument => EvDocumentStart(startDocument.getCharacterEncodingScheme)
    case _: EndDocument => EvDocumentEnd()
    case _: DTD => EvDTD()
    case _: ProcessingInstruction => EvProcessingInstruction()
  }
  private val xmlf = XMLInputFactory.newInstance()
  private def toEvEventIterator(r: XMLEventReader) = new Iterator[EvEvent] {
    override def hasNext = r.hasNext

    override def next() = toEvEvent(r.nextEvent)
  }
  def getXMLEventReader(is: InputStream, encoding: String = "UTF-8") = toEvEventIterator(xmlf.createXMLEventReader(is, encoding))
  def getXMLEventReader(string: String) = toEvEventIterator(xmlf.createXMLEventReader(new StringReader(string)))
}
