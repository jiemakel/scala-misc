import java.io.{InputStream, StringReader}

import javax.xml.stream.events._
import javax.xml.stream.{XMLEventReader, XMLInputFactory}

import scala.jdk.CollectionConverters._

object XMLEventReaderSupport {
  case class EvElemStart(pre: String, name: String, attrs: Map[String,String])
  case class EvElemEnd(pre: String, name: String)
  case class EvText(text: String)
  case class EvEntityRef(entity: String)
  case class EvComment(comment: String)

  implicit def toEvElemStart(startElement: StartElement) = EvElemStart(startElement.getName.getPrefix,startElement.getName.getLocalPart,startElement.getAttributes.asScala.map(a => (if (a.getName.getPrefix!="") a.getName.getPrefix+":" else "") + a.getName.getLocalPart -> a.getValue).toMap)
  implicit def toEvElemEnd(endElement: EndElement) = EvElemEnd(endElement.getName.getPrefix, endElement.getName.getLocalPart)
  implicit def toEvText(text: Characters) = EvText(text.getData)
  implicit def toEvEntityRef(entityRef: EntityReference) = EvEntityRef(entityRef.getName)
  implicit def toEvComment(comment: Comment) = EvComment(comment.getText)
  private val xmlf = XMLInputFactory.newInstance()
  def getXMLEventReader(is: InputStream, encoding: String = "UTF-8"): XMLEventReader = xmlf.createXMLEventReader(is, encoding)
  def getXMLEventReader(string: String): XMLEventReader = xmlf.createXMLEventReader(new StringReader(string))
}
