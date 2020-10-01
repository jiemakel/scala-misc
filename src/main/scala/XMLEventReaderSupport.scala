import java.io.{InputStream, StringReader}

import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}


object XMLEventReaderSupport {

  abstract class EvEvent

  case class EvDocumentStart(encoding: String) extends EvEvent

  case class EvDTD() extends EvEvent

  case class EvProcessingInstruction() extends EvEvent

  case class EvElemStart(pre: (String,String), name: String, attrs: Map[String, String]) extends EvEvent

  case class EvElemEnd(pre: (String,String), name: String) extends EvEvent

  object TextType extends Enumeration {
    val Characters, IgnoreableWhitespace, CData = Value
  }

  case class EvText(text: String, textType: TextType.Value) extends EvEvent

  case class EvEntityRef(entity: String) extends EvEvent

  case class EvComment(comment: String) extends EvEvent

  private val xmlf = XMLInputFactory.newInstance()
  xmlf.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false)

  private def toEvEventIterator(e: XMLStreamReader) = new Iterator[EvEvent] {
    override def hasNext = e.hasNext

    override def next() = {
      val ret = e.getEventType match {
        case XMLStreamConstants.START_ELEMENT => EvElemStart((e.getPrefix, e.getNamespaceURI), e.getLocalName, 0.until(e.getAttributeCount).map(i => (if (e.getAttributePrefix(i) != "") e.getAttributePrefix(i) + ':' + e.getAttributeLocalName(i) else e.getAttributeLocalName(i)) -> e.getAttributeValue(i)).toMap.withDefaultValue(null))
        case XMLStreamConstants.END_ELEMENT => EvElemEnd((e.getPrefix, e.getNamespaceURI), e.getLocalName)
        case XMLStreamConstants.CHARACTERS => EvText(e.getText, TextType.Characters)
        case XMLStreamConstants.COMMENT => EvComment(e.getText)
        case XMLStreamConstants.ENTITY_REFERENCE => EvEntityRef(e.getLocalName)
        case XMLStreamConstants.START_DOCUMENT => EvDocumentStart(e.getCharacterEncodingScheme)
        case XMLStreamConstants.DTD => EvDTD()
        case XMLStreamConstants.PROCESSING_INSTRUCTION => EvProcessingInstruction()
        case XMLStreamConstants.SPACE => EvText(e.getText, TextType.IgnoreableWhitespace)
        case XMLStreamConstants.CDATA => EvText(e.getText, TextType.CData)
        case XMLStreamConstants.ENTITY_DECLARATION => throw new UnsupportedOperationException("An XMLStreamReader should not emit entity declaration events.")
        case XMLStreamConstants.NOTATION_DECLARATION => throw new UnsupportedOperationException("An XMLStreamReader should not emit notation declaration events.")
        case XMLStreamConstants.ATTRIBUTE => throw new UnsupportedOperationException("An XMLStreamReader should not emit attribute events separately.")
        case XMLStreamConstants.NAMESPACE => throw new UnsupportedOperationException("An XMLStreamReader should not emit namespace events separately.")
        case o => throw new UnsupportedOperationException("Don't know how to handle event "+o+". Check XMLStreamConstants for what that number means.")
      }
      e.next() // NOTE: EndDocument will never be reported
      ret
    }
  }
  def getXMLEventReader(is: InputStream, encoding: String = "UTF-8") = toEvEventIterator(xmlf.createXMLStreamReader(is, encoding))
  def getXMLEventReader(string: String) = toEvEventIterator(xmlf.createXMLStreamReader(new StringReader(string)))
}
