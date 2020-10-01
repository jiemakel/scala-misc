import java.io.{BufferedWriter, FileInputStream, FileWriter}

import XMLEventReaderSupport._

import scala.collection.mutable.HashSet

object F1800XMLToFreq {

  def main(args: Array[String]): Unit = {
    val ngrams = new HashSet[String]
    for (file <- new java.io.File("texts/").listFiles) {
      println("Processing: "+file)
      val fis = new FileInputStream(file)
      val xml = getXMLEventReader(fis,"ISO-8859-1")
      var break = false
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"text",_) =>
          while (!break) xml.next match {
             case EvElemStart(_,"foreign", _) =>
               while (!break) {
                 xml.next match {
                   case EvElemEnd(_,"foreign") => break = true
                   case _ => 
                 }
               }
               break = false
             case EvText(text,_)  => text.split("\\PL+").filter(!_.isEmpty).foreach { token => ngrams+=token.toLowerCase }
             case EvElemEnd(_,"text") => break = true
             case _ => 
          }
        case _ =>
      }
      fis.close()
    }
    val file = new java.io.File("tokens.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    ngrams.foreach(token => { bw.write(token);bw.newLine()})
    bw.close()
  }
}
