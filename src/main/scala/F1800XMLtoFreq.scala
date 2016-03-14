import com.bizo.mighty.csv.CSVReader
import java.net.URLEncoder
import scala.io.Source
import scala.xml.pull._
import org.apache.jena.riot.RDFFormat
import org.apache.jena.riot.RDFDataMgr
import java.io.FileOutputStream
import com.bizo.mighty.csv.CSVDictReader
import scala.xml.parsing.XhtmlEntities
import scala.collection.mutable.Stack
import scala.collection.mutable.HashSet
import java.io.BufferedWriter
import java.io.FileWriter

object F1800XMLToFreq {

  def main(args: Array[String]): Unit = {
    val ngrams = new HashSet[String]
    for (file <- new java.io.File("texts/").listFiles) {
      println("Processing: "+file)
      val xml = new XMLEventReader(Source.fromFile(file,"ISO-8859-1"))
      var break = false
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"text",_,_) => 
          while (!break) xml.next match {
             case EvElemStart(_,"foreign", _, _) =>
               while (!break) {
                 xml.next match {
                   case EvElemEnd(_,"foreign") => break = true
                   case _ => 
                 }
               }
               break = false
             case EvText(text) => text.split("\\PL+").filter(!_.isEmpty).foreach { token => ngrams+=token.toLowerCase }
             case EvElemEnd(_,"text") => break = true
             case _ => 
          }
        case _ =>
      }
    }
    val file = new java.io.File("tokens.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    ngrams.foreach(token => { bw.write(token);bw.newLine()})
    bw.close()
  }
}
