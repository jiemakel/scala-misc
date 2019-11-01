import java.io.{FileInputStream, InputStreamReader}

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import org.json4s._
import org.json4s.native.JsonParser._

import scala.util.Try


object CreateCEECFreqCSV {
  def main(args: Array[String]): Unit = {
    val r = CSVReader.open("ceec/ceec-normalised/ceec-noncode-freqs.csv")
    val w = CSVWriter.open("ceec/ceec-normalised/ceec-noncode-allfreqs.csv")
    w.writeRow(Seq("word","ceecfreq","bnfreq","eccofreq"))
    for (row <- r) {
      val bnfreq = Try({
        val JInt(freq) = parse(new InputStreamReader(new FileInputStream("ceec/bn-freqs/"+row(1)+".json"))) \ "results" \ "totalTermFreq"
        ""+freq
      }).getOrElse("0")
      val eccofreq = Try({
        val JInt(freq) = parse(new InputStreamReader(new FileInputStream("ceec/ecco-freqs/"+row(1)+".json"))) \ "results" \ "totalTermFreq"
        ""+freq
      }).getOrElse("0")
      w.writeRow(Seq(row(1),row(0),bnfreq,eccofreq))
    }
    w.close()
  }
}