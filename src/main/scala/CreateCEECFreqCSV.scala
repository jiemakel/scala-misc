import com.bizo.mighty.csv.CSVReader
import com.bizo.mighty.csv.CSVWriter
import org.json4s._
import org.json4s.native.JsonParser._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods.{render, pretty}
import java.io.InputStreamReader
import java.io.FileInputStream
import scala.util.Try


object CreateCEECFreqCSV {
  def main(args: Array[String]): Unit = {
    val r = CSVReader("ceec/ceec-normalised/ceec-noncode-freqs.csv")
    val w = CSVWriter("ceec/ceec-normalised/ceec-noncode-allfreqs.csv")
    w.write(Seq("word","ceecfreq","bnfreq","eccofreq"))
    for (row <- r) {
      val bnfreq = Try({
        val JInt(freq) = parse(new InputStreamReader(new FileInputStream("ceec/bn-freqs/"+row(1)+".json"))) \ "results" \ "totalTermFreq"
        ""+freq
      }).getOrElse("0")
      val eccofreq = Try({
        val JInt(freq) = parse(new InputStreamReader(new FileInputStream("ceec/ecco-freqs/"+row(1)+".json"))) \ "results" \ "totalTermFreq"
        ""+freq
      }).getOrElse("0")
      w.write(Seq(row(1),row(0),bnfreq,eccofreq))
    }
    w.close()
  }
}