import scala.io.Source
import scala.collection.mutable.HashSet
import java.io.PrintWriter
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import cc.mallet.pipe.Input2CharSequence
import cc.mallet.pipe.Pipe
import cc.mallet.pipe.CharSequence2TokenSequence
import java.util.regex.Pattern
import cc.mallet.pipe.TokenSequenceLowercase
import cc.mallet.pipe.TokenSequence2FeatureSequence
import cc.mallet.pipe.TokenSequence2FeatureSequenceWithBigrams
import cc.mallet.pipe.Target2Label
import cc.mallet.pipe.FeatureSequence2FeatureVector
import cc.mallet.pipe.PrintInputAndTarget
import cc.mallet.pipe.SerialPipes
import scala.collection.JavaConversions._
import cc.mallet.types.InstanceList
import cc.mallet.types.Instance
import cc.mallet.topics.ParallelTopicModel
import cc.mallet.types.FeatureSequence
import java.util.Arrays
import cc.mallet.pipe.CharSequenceLowercase
import cc.mallet.pipe.TokenSequenceRemoveStopwords
import java.io.File
import cc.mallet.pipe.TokenSequenceRemoveStopPatterns
import org.json4s._
import org.json4s.native.JsonMethods._

object MarkAnnotations {
  def main(args: Array[String]): Unit = {
    val pw = new java.io.PrintWriter(new File("ner_testidata_organizations-tagged-fuzzy-new.bin"))
    for (file <- new java.io.File("ner_testidata/").listFiles.filter(_.getName.endsWith(".in")).sorted) {
      val annotations = (parse(new java.io.File("ner_testidata/organizations/"+file.getName.replace(".in","-organizations.out"))) \\ "matches" \\ classOf[JString])
      val sannotations = annotations.filter(_.indexOf(' ') == -1).toSet
      val mannotations = annotations.filter(_.indexOf(' ') != -1).sortBy(_.length).reverse
      var text = Source.fromFile(file).getLines().mkString
      for (an <- mannotations) text = text.replaceAllLiterally(an, "<EnamexOrgXxx>"+an+"\t</EnamexOrgXxx>")
      var inMulti = 0
      for (word <-text.split(" +")) {
        if (word.startsWith("<EnamexOrgXxx>")) {
          pw.println(word.replaceAllLiterally("<EnamexOrgXxx>","")+"\t<EnamexOrgXxx>")
          inMulti += "<EnamexOrgXxx>".r.findAllIn(word).length
        } else if (word.contains("\t</EnamexOrgXxx>")) {
          inMulti -= "</EnamexOrgXxx>".r.findAllIn(word).length 
          if (inMulti==0)
            pw.println(word.replaceAllLiterally("\t</EnamexOrgXxx>","")+"\t</EnamexOrgXxx>")
          else 
            pw.println(word.replaceAllLiterally("\t</EnamexOrgXxx>","")+"\t<EnamexOrgXxx>")
        } else if (inMulti>0) pw.println(word+"\t<EnamexOrgXxx>")
        else if (sannotations.contains(word) || sannotations.contains(word.replaceAll("\\W*$",""))) pw.println(word+"\t<EnamexOrgXxx/>")
        else pw.println(word+"\tO")
      }
    }
    pw.close()
  }
}

