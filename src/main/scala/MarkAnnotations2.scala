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

object MarkAnnotations2 {
  def main(args: Array[String]): Unit = {
    var cl = ""
    val pw = new java.io.PrintWriter(new File("ocrtesti_fr11.in"))
    for (line <- Source.fromFile("ocrtesti_fr11").getLines()) {
      cl+=line
      if (line.endsWith(".") && line.length()>5) {
        pw.println(cl.trim)
        cl = ""
      } else cl+=" "
    }
  }
}
