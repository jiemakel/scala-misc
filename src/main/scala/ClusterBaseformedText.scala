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
import scala.jdk.CollectionConverters._
import cc.mallet.types.InstanceList
import cc.mallet.types.Instance
import cc.mallet.topics.ParallelTopicModel
import cc.mallet.types.FeatureSequence
import java.util.Arrays
import cc.mallet.pipe.CharSequenceLowercase
import cc.mallet.pipe.TokenSequenceRemoveStopwords
import java.io.File
import cc.mallet.pipe.TokenSequenceRemoveStopPatterns
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.DCTerms
import org.apache.jena.datatypes.xsd.XSDDatatype

object ClusterBaseformedText {
  def main(args: Array[String]): Unit = {
    val pipeList = new ArrayBuffer[Pipe]
    pipeList += new Input2CharSequence("UTF-8")
    pipeList += new CharSequenceLowercase()
    pipeList += new CharSequence2TokenSequence(Pattern.compile("[\\p{L}\\p{N}_]+"))
    val stopWordFilter = new TokenSequenceRemoveStopwords(new File("fistopwords.txt"),"UTF-8", false, false, false)
    stopWordFilter.addStopWords(Array("momentti", "laki", "luku"))
    pipeList += stopWordFilter
    pipeList += new TokenSequenceRemoveStopPatterns(Array("\\d+"))
    pipeList += new TokenSequence2FeatureSequenceWithBigrams()
    val instances = new InstanceList(new SerialPipes(pipeList.asJavaCollection))
    for (line <- Source.fromFile("ajantasa-content.txt").getLines.map(_.replaceAll("\\\\n"," "))) {
      val idContent = line.split("\\|")
      val id = idContent.head
      val content = idContent.tail.mkString("|")
      instances.addThruPipe(new Instance(content,null,id,id))
    }
    val numTopics = 500
    val numIterations = 2000
    val model = new ParallelTopicModel(numTopics, 1.0, 0.01)
    model.addInstances(instances)
    model.setNumThreads(Runtime.getRuntime.availableProcessors())
    model.setNumIterations(numIterations)
    model.estimate()
    
    val dataAlphabet = instances.getDataAlphabet
    
    val topicSortedWords = model.getSortedWords
    
    var w = new PrintWriter("ajantasa-topics.nt")
    val numKeywords = 5
    for (i <- 0 until numTopics) {
      val topic = "<http://ldf.fi/finlex/topic_"+i+">"
      val kws = topicSortedWords.get(i).asScala.take(numKeywords).map(o => (dataAlphabet.lookupObject(o.getID),o.getWeight))
      for ((kw,j) <- kws.zipWithIndex) {
        val kwi = "<http://ldf.fi/finlex/topic_"+i+"_keyword_"+j+">"
        w.println(topic+" <"+DCTerms.subject+"> "+kwi+" .")
        w.println(kwi+" <"+RDF.value+"> \""+kw._1+"\" .")
        w.println(kwi+" <http://ldf.fi/finlex/schema/weight> \""+kw._2+"\"^^<"+XSDDatatype.XSDdouble.getURI+"> .")
      }
    }
    w.close()
    w = new PrintWriter("ajantasa-partToTopic.nt")
    val numTopicsPerInstance = 5
    for (i <- 0 until instances.size) {
      val topics = model.getTopicProbabilities(i).zipWithIndex.toSeq.filter(_._1>0.2).sortWith(_._1 > _._1).take(numTopicsPerInstance)
      for ((topic,j) <- topics.zipWithIndex) {
        val tpi = "<http://ldf.fi/finlex/instanceToTopic_"+i+"_"+j+">"
        val topicI = "<http://ldf.fi/finlex/topic_"+topic._2+">"
        w.println(instances.get(i).getName+" <"+DCTerms.subject+"> "+tpi+" .")
        w.println(tpi+" <"+RDF.value+"> "+topicI+" .")
        w.println(tpi+" <http://ldf.fi/finlex/schema/weight> \""+topic._1+"\"^^<"+XSDDatatype.XSDdouble.getURI+"> .")
      }
    }
    w.close()    
      
  }
}
