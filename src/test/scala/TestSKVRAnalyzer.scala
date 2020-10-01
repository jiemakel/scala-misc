import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, PositionIncrementAttribute}
import org.junit.Test
import org.junit.Assert._

class TestSKVRAnalyzer {

  private def testAnalyzer(text: String, expectedS: String): Unit = {
    val r = new SKVRXMLIndexer.Reuse
    val f = r.contentField
    f.setValue(text)
    val t = f.tokenStream
    val a: CharTermAttribute = t.getAttribute(classOf[CharTermAttribute])
    val pi: PositionIncrementAttribute = t.getAttribute(classOf[PositionIncrementAttribute])
    t.reset()
    val expected = expectedS.split(" ")
    var i = 0
    while (t.incrementToken()) {
      assertEquals(expected(i),a.toString)
      i+=1
    }
    t.end()
    t.close()
  }

  @Test
  def testAnalyzer: Unit = {
    testAnalyzer(
      "∞15 Tuop_ol' vuaksuva#16† *n[uori]* ,[!],J[ougavoine],[!] €Juokahaińe€. \"Oi šie, ^mua̯mo^, kandai̭̯ańi, Ken °miuv_virren °kiit|tä|nöö,\n@Testaan@ *kaikkea* ki'vaa_mitä_ ¨#17¨ voi olla. Läks_on nogi#1 ˇnuorasestaˇ, joka\noli\n35 jotain∞",
      "tuop ol vuaksuva nuori jougavoine juokahaine juokahaińe oi sie šie muamo mua̯mo kandaiani kandai̭̯ańi ken miuv virren kiittänöö testaan kaikkea kivaa mitä voi olla läks on nogi nuorasesta joka oli jotain"
    )
  }

  @Test
  def testAnalyzer2: Unit = {
    testAnalyzer(
    "\"Vesi miun.\n£(Pakka^i^ńi£\n*V[aka] seb°üä°^i^l'i",
"vesi miun pakkaini pakkaińi vaka sebyäili sebüäili"
    )
  }

  @Test
  def testAnalyzer3: Unit = {
    testAnalyzer("*Nouzigo kolmen","nouzigo kolmen")
    testAnalyzer("\"Vesi","vesi")
    testAnalyzer("\"*seb°üä°^i^l'i","sebyäili sebüäili")
  }




}
