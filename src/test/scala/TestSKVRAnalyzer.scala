import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, PositionIncrementAttribute}
import org.junit.Test
import org.junit.Assert._

class TestSKVRAnalyzer {

  @Test
  def testAnalyzer: Unit = {
    val r = new SKVRXMLIndexer.Reuse
    val f = r.contentField
    f.setValue("15 Tuop_ol' vuaksuva#16† *n[uori]* ,[!],J[ougavoine],[!] €Juokahaińe€. \"Oi šie, ^mua̯mo^, kandai̭̯ańi, Ken °miuv_virren °kiittänöö,\n@Testaan@ *kaikkea* ki'vaa_mitä_ ¨#17¨ voi olla. Läks_on nogi#1 ˇnuorasestaˇ, joka\noli\n35 jotain")
    val t = f.tokenStream
    val a: CharTermAttribute = t.getAttribute(classOf[CharTermAttribute])
    val pi: PositionIncrementAttribute = t.getAttribute(classOf[PositionIncrementAttribute])
    t.reset()
    val expected = "tuop ol vuaksuva nuori jougavoine juokahaine juokahaińe oi sie šie muamo mua̯mo kandaiani kandai̭̯ańi ken miuv virren kiittänöö testaan kaikkea kivaa mitä voi olla läks on nogi nuorasesta joka oli jotain".split(" ")
    var i = 0
    while (t.incrementToken()) {
      assertEquals(expected(i),a.toString)
      i+=1
    }
    t.end()
    t.close()

  }

}
