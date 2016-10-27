import org.apache.jena.query.QueryExecutionFactory
import com.bizo.mighty.csv.CSVReader
import org.apache.jena.query.ParameterizedSparqlString
import collection.JavaConverters._
import com.bizo.mighty.csv.CSVWriter
import scala.collection.mutable.ArrayBuffer

object CEECQueryToCSV {
  val pss = new ParameterizedSparqlString("PREFIX text: <http://jena.apache.org/text#>\n"+
"PREFIX cs: <http://ldf.fi/ceec-schema#>\n"+
"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"+
"SELECT DISTINCT ?id ?fulltext WHERE {\n"+
" BIND(CONCAT(\"\\\"\",REPLACE(?query,\"([\\\\+\\\\-\\\\&\\\\|\\\\!\\\\(\\\\)\\\\{\\\\}\\\\[\\\\]\\\\^\\\\\\\"\\\\~\\\\*\\\\?\\\\:\\\\/\\\\\\\\])\",\"\\\\\\\\$1\"),\"\\\"\") AS ?escaped_query)\n"+
"  ?id text:query (?escaped_query 1000000000) .\n"+
"  ?id cs:fulltext ?fulltext .\n"+
"  ?id cs:year ?year .\n"+
"  FILTER (REGEX(?fulltext, ?regexp,\"i\") && xsd:int(?year)>=1680)\n"+
"}");
  def main(args: Array[String]): Unit = {
    val r = CSVReader("fica-groupedWords-1680-.csv")
    val wr = CSVWriter("fica-groupedWords-unclear.csv")
    wr.write(Seq("id","offset","before","word","after","URL"))
    for (line <- r; if line(2)=="unclear") {
      pss.setLiteral("query", line(1))
      val regex = "(?i)((?:\\W|^)" + line(1).replace("[-\\/\\^$*+?.()|[\\]{}]", "\\$&") + "(?:\\W|$))"
      pss.setLiteral("regexp",regex)
      val qe = QueryExecutionFactory.sparqlService("http://ldf.fi/ceec/sparql", pss.toString())
      for (res <- qe.execSelect().asScala) {
        val id = res.getResource("id").getURI
        val text = res.getLiteral("fulltext").getString
        val parts = new ArrayBuffer[String]
        var li = 0
        for (m <- regex.r.findAllMatchIn(text)) {
          parts += text.substring(li,m.start)
          parts += m.group(1)
          li = m.end
        }
        parts += text.substring(li)
        var lastBefore: String = null
        var before: String = ""
        var after: String = ""
        if (parts(0).length > 120) {
          lastBefore = parts(0).substring(parts(0).length - 120)
          lastBefore = lastBefore.substring(lastBefore.indexOf(' ') + 1)
        } else lastBefore = parts(0)
        var pos = parts(0).length
        for (i <- 2 until parts.length by 2) {
          before = lastBefore
          if (parts(i).length > 120) {
            after = parts(i).substring(0, 120)
            after = after.substring(0, after.lastIndexOf(' '))
            lastBefore = parts(i).substring(parts(i).length - 120)
            lastBefore = lastBefore.substring(lastBefore.indexOf(' ') + 1)
          } else {
            after = parts(i)
            lastBefore = parts(i)
          }
          wr.write(Seq(id.substring(26),""+pos, before.replace("\n"," "),line(1),after.replace("\n"," "),"http://h89.it.helsinki.fi/ceec/func/letterFunc.jsp?letterID="+id.substring(26)))
          pos += parts(i - 1).length + parts(i).length
        }
      }
    }
    r.close
    wr.close
  }
}