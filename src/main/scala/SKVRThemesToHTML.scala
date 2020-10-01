import java.io.{FileInputStream, InputStreamReader, PrintWriter}

import XMLEventReaderSupport._
import com.github.tototoshi.csv.CSVReader
import org.json4s.native.JsonParser._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SKVRThemesToHTML {

  private def readContents(element: String)(implicit xml: Iterator[EvEvent]): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_) =>
      case EvText(text,_)  => content.append(text.replaceAllLiterally("<","&lt;").replaceAllLiterally(">","&gt;"))
      case EvEntityRef(entity) => content.append("&"+entity+";")
      case EvComment(comment) =>
        println("Encountered comment: "+comment)
      case EvElemEnd(_,element) => break = true
    }
    content.toString.trim
  }

  def main(args: Array[String]): Unit = {

    val metadata = new mutable.HashMap[String,(Int,Int,Int)]
    val themePoems = new mutable.HashMap[Int, ArrayBuffer[(String,String,Boolean)]]
    val places = new mutable.HashMap[Int, (String, String)]
    val collectors = new mutable.HashMap[Int, String]

    for (row <- CSVReader.open("/Users/jiemakel/tyo/scala-misc-data/SKVR_kaikki_050218/metadata.csv"))
      metadata.put(row.head,(row(1).toInt,row(2).toInt,row(3).toInt))
    for (row <- CSVReader.open("/Users/jiemakel/tyo/scala-misc-data/SKVR_kaikki_050218/places.csv"))
      places.put(row.head.toInt, (row(1), row(2)))
    for (row <- CSVReader.open("/Users/jiemakel/tyo/scala-misc-data/SKVR_kaikki_050218/collectors.csv"))
      collectors.put(row.head.toInt, row(1))
    for (row <- CSVReader.open("/Users/jiemakel/tyo/scala-misc-data/SKVR_kaikki_050218/themerefs.csv"))
      themePoems.getOrElseUpdate(row.head.toInt, new ArrayBuffer[(String,String,Boolean)]) += ((row(3),row(1)+" "+row(2),row(4)=="*"))

    val l1h = new mutable.HashMap[String,String]
    val l2h = new mutable.HashMap[String,String]

    val w: PrintWriter = new PrintWriter("/Users/jiemakel/tyo/scala-misc-data/skvr-themes.html")

    w.append("<html><head><script>function o(themeId) { window.open('https://jiemakel.github.io/octavo-nui/#/search?endpoint=https%3A%2F%2Foctavo-acathoth.rahtiapp.fi%2Fskvr%2F&level=POEM&fieldEnricher=if%20%28field%3D%3D%22skvrID%22%29%20return%20%7B%20link%3A%20%22http%3A%2F%2Fskvr-webapp-acathoth.rahtiapp.fi%2Fruno%3Fnro%3D%22%2Bvalue%20%7D&offsetDataConverter=&field=collector&field=skvrID&field=year&field=skvrURL&field=region&field=content&field=theme&field=place&offset=0&limit=20&snippetLimit=1&contextLevel=Document&contextExpandLeft=0&contextExpandRight=0&sort=score&sortDirection=D&query=themeID:'+themeId) }\nfunction t(themeId) { var e = document.getElementById('t'+themeId); e.style.display = e.style.display === 'table' ? 'none' : 'table'}\nfunction p(poemId) { window.open('/runo?nro='+poemId,'f'); window.location.hash='f' }</script>\n<style>div { margin-top:1em; margin-bottom: 1em; }\n.t { background: lightgray;\n border-radius: 10px;\n padding: 10px;\n margin: 5px;}\nh4 { margin: 0px;\n margin-bottom: 1em; }\n table { display: none }\n</style></head><body onload=\"window.alert('Valmis!')\">\nSKVR-korpuksen keskeneräinen teemahakemisto kommentaareineen (19.2.2018). Sisällön korjausehdotukset lomakkeella <a href=\"https://www.finlit.fi/fi/skvr-palautelomake\">https://www.finlit.fi/fi/skvr-palautelomake</a>. Hakemisto on erittäin iso. Ennen käyttöä odota että selain näyttää viestin \"Valmis!\".<h1><a id=\"paahakemisto\">Päähakemisto</a></h1>\n<ul>\n")

    parse(new InputStreamReader(new FileInputStream("/Users/jiemakel/tyo/scala-misc-data/SKVR_kaikki_050218/themetree.json")), (p: Parser) => {
      var token = p.nextToken
      var code = ""
      var fl1h = true
      var fl2h = true
      while (token != End) {
        token match {
          case FieldStart("id") => code = p.nextToken.asInstanceOf[StringVal].value
          case FieldStart("label") =>
            if (!code.contains("_")) {
              val label = p.nextToken.asInstanceOf[StringVal].value
              if (code.length==3) {
                l1h.put(code,label)
                if (!fl2h)
                  w.append("  </ul></li>\n")
                if (!fl1h)
                  w.append("</ul></li>\n")
                else fl1h = false
                fl2h = true
                w.append("<li><a href=\"#"+code+"\">"+label+"</a><ul>\n")
              } else if (code.endsWith("00")) {
                l2h.put(code,label)
                if (!fl2h)
                  w.append("  </ul></li>\n")
                else fl2h = false
                w.append("  <li><a href=\"#"+code+"\">"+label+"</a><ul>\n")
              } else
                w.append("    <li><a href=\"#"+code+"\">"+label+"</a></li>\n")
            }
          case _ =>
        }
        token = p.nextToken
      }
    })
    w.append("</ul></ul></ul>\n")

    var s = new FileInputStream("/Users/jiemakel/tyo/scala-misc-data/SKVR_kaikki_050218/tyyppiluettelo.xml")
    implicit var xml = getXMLEventReader(s, "UTF-8")

    val m = new mutable.HashMap[String, String]

    var code = ""

    while (xml.hasNext) xml.next() match {
      case EvElemStart(_, "code", _) =>
        code = readContents("code")
      case EvElemStart(_, "title_1", _) =>
        m.put(readContents("title_1"), code)
      case EvElemStart(_, "title_2", _) =>
        val title = readContents("title_2")
        if (title.nonEmpty) m.put(title, code)
      case _ =>
    }

    val e = m.toSeq.sortWith { case ((k1, _), (k2, _)) => k1.length > k2.length }

    s = new FileInputStream("/Users/jiemakel/tyo/scala-misc-data/SKVR_kaikki_050218/tyyppiluettelo.xml")
    xml = getXMLEventReader(s, "UTF-8")

    var cl1h = ""
    var cl2h = ""
    var codeI = 0
    var codeS = ""
    while (xml.hasNext) xml.next() match {
      case EvElemStart(_, "main_title", _) =>
        val mtitle = readContents("main_title")
        val code = mtitle.takeWhile(_ != ' ')
        val ll1h = cl1h
        val ll2h = cl2h
        cl1h = code.substring(0,3)
        cl2h = code.substring(0,5)+"00"
        if (cl1h!=ll1h && l1h.contains(cl1h)) w.append("<h1><a id=\""+cl1h+"\" href=\"#paahakemisto\">"+l1h(cl1h)+"</a></h1>\n")
        if (cl2h!=ll2h && l2h.contains(cl2h)) w.append("<h2><a id=\""+cl2h+"\" href=\"#paahakemisto\">"+l2h(cl2h)+"</a></h2>\n")
        if (cl2h!=code) w.append("<h3><a id=\""+code+"\" href=\"#paahakemisto\">"+mtitle+"</a></h3>\n")
      case EvElemStart(_, "type", _) => w.append("<div class=\"t\"><h4>")
      case EvElemEnd(_, "type") => w.append("</div>")
      case EvElemStart(_, "code", _) =>
        codeS = readContents("code")
        codeI = (codeS.substring(1, 7) + codeS.substring(8)).toInt
      case EvElemStart(_, "title_1", _) =>
        if (l1h.contains(cl1h))
          w.append("<a href=#"+cl1h+">"+l1h(cl1h)+"</a> &gt; ")
        if (l2h.contains(cl2h))
          w.append("<a href=#"+cl2h+">"+l2h(cl2h)+"</a> &gt; ")
        //w.append("<a id=\""+codeS+"\" target=\"_blank\" href=\"https://jiemakel.github.io/octavo-nui/#/search?endpoint=https%3A%2F%2Foctavo-acathoth.rahtiapp.fi%2Fskvr%2F&level=POEM&fieldEnricher=if%20%28field%3D%3D%22skvrID%22%29%20return%20%7B%20link%3A%20%22http%3A%2F%2Fskvr-webapp-acathoth.rahtiapp.fi%2Fruno%3Fnro%3D%22%2Bvalue%20%7D&field=collector&field=skvrID&field=year&field=skvrURL&field=region&field=content&field=theme&field=place&contextLevel=Document&sort=region&query=themeID:" + codeI + "\">"+readContents("title_1")+"</a>")
        w.append("<a id=\""+codeS+"\">"+readContents("title_1")+"</a>")
        w.append("</h4>\n")
      case EvElemStart(_, "title_2", _) =>
        val t = readContents("title_2")
        if (t.nonEmpty) {
          w.append("<b>Vanhat nimitykset: </b>")
          w.append(t)
          w.append("<br />\n")
        }
      case EvElemStart(_, "title_3", _) =>
        var t = readContents("title_3")
        if (t.nonEmpty) {
          e.foreach(p => t = t.replaceAllLiterally(p._1, "<a href=\"#" + p._2 + "\">" + p._1 + "</a>"))
          w.append("<b>Samankaltaiset runoaihelmat: </b>")
          w.append(t)
          w.append("\n")
        }
      case EvElemStart(_, "notes", _) =>
        w.append("<div>")
        var notes = readContents("notes")
        e.foreach(p => notes = notes.replaceAllLiterally(p._1, "<a href=\"#" + p._2 + "\">" + p._1 + "</a>"))
        w.append(notes)
        w.append("</div>\n")
        val poems = themePoems.getOrElse(codeI, Seq.empty)
        w.append("<div><a href=\"javascript:o('" + codeI + "')\">Octavo</a> / <a href=\"javascript:t('"+codeS+"')\">Runot</a> ("+poems.length+")</div>\n")
        w.append("<table id=\"t"+codeS+"\">\n")
        poems.foreach{
          case (poem,poemRef,tenuous) if metadata.contains(poem) =>
            val (year,placeId,collectorId) = metadata(poem)
            val (region,place) = places(placeId)
            w.append("<tr><td><a href=\"javascript:p('"+poem+"')\">"+poemRef+"</a>"+(if (tenuous) "*" else "")+"</td><td>"+year+"</td><td>"+collectors(collectorId)+"</td><td>"+region+"</td><td>"+place+"</td></tr>\n")
          case (poem,poemRef,tenuous) => println("Unknown ID "+poem+","+poemRef+","+tenuous)
        }
        w.append("</table>\n")
      case _ =>
    }

    w.append("<iframe name=\"f\" id=\"f\" style=\"width:100%;height:100%\"></iframe></body></html>\n")

    w.close()
  }
}
