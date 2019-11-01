import java.io.File

import scala.io.Source

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
