import scala.io.Source
import scala.collection.mutable.HashSet
import java.io.PrintWriter

object ProcessBaseformedText {
  def main(args: Array[String]): Unit = {
    val w = new PrintWriter("content.txt.lemmatized.keywords")
    for (line <- Source.fromFile("content.txt.lemmatized").getLines.map(_.replaceAll("\\\\n"," "))) {
      val set = new HashSet[String]
      for (token <- line.split(' ')) {
        val parts = token.split('#').filter(!_.isEmpty)
        set.add(parts.mkString(""))
/*        for (i <- 1 to parts.length) {
          var str = parts.take(i).mkString("")
          if (str.length>2)
            set.add(str)
          str = parts.takeRight(i).mkString("") 
          if (str.length>2)
            set.add(str)
        } */
      }
      w.println(set.mkString(" "))
    }
    w.close()
  }
}