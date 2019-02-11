import java.io.File
import java.nio.file.{Files, Paths}

import au.com.bytecode.opencsv.CSVParser
import com.bizo.mighty.csv._
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ECCOTCPAligner extends ParallelProcessor {

  def levenshteinSubstringCosts(needle: String, haystack: String): Seq[(Int,Int,Int)] = {
    if (haystack.isEmpty) return Seq((0,0,needle.length))
    var row1 = new Array[Int](haystack.length + 1)
    var rlen1 = new Array[Int](haystack.length + 1)
    for (i <- 0 until needle.length) {
      val row2 = new Array[Int](haystack.length + 1)
      val rlen2 = new Array[Int](haystack.length + 1)
      row2(0) = i + 1
      rlen2(0) = i + 1
      for (j <- 0 until haystack.length) {
        val sub = (row1(j) + (if (needle(i) != haystack(j)) 1 else 0),rlen1(j) + 1)
        val del = (row1(j+1)+1,rlen1(j+1))
        val ins = (row2(j)+1,rlen2(j) + 1)
        val (opc,opl) = List(ins,sub,del).minBy(_._1)
        row2(j+1) = opc
        rlen2(j+1) = opl
      }
      row1 = row2
      rlen1 = rlen2
    }
    row1.zip(rlen1).zipWithIndex.map{
      case ((cost,len),ind) => (ind-len,len,cost)
    }.filter(_._1>=0)
  }

  def process(tcpp: String, galepagerows: ArrayBuffer[Row], galef: String, galew: CSVWriter,tcpid: String, galeid: String, llastline: String): String = {
    if (!Files.exists(Paths.get(tcpp + galepagerows(0)(0) + ".txt"))) {
      logger.warn(tcpp + galepagerows(0)(0) + ".txt does not exist for " + galef + "!")
      ""
    } else {
      val tcppagelines = Seq(llastline) ++ Source.fromFile(tcpp + galepagerows(0)(0) + ".txt").getLines().toSeq
      val lastline = tcppagelines.last
      val tcppage = tcppagelines.mkString("\n")
      val galepage = galepagerows.map(_ (0)).mkString("\n")
      var galepageoffset = 0
      for ((galerow,index) <- galepagerows.zipWithIndex) {
        val rowtext = galerow(5)
        val tcpoptimalind = tcppage.length * galepageoffset / galepage.length
        val costs = levenshteinSubstringCosts(rowtext, tcppage).map { case (ind, len, cost) => (cost, ind, len) }
        var (optimalcost, optimalind, optimallen) = if (costs.isEmpty) (rowtext.length,0,math.min(tcppage.length,rowtext.length)) else costs.min
        val optimalind2 = tcppage.substring(0,optimalind).lastIndexWhere {
          case a if !a.isLetterOrDigit => true
          case _ => false
        } + 1
        var optimalendind = optimalind + optimallen
        val optimalendindincr = tcppage.substring(optimalendind).indexWhere {
          case a if !a.isLetterOrDigit => true
          case _ => false
        }
        val optimalendind2 = if (optimalendindincr != -1) optimalendind + optimalendindincr else optimalendind
        val silverrow = tcppage.substring(optimalind, optimalendind).trim
        galew.write(Seq(tcpid,galeid,galerow.head,""+(index+1)) ++ galerow.tail ++ Seq(silverrow, "" + optimalcost, "" + optimalind, "" + (optimalind != optimalind2), "" + (optimalendind != optimalendind2)))
        galepageoffset += rowtext.length + 1
      }
      lastline
    }
  }

  def process(tcpp: String, galef: String, dest: String): Unit = {
    logger.info("Processing pair "+tcpp+":"+galef)
    var galeid = galef.substring(galef.lastIndexOf('/')+1)
    galeid = galeid.substring(0,galeid.indexOf('-'))
    var tcpid = tcpp.substring(tcpp.lastIndexOf('/')+1)
    tcpid = tcpid.substring(0,tcpid.indexOf('_'))
    val galed = dest+"/"+galef.substring(galef.lastIndexOf('/')+1).replaceAllLiterally(".csv","-silver.csv")
    val galew = CSVWriter(galed)(CSVWriterSettings.Standard.copy(escapechar = '\\'))
    var page = "0"
    val galepagerows = new ArrayBuffer[Row]
    var lastline = ""
    for (row <- CSVReader(galef)(CSVReaderSettings.Standard.copy(escapechar =  CSVParser.NULL_CHARACTER))) {
      if (row.length!=6) logger.error("Bad row: "+row.toSeq)
      else {
        if (page!=row(0)) {
          if (page != "0")
            lastline = process(tcpp, galepagerows,galef,galew,tcpid,galeid,lastline)
          galepagerows.clear()
          page = row(0)
        }
        galepagerows += row
      }
    }
    process(tcpp, galepagerows,galef,galew,tcpid,galeid,lastline)
    galew.close()
    logger.info("Wrote "+galed)
  }

  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val partsFile = trailArg[String](required = true)
      val dest = opt[String](required = true)
      verify()
    }
    val dest = opts.dest()
    new File(dest).mkdirs()
    feedAndProcessFedTasksInParallel(() => {
      for ((tcpp,galef) <- Source.fromFile(opts.partsFile()).getLines.map(l => {
        val s = l.indexOf(' ')
        (l.substring(0,s),l.substring(s+1))
      })) addTask(tcpp+":"+galef,() => process(tcpp,galef,dest))
    })
    logger.info("All done!")
  }
}
