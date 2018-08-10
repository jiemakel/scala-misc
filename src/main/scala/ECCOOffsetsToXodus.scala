import java.io._

import com.typesafe.scalalogging.LazyLogging
import jetbrains.exodus.bindings.{IntegerBinding, StringBinding}
import jetbrains.exodus.env.{EnvironmentConfig, Environments, StoreConfig}
import jetbrains.exodus.util.LightOutputStream
import jetbrains.exodus.{ByteIterable, CompoundByteIterable}
import org.rogach.scallop.ScallopConf

import scala.concurrent.ExecutionContext
import scala.io.Source

object ECCOOffsetsToXodus extends LazyLogging {
  
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)

  
  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val directories = trailArg[List[String]](required = true)
      val dest = opt[String](required = true)
      verify()
    }
    val e = Environments.newContextualInstance(opts.dest(), new EnvironmentConfig().setLogFileSize(Int.MaxValue+1l))
    e.clear()
    val s = e.openStore("coords", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING)
    e.beginExclusiveTransaction()
    implicit val iec = ExecutionContext.Implicits.global
    val kbi = new Array[ByteIterable](2)
    val bytes = new Array[Int](4)
    for (
        dir <- opts.directories().toStream;
        fd = new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!");
        file <- getFileTree(fd) if file.getName.endsWith("-payload.csv")
    ) {
      val documentID = Source.fromFile(file.getPath.replace("-payload.csv","-id.txt")).getLines.next
      logger.info("Processing "+documentID+":"+file)
      kbi(0) = StringBinding.stringToEntry(documentID)
      for (
        curline <- Source.fromFile(file).getLines
      ) {
        val entries = curline.split(',').map(_.toInt).toSeq
        kbi(1) = IntegerBinding.intToCompressedEntry(entries.head)
        val vlos = new LightOutputStream()
        for (e <- entries.tail)
          IntegerBinding.writeCompressed(vlos,e,bytes)
        val k = new CompoundByteIterable(kbi)
        s.add(k, vlos.asArrayByteIterable())
      }
    }
    e.getCurrentTransaction.commit()
    e.close()
  }
}
