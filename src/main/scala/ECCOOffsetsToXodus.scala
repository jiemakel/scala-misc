import java.io._

import com.typesafe.scalalogging.LazyLogging
import jetbrains.exodus.bindings.{IntegerBinding, StringBinding}
import jetbrains.exodus.env.{Environments, StoreConfig}
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
    val dest = new File(opts.dest()).getAbsolutePath
    val e = Environments.newInstance(dest)
    e.clear()
    val t = e.beginExclusiveTransaction()
    val s = e.openStore("coords", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, t)
    implicit val iec = ExecutionContext.Implicits.global
    val kbi = new Array[ByteIterable](2)
    val k = new CompoundByteIterable(kbi)
    val vbi = new Array[ByteIterable](5)
    val v = new CompoundByteIterable(vbi)
    for (
        dir <- opts.directories().toStream;
        fd = new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!");
        file <- getFileTree(fd) if file.getName.endsWith("-payload.csv");
        documentID = StringBinding.stringToEntry(Source.fromFile(file.getPath.replace("-payload.csv","-id.txt")).getLines.next)
    ) {
      logger.info("Processing "+file)
      kbi(0) = documentID
      for (
        curline <- Source.fromFile(file).getLines
      ) {
        val entries = curline.split(',').map(_.toInt)
        kbi(1) = IntegerBinding.intToCompressedEntry(entries(0))
        vbi(0) = IntegerBinding.intToCompressedEntry(entries(1))
        vbi(1) = IntegerBinding.intToCompressedEntry(entries(2))
        vbi(2) = IntegerBinding.intToCompressedEntry(entries(3))
        vbi(3) = IntegerBinding.intToCompressedEntry(entries(4))
        vbi(4) = IntegerBinding.intToCompressedEntry(entries(5))
        s.put(t, k, v)
      }
    }
    t.commit()
    e.close()
  }
}
