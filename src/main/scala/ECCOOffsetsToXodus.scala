import java.io._

import com.typesafe.scalalogging.LazyLogging
import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.bindings.{IntegerBinding, StringBinding}
import jetbrains.exodus.env._
import jetbrains.exodus.util.LightOutputStream
import org.rogach.scallop.ScallopConf

import scala.io.Source

object ECCOOffsetsToXodus extends LazyLogging {
  
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
      else Stream.empty)

  val bytes = new Array[Int](4)

  def processFile(file: File): Unit = {
    var fs = Source.fromFile(file.getPath.replace("-payload.csv","-id.txt"))
    val documentID = StringBinding.stringToEntry(fs.getLines.next)
    fs.close()
    fs = Source.fromFile(file)
    val t = e.beginExclusiveTransaction()
    for (curline <- fs.getLines) {
      val entries = curline.split(',').map(_.toInt).toSeq
      val offset = IntegerBinding.intToCompressedEntry(entries.head)
      val vlos = new LightOutputStream()
      for (e <- entries.tail)
        IntegerBinding.writeCompressed(vlos,e,bytes)
      s.add(new CompoundByteIterable(Array(documentID,offset)), vlos.asArrayByteIterable())
    }
    t.commit()
    fs.close()
  }

  var e: ContextualEnvironment = _
  var s: ContextualStore = _

  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val directories = trailArg[List[String]](required = true)
      val dest = opt[String](required = true)
      verify()
    }
    e = Environments.newContextualInstance(opts.dest(), new EnvironmentConfig().setLogFileSize(Int.MaxValue+1l).setMemoryUsage(1073741824l))
    e.clear()
    s = e.openStore("coords", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING)
    for (
        dir <- opts.directories().toStream;
        fd = new File(dir);
        _ = if (!fd.exists()) logger.warn(dir+" doesn't exist!");
        file <- getFileTree(fd) if file.getName.endsWith("-payload.csv")
    ) processFile(file)
    //t.commit()
    e.close()
  }
}
