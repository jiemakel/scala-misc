import java.nio.file.FileSystems

import com.typesafe.scalalogging.LazyLogging
import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.bindings.{IntegerBinding, StringBinding}
import jetbrains.exodus.env.{EnvironmentConfig, Environments, StoreConfig}
import org.apache.lucene.index.{DirectoryReader, DocValues}
import org.apache.lucene.store.MMapDirectory
import org.rogach.scallop.ScallopConf

import scala.concurrent.ExecutionContext

object ECCOOffsetsToLuceneXodus extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val index = opt[String](required = true)
      val source = opt[String](required = true)
      val dest = opt[String](required = true)
      verify()
    }
    val se = Environments.newContextualInstance(opts.source(), new EnvironmentConfig().setLogFileSize(Int.MaxValue+1l).setMemoryUsage(1073741824l))
    se.beginReadonlyTransaction()
    val ss = se.openStore("coords", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING)
    val e = Environments.newContextualInstance(opts.dest(), new EnvironmentConfig().setLogFileSize(Int.MaxValue+1l).setMemoryUsage(1073741824l))
    e.clear()
    val s = e.openStore("offsetdata", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING)
    val sc = ss.openCursor()
    implicit val iec = ExecutionContext.Implicits.global
    val ir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault.getPath(opts.index()))).leaves().get(0).reader
    logger.info("Going to process " + ir.maxDoc + " documents in " + opts.index() + ".")
    val dv = DocValues.getSorted(ir, "documentID")
    for (d <- 0 until ir.maxDoc) {
      dv.advance(d)
      val skey = StringBinding.stringToEntry(dv.binaryValue.utf8ToString)
      val dkey = IntegerBinding.intToCompressedEntry(d)
      sc.getSearchKeyRange(skey)
      val t = e.beginExclusiveTransaction()
      do {
        val offset = sc.getKey.subIterable(skey.getLength, sc.getKey.getLength - skey.getLength)
        s.put(new CompoundByteIterable(Array(dkey,offset)), sc.getValue)
      } while (sc.getNext && sc.getKey.subIterable(0, skey.getLength).equals(skey))
      t.commit()
    }
    e.close()
  }
}
