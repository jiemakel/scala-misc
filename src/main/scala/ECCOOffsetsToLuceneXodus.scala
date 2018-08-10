import java.nio.file.FileSystems

import com.typesafe.scalalogging.LazyLogging
import jetbrains.exodus.bindings.{IntegerBinding, StringBinding}
import jetbrains.exodus.env.{EnvironmentConfig, Environments, StoreConfig}
import jetbrains.exodus.{ByteIterable, CompoundByteIterable}
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
    val se = Environments.newContextualInstance(opts.source(), new EnvironmentConfig().setLogFileSize(Int.MaxValue+1l))
    se.beginReadonlyTransaction()
    val ss = se.openStore("coords", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING)
    val e = Environments.newContextualInstance(opts.dest(), new EnvironmentConfig().setLogFileSize(Int.MaxValue+1l))
    e.clear()
    val s = e.openStore("offsetdata", StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING)
    e.beginExclusiveTransaction()
    val sc = ss.openCursor()
    implicit val iec = ExecutionContext.Implicits.global
    val kbi = new Array[ByteIterable](2)
    val vbi = new Array[ByteIterable](5)
    val ir = DirectoryReader.open(new MMapDirectory(FileSystems.getDefault.getPath(opts.index()))).leaves().get(0).reader
    logger.info("Going to process " + ir.maxDoc + " documents in " + opts.index() + ".")
    val dv = DocValues.getSorted(ir, "documentID")
    for (d <- 0 until ir.maxDoc) {
      dv.advance(d)
      val skey = StringBinding.stringToEntry(dv.binaryValue.utf8ToString)
      kbi(0) = IntegerBinding.intToCompressedEntry(d)
      sc.getSearchKeyRange(skey)
      do {
        kbi(1) = sc.getKey.subIterable(skey.getLength, sc.getKey.getLength - skey.getLength)
        s.put(new CompoundByteIterable(kbi), sc.getValue)
      } while (sc.getNext && sc.getKey.subIterable(0, skey.getLength).equals(skey))
    }
    e.getCurrentTransaction.commit()
    e.close()
  }
}
