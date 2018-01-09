import java.io.File
import java.nio.ByteBuffer

import org.rogach.scallop._
import com.sleepycat.je._

import scala.io.Source
import scala.language.{postfixOps, reflectiveCalls}

object ECCOClusterMappingDBCreator extends ParallelProcessor {

  var db: Database = null

  def index(file: File): Unit = {
    val lines = Source.fromFile(file).getLines()
    lines.next()
    val keya = new Array[Byte](java.lang.Long.BYTES+java.lang.Integer.BYTES*2)
    val valuea = new Array[Byte](java.lang.Integer.BYTES*2)
    val dkey = new DatabaseEntry(keya)
    val dvalue = new DatabaseEntry(valuea)
    val kbb = ByteBuffer.wrap(keya)
    val vbb = ByteBuffer.wrap(valuea)
    for (line <- lines) {
      val documentIdEnd = line.indexOf(',')
      kbb.putLong(0,line.substring(0,documentIdEnd).toLong)
      val clusterIdEnd = line.indexOf(',',documentIdEnd+1)
      kbb.putInt(java.lang.Long.BYTES, line.substring(documentIdEnd+1, clusterIdEnd).toInt)
      val apiStartIndexEnd = line.indexOf(',',clusterIdEnd+1)
      kbb.putInt(java.lang.Long.BYTES + java.lang.Integer.BYTES, line.substring(clusterIdEnd+1,apiStartIndexEnd).toInt)
      val realStartIndexEnd = line.indexOf(',',apiStartIndexEnd+1)
      vbb.putInt(0,line.substring(apiStartIndexEnd+1,realStartIndexEnd).toInt)
      vbb.putInt(java.lang.Integer.BYTES, line.substring(realStartIndexEnd+1).toInt)
      db.put(null,dkey,dvalue)
    }
    logger.info("Processed "+file)
  }

  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val mappingsDb = opt[String](required = true)
      val directories = trailArg[List[String]](required = false)
      verify()
    }
    val envDir = new File(opts.mappingsDb())
    envDir.mkdirs()
    val env = new Environment(envDir,new EnvironmentConfig().setAllowCreate(true).setTransactional(false).setConfigParam(EnvironmentConfig.LOG_FILE_MAX,"1073741824"))
    db = env.openDatabase(null, "sampleDatabase", new DatabaseConfig().setAllowCreate(true).setDeferredWrite(true).setTransactional(false))
    feedAndProcessFedTasksInParallel(() =>
      opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".csv")).foreach(file => addTask(file.getPath, () => index(file)))
    )
/*    val keya = new Array[Byte](java.lang.Long.BYTES+java.lang.Integer.BYTES*2)
    val dkey = new DatabaseEntry(keya)
    val dvalue = new DatabaseEntry()
    val kbb = ByteBuffer.wrap(keya)
    kbb.putLong(0,"0506700400".toLong)
    kbb.putInt(java.lang.Long.BYTES,"162855".toInt)
    kbb.putInt(java.lang.Long.BYTES+java.lang.Integer.BYTES,"137667".toInt)
    db.get(null,dkey,dvalue, LockMode.DEFAULT)
    val vbb = ByteBuffer.wrap(dvalue.getData)
    println(vbb.getInt+","+vbb.getInt)*/
    db.close()
    env.close()
  }
}
