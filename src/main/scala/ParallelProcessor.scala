import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.ArrayBlockingQueue
import java.io.File
import java.io.StringWriter
import java.io.PrintWriter
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory
import java.util.concurrent.ForkJoinWorkerThread


class ParallelProcessor extends LazyLogging {
  
  private val numWorkers = sys.runtime.availableProcessors
  val availableMemory = Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
  private val queueCapacity = 10000
  private val fjp = new ForkJoinPool(numWorkers, new ForkJoinWorkerThreadFactory() {
     override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
       val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
       worker.setName("indexing-worker-" + worker.getPoolIndex())
       worker
     }
   }, null, true)
  private val indexingTaskExecutionContext = ExecutionContext.fromExecutorService(fjp)
  
  /** helper function to get a recursive stream of files for a directory */
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().sorted.toStream.flatMap(getFileTree)
      else Stream.empty)
      
  def getFileTreeSize(path: String): Long = getFileTree(new File(path)).foldLeft(0l)((s,f) => s+f.length)    
  
  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
  
    
  def addTask(id: String, taskFunction: () => Unit): Unit = {
    while (fjp.getQueuedTaskCount>queueCapacity) Thread.sleep(100)
    processingQueue.put(Future { 
      try {
        taskFunction()
      } catch {
        case cause: Exception =>
          logger.error("An error has occured processing source "+id+": " + getStackTraceAsString(cause))
          throw new Exception("An error has occured processing source "+id, cause)       
      }
    }(indexingTaskExecutionContext))
  }
  
  private val processingQueue = new ArrayBlockingQueue[Future[Unit]](queueCapacity)

  def feedAndProcessFedTasksInParallel(taskFeeder: () => Unit) {
    implicit val iec = ExecutionContext.Implicits.global 
    val all = Promise[Unit]()
    val poison = Future(())
    val sf = Future {
      taskFeeder()
      logger.info("All sources successfully fed.")
      processingQueue.put(poison)
    }
    sf.onComplete { 
      case Failure(t) => 
        logger.error("Feeding of at least one source resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t))
        processingQueue.put(poison)
      case Success(_) =>
    }
    var f = processingQueue.take()
    while (f ne poison) {
      Await.ready(f, Duration.Inf)
      f.onComplete { 
        case Failure(t) => if (!all.isCompleted) all.tryFailure(t)
        case Success(_) =>
      }
      f = processingQueue.take()
    }
    if (!all.isCompleted) all.trySuccess(Unit)
    all.future.onComplete {
      case Success(_) => logger.info("Successfully processed all sources.")
      case Failure(t) => logger.error("Processing of at least one source resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t))
    }
    indexingTaskExecutionContext.shutdown()
  }

}