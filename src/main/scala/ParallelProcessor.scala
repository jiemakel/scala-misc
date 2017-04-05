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


class ParallelProcessor extends LazyLogging {
  
  private val numWorkers = sys.runtime.availableProcessors
  private val queueCapacity = 1000
  private val ec = ExecutionContext.fromExecutorService(
   new ThreadPoolExecutor(
     numWorkers, numWorkers,
     0L, TimeUnit.SECONDS,
     new ArrayBlockingQueue[Runnable](queueCapacity) {
       override def offer(e: Runnable) = {
         put(e)
         true
       }
     }
   )
  )
  
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
    processingQueue.put(Future { 
      try {
        taskFunction()
      } catch {
        case cause: Exception =>
          logger.error("An error has occured processing source "+id+": " + getStackTraceAsString(cause))
          throw new Exception("An error has occured processing source "+id, cause)       
      }
    }(ec))
  }
  
  private val processingQueue = new ArrayBlockingQueue[Future[Unit]](queueCapacity)

  def feedAndProcessFedTasksInParallel(taskFeeder: () => Unit) {
    implicit val iec = ExecutionContext.Implicits.global 
    val all = Promise[Unit]()
    val poison = Future(())
    val sf = Future {
      taskFeeder()
      logger.info("All sources processed.")
      processingQueue.put(poison)
    }
    sf.onComplete { 
      case Failure(t) => logger.error("Processing of at least one source resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t))
      case Success(_) =>
    }
    var f = processingQueue.take()
    while (f ne poison) {
      Await.ready(f, Duration.Inf)
      f.onComplete { 
        case Failure(t) => if (!all.isCompleted) all.failure(t)
        case Success(_) =>
      }
      f = processingQueue.take()
    }
    if (!all.isCompleted) all.success(Unit)
    all.future.onComplete {
      case Success(_) => logger.info("Successfully processed all sources.")
      case Failure(t) => logger.error("Processing of at least one source resulted in an error:" + t.getMessage+": " + getStackTraceAsString(t))
    }
    ec.shutdown()
  }

}