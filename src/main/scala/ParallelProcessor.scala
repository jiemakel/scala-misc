import java.io.{File, PrintWriter, StringWriter}
import java.util.concurrent.{ArrayBlockingQueue, ForkJoinPool}

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}


class ParallelProcessor extends LazyLogging {
  
  private val numWorkers = sys.runtime.availableProcessors
  val availableMemory = Runtime.getRuntime.maxMemory - (Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory)
  private val queueCapacity = 1024
  private val fjp = new ForkJoinPool(numWorkers, (pool: ForkJoinPool) => {
    val worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
    worker.setName("indexing-worker-" + worker.getPoolIndex)
    worker
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
  
  def createHashDirectories(dest: String): Unit = {
    for (
        i <- 0 to 9;
        j <- 0 to 9) new File(dest+"/"+i+"/"+j).mkdirs()
  }
  
  private var tasks = 0
    
  def addTask(id: String, taskFunction: () => Unit): Unit = {
    tasks += 1
    while (fjp.getQueuedTaskCount>queueCapacity) Thread.sleep(500)
    processingQueue.put(Future { 
      try {
        taskFunction()
      } catch {
        case cause: Exception =>
          logger.error("An error has occured processing source "+id,cause)
          throw new Exception("An error has occured processing source "+id, cause)       
      }
    }(indexingTaskExecutionContext))
  }
  
  private val processingQueue = new ArrayBlockingQueue[Future[Unit]](queueCapacity)

  def feedAndProcessFedTasksInParallel(taskFeeder: () => Unit) {
    implicit val iec = ExecutionContext.Implicits.global 
    val all = Promise[Unit]()
    val poison = Future(())
    val failures = new ArrayBuffer[Throwable]
    val sf = Future {
      taskFeeder()
      logger.info("All sources successfully fed, producing a total of "+tasks+" tasks.")
      processingQueue.put(poison)
    }
    sf.onComplete { 
      case Failure(t) => 
        logger.error("Feeding ended in an error:" + t.getMessage,t)
        processingQueue.put(poison)
      case Success(_) =>
    }
    var f = processingQueue.take()
    while (f ne poison) {
      Await.ready(f, Duration.Inf)
      f.onComplete { 
        case Failure(t) =>
          failures synchronized { failures += t }
          if (!all.isCompleted) all.tryFailure(t)
        case Success(_) =>
      }
      f = processingQueue.take()
    }
    if (!all.isCompleted) all.trySuccess(())
    all.future.onComplete {
      case Success(_) => logger.info("Successfully processed all sources.")
      case Failure(t) =>
        logger.error(f"Processing of ${failures.size}%,d sources resulted in errors." )
        //logger.error("Processing of at least one source resulted in an error:" + t.getMessage,t)
        logger.error(f"Processing of ${failures.size}%,d sources resulted in errors." )
        for (failure <- failures) logger.error("Error:",failure)
        logger.error(f"Processing of ${failures.size}%,d sources resulted in errors." )
    }
    indexingTaskExecutionContext.shutdown()
  }
  
  def runSequenceInOtherThread(tasks: (() => Unit)*): Future[Unit] = Future {
    for (task <- tasks) task()
  }(ExecutionContext.Implicits.global)
  
  def waitForTasks(tasks: Future[Unit]*) {
    for (task <- tasks) Try(Await.result(task, Duration.Inf)).toEither.left.foreach(logger.error("Task encountered exception",_))
  }


}