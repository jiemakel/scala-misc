import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext.Implicits.global

object ECCOIndexMerger extends LazyLogging {
  def main(args: Array[String]): Unit = {
    ECCOIndexer.mergeIndices(args.last, args.length!=1)
  }
}
