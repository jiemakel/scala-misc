import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField

object ECCOIndexMerger extends LazyLogging {
  def main(args: Array[String]): Unit = {
    ECCOIndexer.mergeIndices(Seq(
     (args.last+"/dindex", new Sort(new SortField("documentID",SortField.Type.STRING))),
     (args.last+"/dpindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("partID", SortField.Type.LONG))),
     (args.last+"/sindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("sectionID", SortField.Type.LONG))),
     (args.last+"/pindex", new Sort(new SortField("documentID",SortField.Type.STRING), new SortField("paragraphID", SortField.Type.LONG)))
    ),args.length!=1)
  }
}
