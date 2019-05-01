import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.util.AttributeSource

import scala.collection.mutable.ArrayBuffer

class ReusableCachedTokenStream(as: AttributeSource) extends TokenStream(as) {
  private val cache = new ArrayBuffer[AttributeSource.State]()
  private var iterator: Iterator[AttributeSource.State] = _
  private var finalState: AttributeSource.State = _
  def fill(input: TokenStream): Unit = {
    input.reset()
    cache.clear()
    while (input.incrementToken())
      cache += captureState()
    input.end()
    finalState = captureState()
    input.close()
  }
  override def reset(): Unit = {
    iterator = cache.iterator
  }
  override def end(): Unit = if (finalState!=null) restoreState(finalState)
  override final def incrementToken() =
    if (iterator.hasNext) {
      restoreState(iterator.next)
      true
    } else false
}
