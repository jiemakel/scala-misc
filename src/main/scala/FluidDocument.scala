import org.apache.lucene.index.IndexableField

import scala.collection.mutable.ArrayBuffer

class FluidDocument extends java.lang.Iterable[IndexableField] {
  val fields = new ArrayBuffer[IndexableField] {
    def setSize(n: Int): Unit = {
      size0 = n
    }
  }
  var requiredFieldCount = 0
  def clearOptional(): Unit = fields.setSize(requiredFieldCount)
  def addRequired(rfields: IndexableField*): Unit = {
    requiredFieldCount += rfields.length
    fields.++=:(rfields)
  }
  def removeRequiredFields(name: String): Unit = {
    var i = fields.lastIndexWhere(f => f.name==name, requiredFieldCount - 1)
    while (i != -1) {
      fields.remove(i)
      requiredFieldCount -= 1
      i = fields.lastIndexWhere(f => f.name==name,i-1)
    }
  }
  def addOptional(ofields: IndexableField*): Unit = fields ++= ofields
  override def iterator(): java.util.Iterator[IndexableField] = new java.util.Iterator[IndexableField] {
    var i = 0
    override def hasNext = i<fields.length

    override def next() = {
      val ret = fields(i)
      i += 1
      ret
    }
  }
}
