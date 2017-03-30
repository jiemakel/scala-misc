

object FinnishNewspapersIndexer extends OctavoIndexer {
  
  // hierarchy: newspaper, article, paragraph
  
  /*
   * Needs to handle results from morphological analysis.
	 * Plan:
	 * Index for original data
	 * Parallel index for best guess lemma
	 * Parallel index for all guessed lemmas (with positionIncrementAttr set to zero, and weights [and/or morphological analysis] stored as payloads)
	 * Parallel index for additional metadata
   */
  
  def main(args: Array[String]): Unit = {
    
  }
}