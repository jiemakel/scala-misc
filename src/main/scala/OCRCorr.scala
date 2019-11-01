import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import org.apache.commons.math3.stat.correlation.{KendallsCorrelation, PearsonsCorrelation, SpearmansCorrelation}

import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap}

object OCRCorr {
  def main(args: Array[String]): Unit = {
    val wordConfidenceByISSN = new HashMap[String,Buffer[Double]]
    val charConfidenceByISSN = new HashMap[String,Buffer[Double]]
    val isWordCorrectByISSN = new HashMap[String,Buffer[Double]]
    val isCharCorrectByISSN = new HashMap[String,Buffer[Double]]
    val wordConfidenceByYear = new HashMap[String,Buffer[Double]]
    val charConfidenceByYear = new HashMap[String,Buffer[Double]]
    val isWordCorrectByYear = new HashMap[String,Buffer[Double]]
    val isCharCorrectByYear = new HashMap[String,Buffer[Double]]
    val wr = CSVReader.open("ocr_comparison_gt2.csv")(new DefaultCSVFormat {
      override val escapeChar='¤'
    })
    for (r <- wr.iteratorWithHeaders) {
      val issn = r("PAGENAME").replaceFirst("_.*","")
      val year = r("PAGENAME").replaceFirst(".*?_","").replaceFirst("-.*","")
      val isWordCorrect1 = isWordCorrectByISSN.getOrElseUpdate(issn,new ArrayBuffer[Double])
      val isWordCorrect2 = isWordCorrectByYear.getOrElseUpdate(year,new ArrayBuffer[Double])
      val wordConfidence1 = wordConfidenceByISSN.getOrElseUpdate(issn,new ArrayBuffer[Double])
      val wordConfidence2 = wordConfidenceByYear.getOrElseUpdate(year,new ArrayBuffer[Double])
      val isCharCorrect1 = isCharCorrectByISSN.getOrElseUpdate(issn,new ArrayBuffer[Double])
      val isCharCorrect2 = isCharCorrectByYear.getOrElseUpdate(year,new ArrayBuffer[Double])
      val charConfidence1 = charConfidenceByISSN.getOrElseUpdate(issn,new ArrayBuffer[Double])
      val charConfidence2 = charConfidenceByYear.getOrElseUpdate(year,new ArrayBuffer[Double])
      val isCorrect = if (r("NLF")==r("FR")) 1 else 0
      if (r("FR_WC")!="") {
        val confidence = r("FR_WC").toDouble
        isWordCorrect1 += isCorrect
        isWordCorrect2 += isCorrect
        wordConfidence1 += confidence
        wordConfidence2 += confidence
      }
      if (r("NLF").length==r("FR").length && r("NLF").length==r("FR_CC").length) {
        var j = 0
        val cc = r("FR_CC")
        for (curChar <- r("FR")) {
          val isCorrect = if (curChar==r("NLF")(j)) 1 else 0
          val confidence = ("0."+cc(j)).toDouble
          isCharCorrect1 += isCorrect
          isCharCorrect2 += isCorrect
          charConfidence1 += confidence
          charConfidence2 += confidence
          j+=1
        }
      }
    }
/*    for (file <- new File("fr-gt-gt2-FR").listFiles();if (file.getName.endsWith("-gt2.xml"))) {
      var s = Source.fromFile(file,"UTF-8")
      val issn = file.getName.replaceFirst("_.*","")
      val year = file.getName.replaceFirst(".*?_","").replaceFirst("-.*","")
      val correctWords = new ArrayBuffer[String]
      var xml = new XMLEventReader(s)
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"String",attrs,_) => correctWords += attrs("CONTENT")(0).text 
        case _ =>
      }
      s.close()
      val isWordCorrect1 = isWordCorrectByISSN.getOrElseUpdate(issn,new ArrayBuffer[Double])
      val isWordCorrect2 = isWordCorrectByYear.getOrElseUpdate(year,new ArrayBuffer[Double])
      val wordConfidence1 = wordConfidenceByISSN.getOrElseUpdate(issn,new ArrayBuffer[Double])
      val wordConfidence2 = wordConfidenceByYear.getOrElseUpdate(year,new ArrayBuffer[Double])
      val isCharCorrect1 = isCharCorrectByISSN.getOrElseUpdate(issn,new ArrayBuffer[Double])
      val isCharCorrect2 = isCharCorrectByYear.getOrElseUpdate(year,new ArrayBuffer[Double])
      val charConfidence1 = charConfidenceByISSN.getOrElseUpdate(issn,new ArrayBuffer[Double])
      val charConfidence2 = charConfidenceByYear.getOrElseUpdate(year,new ArrayBuffer[Double])
      s = Source.fromFile(new File(file.getPath.replaceFirst("-gt2.xml","-FR.xml")),"UTF-8")
      xml = new XMLEventReader(s)
      val detectedWords = new ArrayBuffer[String]
      val detectedWC = new ArrayBuffer[Double]
      val detectedCC = new ArrayBuffer[String]
      while (xml.hasNext) xml.next match {
        case EvElemStart(_,"String",attrs,_) =>
          detectedWords += attrs("CONTENT")(0).text
          detectedWC += attrs("WC")(0).text.toDouble
          detectedCC += attrs("CC")(0).text
        case _ => 
      }
      var i = 0
      println(file+": "+detectedWords.length+"/"+correctWords.length)
      if (detectedWords.length==correctWords.length) for (curWord <- detectedWords) {
        val isCorrect = if (curWord==correctWords(i)) 1 else 0
        val confidence = detectedWC(i)
        isWordCorrect1 += isCorrect
        isWordCorrect2 += isCorrect
        wordConfidence1 += confidence
        wordConfidence2 += confidence
        if (curWord.length==correctWords(i).length) {
          var j = 0
          val cc = detectedCC(i)
          for (curChar <- curWord) {
            val isCorrect = if (curChar==correctWords(i)(j)) 1 else 0
            val confidence = cc(j).toDouble
            isCharCorrect1 += isCorrect
            isCharCorrect2 += isCorrect
            charConfidence1 += confidence
            charConfidence2 += confidence
            j+=1
          }
        }
        i+=1
      }
      s.close()
    }*/
    val totalIsWordCorrect = new ArrayBuffer[Double]
    val totalWordConfidence = new ArrayBuffer[Double]
    val totalIsCharCorrect = new ArrayBuffer[Double] 
    val totalCharConfidence = new ArrayBuffer[Double] 
    for (key <- isWordCorrectByISSN.keys) {
      totalIsWordCorrect ++= isWordCorrectByISSN(key)
      totalWordConfidence ++= wordConfidenceByISSN(key)
      val isWordCorrect = isWordCorrectByISSN(key).toArray
      val wordConfidence = wordConfidenceByISSN(key).toArray
      val pcorr = if (isWordCorrect.length>1) new PearsonsCorrelation().correlation(isWordCorrect,wordConfidence) else "?"
      val scorr = if (isWordCorrect.length>1) new SpearmansCorrelation().correlation(isWordCorrect,wordConfidence) else "?"
      val kcorr = if (isWordCorrect.length>1) new KendallsCorrelation().correlation(isWordCorrect,wordConfidence) else "?"
      val isCharCorrect = isCharCorrectByISSN(key).toArray
      val charConfidence = charConfidenceByISSN(key).toArray
      totalIsCharCorrect ++= isCharCorrectByISSN(key)
      totalCharConfidence ++= charConfidenceByISSN(key)
      val pccorr = if (isCharCorrect.length>1) new PearsonsCorrelation().correlation(isCharCorrect,charConfidence) else "?"
      val sccorr = if (isCharCorrect.length>1) new SpearmansCorrelation().correlation(isCharCorrect,charConfidence) else "?"
      val kccorr = if (isCharCorrect.length>1) new KendallsCorrelation().correlation(isCharCorrect,charConfidence) else "?"
      println(s"ISSN: $key - by word: p: $pcorr - s: $scorr - k: $kcorr / by char: p: $pccorr - s: $sccorr - k: $kccorr")
    }
    for (key <- isWordCorrectByYear.keys) {
      val isWordCorrect = isWordCorrectByYear(key).toArray
      val wordConfidence = wordConfidenceByYear(key).toArray
      val pcorr = if (isWordCorrect.length>1) new PearsonsCorrelation().correlation(isWordCorrect,wordConfidence) else "?"
      val scorr = if (isWordCorrect.length>1) new SpearmansCorrelation().correlation(isWordCorrect,wordConfidence) else "?"
      val kcorr = if (isWordCorrect.length>1) new KendallsCorrelation().correlation(isWordCorrect,wordConfidence) else "?"
      val isCharCorrect = isCharCorrectByYear(key).toArray
      val charConfidence = charConfidenceByYear(key).toArray
      val pccorr = if (isCharCorrect.length>1) new PearsonsCorrelation().correlation(isCharCorrect,charConfidence) else "?"
      val sccorr = if (isCharCorrect.length>1) new SpearmansCorrelation().correlation(isCharCorrect,charConfidence) else "?"
      val kccorr = if (isCharCorrect.length>1) new KendallsCorrelation().correlation(isCharCorrect,charConfidence) else "?"
      println(s"Year: $key - by word: p: $pcorr - s: $scorr - k: $kcorr / by char: p: $pccorr - s: $sccorr - k: $kccorr")
    }
    val isWordCorrect = totalIsWordCorrect.toArray
    val wordConfidence = totalWordConfidence.toArray
    val pcorr = new PearsonsCorrelation().correlation(isWordCorrect,wordConfidence)
    val scorr = new SpearmansCorrelation().correlation(isWordCorrect,wordConfidence)
    val kcorr = new KendallsCorrelation().correlation(isWordCorrect,wordConfidence)
    val isCharCorrect = totalIsCharCorrect.toArray
    val charConfidence = totalCharConfidence.toArray
    val pccorr = new PearsonsCorrelation().correlation(isCharCorrect,charConfidence)
    val sccorr = new SpearmansCorrelation().correlation(isCharCorrect,charConfidence)
    val kccorr = new KendallsCorrelation().correlation(isCharCorrect,charConfidence)
    println(s"Total: - by word: p: $pcorr - s: $scorr - k: $kcorr / by char: p: $pccorr - s: $sccorr - k: $kccorr")
  }
}