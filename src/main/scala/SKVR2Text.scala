import java.io.{File, FileInputStream, PrintWriter}

import ECCOIndexer.getFileTree
import ECCOXML2Text.attrsToString
import XMLEventReaderSupport._
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.LazyLogging
import org.rogach.scallop.ScallopConf

import scala.xml.Utility
import scala.xml.parsing.XhtmlEntities

object SKVR2Text extends LazyLogging {

  /*
      $string =~ s/&#227;&#772;/ää/g;        #
    $string =~ s/e&#814;&#772;/ee/g;           #
    $string =~ s/i&#815;&#772;/ii/g;           #
    $string =~ s/k&#769;&#772;/kk/g;           #
    $string =~ s/&#331;&#772;/ng/g;           #
    $string =~ s/o&#816&#772;/oo/g;           #
    $string =~ s/o&#813;&#772;/oo/g;           #
    $string =~ s/p&#769;&#772;/pp/g;           #
    $string =~ s/u&#815;&#772;/uu/g;           #
    $string =~ s/a&#772;&#776;/ää/g;           #
    $string =~ s/ö&#815;&#772;/öö/g;           #
    $string =~ s/&#603;&#772;/ää/g;           #
    $string =~ s/a&#772;/aa/g;           #
    $string =~ s/b&#772;/bb/g;           #
    $string =~ s/d&#772;/dd/g;           #
    $string =~ s/e&#772;/ee/g;           #
    $string =~ s/g&#772;/gg/g;           #
    $string =~ s/h&#772;/hh/g;           #
    $string =~ s/i&#772;/ii/g;           #
    $string =~ s/j&#772;/jj/g;           #
    $string =~ s/k&#772;/kk/g;           #
    $string =~ s/l&#772;/ll/g;           #
    $string =~ s/m&#772;/mm/g;           #
    $string =~ s/n&#772;/nn/g;           #
    $string =~ s/o&#772;/oo/g;           #
    $string =~ s/p&#772;/pp/g;           #
    $string =~ s/r&#772;/rr/g;           #
    $string =~ s/s&#772;/ss/g;           #
    $string =~ s/t&#772;/tt/g;           #
    $string =~ s/u&#772;/uu/g;           #
    $string =~ s/v&#772;/vv/g;           #
    $string =~ s/&#252;&#772;/yy/g;      #
    $string =~ s/y&#772;/yy/g;           #
    $string =~ s/z&#772;/zz/g;           #
    $string =~ s/å&#772;/aa/g;           #
    $string =~ s/ä&#772;/ää/g;           #
    $string =~ s/ö&#772;/öö/g;           #
    $string =~ s/A&#772;/aa/g;           #
    $string =~ s/E&#772;/ee/g;           #
    $string =~ s/I&#772;/ii/g;           #
    $string =~ s/O&#772;/oo/g;           #
    $string =~ s/U&#772;/uu/g;           #
    $string =~ s/&#220;&#772;/yy/g;      #
    $string =~ s/Y&#772;/yy/g;           #
    $string =~ s/Ä&#772;/ää/g;           #
    $string =~ s/Ö&#772;/öö/g;           #
    $string =~ s/&#192;/a/g;           #
    $string =~ s/&#193;/a/g;           #
    $string =~ s/&#194;/aa/g;           #
    $string =~ s/&#195;/a/g;           #
    $string =~ s/&#196;/ä/g;           #
    $string =~ s/&#197;/å/g;           #
    $string =~ s/&#198;/ae/g;           #
    $string =~ s/&#199;/c/g;           #
    $string =~ s/&#200;/e/g;           #
    $string =~ s/&#201;/e/g;           #
    $string =~ s/&#202;/ee/g;           #
    $string =~ s/&#203;/e/g;           #
    $string =~ s/&#204;/i/g;           #
    $string =~ s/&#205;/i/g;           #
    $string =~ s/&#206;/ii/g;           #
    $string =~ s/&#207;/i/g;           #
    $string =~ s/&#208;/d/g;           #
    $string =~ s/&#209;/n/g;           #
    $string =~ s/&#210;/o/g;           #
    $string =~ s/&#211;/o/g;           #
    $string =~ s/&#212;/oo/g;           #
    $string =~ s/&#213;/o/g;           #
    $string =~ s/&#214;/ö/g;           #
    $string =~ s/&#215;//g;           #
    $string =~ s/&#216;/ö/g;           #
    $string =~ s/&#217;/u/g;           #
    $string =~ s/&#218;/u/g;           #
    $string =~ s/&#219;/uu/g;           #
    $string =~ s/&#220;/y/g;           #
    $string =~ s/&#221;/y/g;           #
    $string =~ s/&#222;/t/g;           #
    $string =~ s/&#223;/ss/g;           #
    $string =~ s/&#224;/a/g;           #
    $string =~ s/&#225;/a/g;           #
    $string =~ s/&#226;/aa/g;           #
    $string =~ s/&#227;/a/g;           #
    $string =~ s/&#228;/ä/g;           #
    $string =~ s/&#229;/å/g;           #
    $string =~ s/&#230;/ae/g;           #
    $string =~ s/&#231;/c/g;           #
    $string =~ s/&#232;/e/g;           #
    $string =~ s/&#233;/e/g;           #
    $string =~ s/&#234;/ee/g;           #
    $string =~ s/&#235;/e/g;           #
    $string =~ s/&#236;/i/g;           #
    $string =~ s/&#237;/i/g;           #
    $string =~ s/&#238;/ii/g;           #
    $string =~ s/&#239;/i/g;           #
    $string =~ s/&#240;/d/g;           #
    $string =~ s/&#241;/n/g;           #
    $string =~ s/&#242;/o/g;           #
    $string =~ s/&#243;/o/g;           #
    $string =~ s/&#244;/oo/g;           #
    $string =~ s/&#245;/o/g;           #
    $string =~ s/&#246;/ö/g;           #
    $string =~ s/&#247;//g;           #
    $string =~ s/&#248;/ö/g;           #
    $string =~ s/&#249;/u/g;           #
    $string =~ s/&#250;/u/g;           #
    $string =~ s/&#251;/uu/g;           #
    $string =~ s/&#252;/y/g;           #
    $string =~ s/&#253;/y/g;           #
    $string =~ s/&#254;/t/g;           #
    $string =~ s/&#255;/y/g;           #
    $string =~ s/&#330;/n/g;           #
    $string =~ s/&#331;&#331;/ng/g;     #
    $string =~ s/&#331;/n/g;           #
    $string =~ s/&#339;/ö/g;           #
    $string =~ s/&#383;/s/g;           #
    $string =~ s/&#477;/e/g;           #
    $string =~ s/&#592;/a/g;           #
    $string =~ s/&#603;/e/g;           #
    $string =~ s/&#623;/uu/g;           #
    $string =~ s/&#658;/g/g;           #
    $string =~ s/&#7446;&#776;/ö/g;           #
    $string =~ s/&#7446;/o/g;           #
    $string =~ s/&#7491;&#816;&#776;/ä/g; #
    $string =~ s/&#7491&#776;/ä/g;        #
    $string =~ s/&#7491;/a/g;           #
    $string =~ s/&#7495;/b/g;           #
    $string =~ s/&#7496;/d/g;           #
    $string =~ s/&#7497;/e/g;           #
    $string =~ s/&#7501;/g/g;           #
    $string =~ s/&#688;/h/g;           #
    $string =~ s/&#8305;/i/g;           #
    $string =~ s/&#7503;/k/g;           #
    $string =~ s/&#737;/l/g;           #
    $string =~ s/&#7504;/m/g;           #
    $string =~ s/&#8319;/n/g;           #
    $string =~ s/&#7505;/n/g;           #
    $string =~ s/&#7506;&#776;/ö/g;     #
    $string =~ s/&#7506;/o/g;           #
    $string =~ s/&#7510;/p/g;           #
    $string =~ s/&#691;/r/g;           #
    $string =~ s/&#738;/s/g;           #
    $string =~ s/&#7511;/t/g;           #
    $string =~ s/&#7512;&#815;&#776;/y/g;  #
    $string =~ s/&#7512;&#776;/y/g;        #
    $string =~ s/&#7512;/u/g;           #
    $string =~ s/&#7515;/v/g;           #
    $string =~ s/&#696;/y/g;           #
    $string =~ s/&#7468;&#776;/ä/g;     #
    $string =~ s/&#7468;/a/g;           #
    $string =~ s/&#7482;/n/g;           #
    $string =~ s/&#7481;/m/g;           #
    $string =~ s/&#8336;&#776;/ä/g;     #
    $string =~ s/&#8336;/a/g;           #
    $string =~ s/&#8337;/e/g;           #
    $string =~ s/&#7522;/i/g;           #
    $string =~ s/&#8338;&#776;/ö/g      #
    $string =~ s/&#8338;/o/g;           #
    $string =~ s/&#7523;/r/g;           #
    $string =~ s/&#7524;/u/g;           #
    $string =~ s/&#7525;/v/g;           #

    $string =~ s/&#7429;/d/g;           #
    $string =~ s/&#665;/b/g;           #
    $string =~ s/&#610;/g/g;           #
    $string =~ s/&#7434;/j/g;           #
    $string =~ s/&#628;/n/g;          #

    $string =~ s/&#170;//g;           #
    $string =~ s/&#183;//g;           #
    $string =~ s/&#186;//g;           #
    $string =~ s/&#702;//g;           #
    $string =~ s/&#703;//g;           #
    $string =~ s/&#704;//g;           #
    $string =~ s/&#8224;//g;           #
    $string =~ s/&#8304;//g;           #
    $string =~ s/&#8333;//g;           #
    $string =~ s/&#8968;//g;           #
    $string =~ s/&#8969;//g;           #
    $string =~ s/&#8970;//g;           #
    $string =~ s/&#8971;//g;           #

    $string =~ s/&#1040;/a/g;           #
    $string =~ s/&#1041;/b/g;           #
    $string =~ s/&#1042;/v/g;           #
    $string =~ s/&#1043;/g/g;           #
    $string =~ s/&#1044;/d/g;           #
    $string =~ s/&#1045;/e/g;           #
    $string =~ s/&#1046;//g;           #
    $string =~ s/&#1047;//g;           #
    $string =~ s/&#1048;/i/g;           #
    $string =~ s/&#1049;//g;           #
    $string =~ s/&#1050;/k/g;           #
    $string =~ s/&#1051;/j/g;           #
    $string =~ s/&#1052;/m/g;           #
    $string =~ s/&#1053;/n/g;           #
    $string =~ s/&#1054;/o/g;           #
    $string =~ s/&#1055;/p/g;           #
    $string =~ s/&#1056;/r/g;           #
    $string =~ s/&#1057;/s/g;           #
    $string =~ s/&#1058;/t/g;           #
    $string =~ s/&#1059;/u/g;           #
    $string =~ s/&#1060;//g;           #
    $string =~ s/&#1061;/h/g;           #
    $string =~ s/&#1062;/ts/g;           #
    $string =~ s/&#1063;/ts/g;           #
    $string =~ s/&#1064;//g;           #
    $string =~ s/&#1065;//g;           #
    $string =~ s/&#1066;//g;           #
    $string =~ s/&#1067;//g;           #
    $string =~ s/&#1068;//g;           #
    $string =~ s/&#1069;/e/g;           #
    $string =~ s/&#1070;//g;           #
    $string =~ s/&#1071;//g;           #
    $string =~ s/&#1072;/a/g;           #
    $string =~ s/&#1073;/b/g;           #
    $string =~ s/&#1074;/v/g;           #
    $string =~ s/&#1075;/g/g;           #
    $string =~ s/&#1076;/d/g;           #
    $string =~ s/&#1077;/e/g;           #
    $string =~ s/&#1078;/z/g;           #
    $string =~ s/&#1079;/z/g;           #
    $string =~ s/&#1080;/i/g;           #
    $string =~ s/&#1081;/i/g;           #
    $string =~ s/&#1082;/k/g;           #
    $string =~ s/&#1083;/l/g;           #
    $string =~ s/&#1084;/m/g;           #
    $string =~ s/&#1085;/n/g;           #
    $string =~ s/&#1086;/o/g;           #
    $string =~ s/&#1087;/p/g;           #
    $string =~ s/&#1088;/r/g;           #
    $string =~ s/&#1089;/s/g;           #
    $string =~ s/&#1090;/t/g;           #
    $string =~ s/&#1091;/u/g;           #
    $string =~ s/&#1092;/f/g;           #
    $string =~ s/&#1093;/h/g;           #
    $string =~ s/&#1094;/ts/g;           #
    $string =~ s/&#1095;/ts/g;           #
    $string =~ s/&#1096;/s/g;           #
    $string =~ s/&#1097;/sts/g;           #
    $string =~ s/&#1098;//g;           #
    $string =~ s/&#1099;/y/g;           #
    $string =~ s/&#1100;//g;           #
    $string =~ s/&#1101;/e/g;           #
    $string =~ s/&#1102;/ju/g;           #
    $string =~ s/&#1103;/ja/g;           #
    $string =~ s/&#1104;//g;           #
    $string =~ s/&#1105;/jo/g;           #
    $string =~ s/&#1106;//g;           #
    $string =~ s/&#1107;//g;           #
    $string =~ s/&#1108;//g;           #
    $string =~ s/&#1109;//g;           #
    $string =~ s/&#1110;/i/g;           #
    $string =~ s/&#1111;//g;           #
    $string =~ s/&#1112;//g;           #
    $string =~ s/&#1113;/lj/g;           #
    $string =~ s/&#1114;//g;           #
    $string =~ s/&#1115;//g;           #
    $string =~ s/&#1116;//g;           #
    $string =~ s/&#1117;//g;           #
    $string =~ s/&#1118;//g;           #
    $string =~ s/&#1119;//g;           #
    $string =~ s/&#1120;//g;           #
    $string =~ s/&#1121;//g;           #
    $string =~ s/&#1122;/e/g;           #
    $string =~ s/&#1123;/e/g;           #

    $string =~ s/&#936;/ps/g;           #
    $string =~ s/&#945;/a/g;           #
    $string =~ s/&#946;/b/g;           #
    $string =~ s/&#947;/g/g;           #
    $string =~ s/&#948;/d/g;           #
    $string =~ s/&#949;/e/g;           #
    $string =~ s/&#950;/z/g;           #
    $string =~ s/&#951;/e/g;           #
    $string =~ s/&#953;/i/g;           #
    $string =~ s/&#954;/k/g;           #
    $string =~ s/&#955;/l/g;           #
    $string =~ s/&#957;/n/g;           #
    $string =~ s/&#958;/ks/g;           #
    $string =~ s/&#959;/o/g;           #
    $string =~ s/&#960;/p/g;           #
    $string =~ s/&#961;/r/g;           #
    $string =~ s/&#963;/s/g;           #
    $string =~ s/&#964;/t/g;           #
    $string =~ s/&#965;/y/g;           #
    $string =~ s/&#966;/f/g;           #
    $string =~ s/&#967;/kh/g;           #
    $string =~ s/&#968;/ps/g;           #
    $string =~ s/&#977;/th/g;           #
    $string =~ s/&#1008;/k/g;           #


# Díakriittiset merkit


    $string =~ s/&#768;//g;
    $string =~ s/&#769;//g;
    $string =~ s/&#770;//g;
    $string =~ s/&#771;//g;
    $string =~ s/&#773;//g;
    $string =~ s/&#774;//g;
    $string =~ s/&#775;//g;
    $string =~ s/&#776;//g;
    $string =~ s/&#777;//g;
    $string =~ s/&#778;//g;
    $string =~ s/&#779;//g;
    $string =~ s/&#780;//g;
    $string =~ s/&#781;//g;
    $string =~ s/&#782;//g;
    $string =~ s/&#783;//g;
    $string =~ s/&#784;//g;
    $string =~ s/&#785;//g;
    $string =~ s/&#786;//g;
    $string =~ s/&#787;//g;
    $string =~ s/&#788;//g;
    $string =~ s/&#789;//g;
    $string =~ s/&#790;//g;
    $string =~ s/&#791;//g;
    $string =~ s/&#792;//g;
    $string =~ s/&#793;//g;
    $string =~ s/&#794;//g;
    $string =~ s/&#795;//g;
    $string =~ s/&#796;//g;
    $string =~ s/&#797;//g;
    $string =~ s/&#798;//g;
    $string =~ s/&#799;//g;
    $string =~ s/&#800;//g;
    $string =~ s/&#801;//g;
    $string =~ s/&#802;//g;
    $string =~ s/&#803;//g;
    $string =~ s/&#804;//g;
    $string =~ s/&#805;//g;
    $string =~ s/&#806;//g;
    $string =~ s/&#807;//g;
    $string =~ s/&#808;//g;
    $string =~ s/&#809;//g;
    $string =~ s/&#810;//g;
    $string =~ s/&#811;//g;
    $string =~ s/&#812;//g;
    $string =~ s/&#813;//g;
    $string =~ s/&#814;//g;
    $string =~ s/&#815;//g;
    $string =~ s/&#816;//g;
    $string =~ s/&#817;//g;
    $string =~ s/&#818;//g;
    $string =~ s/&#819;//g;
    $string =~ s/&#820;//g;
    $string =~ s/&#821;//g;
    $string =~ s/&#822;//g;
    $string =~ s/&#823;//g;
    $string =~ s/&#824;//g;
    $string =~ s/&#825;//g;
    $string =~ s/&#826;//g;
    $string =~ s/&#827;//g;
    $string =~ s/&#828;//g;
    $string =~ s/&#829;//g;
    $string =~ s/&#830;//g;
    $string =~ s/&#831;//g;
    $string =~ s/&#832;//g;
    $string =~ s/&#833;//g;
    $string =~ s/&#834;//g;
    $string =~ s/&#835;//g;
    $string =~ s/&#836;//g;
    $string =~ s/&#837;//g;
    $string =~ s/&#838;//g;
    $string =~ s/&#839;//g;
    $string =~ s/&#840;//g;
    $string =~ s/&#841;//g;
    $string =~ s/&#842;//g;
    $string =~ s/&#843;//g;
    $string =~ s/&#844;//g;
    $string =~ s/&#845;//g;
    $string =~ s/&#846;//g;
    $string =~ s/&#847;//g;
    $string =~ s/&#848;//g;
    $string =~ s/&#849;//g;
    $string =~ s/&#850;//g;
    $string =~ s/&#851;//g;
    $string =~ s/&#852;//g;
    $string =~ s/&#853;//g;
    $string =~ s/&#854;//g;
    $string =~ s/&#855;//g;
    $string =~ s/&#856;//g;
    $string =~ s/&#857;//g;
    $string =~ s/&#858;//g;
    $string =~ s/&#859;//g;
    $string =~ s/&#860;//g;
    $string =~ s/&#861;//g;
    $string =~ s/&#862;//g;
    $string =~ s/&#863;//g;
    $string =~ s/&#864;//g;
    $string =~ s/&#865;//g;
    $string =~ s/&#866;//g;
    $string =~ s/&#867;//g;
    $string =~ s/&#868;//g;
    $string =~ s/&#869;//g;
    $string =~ s/&#870;//g;
    $string =~ s/&#871;//g;
    $string =~ s/&#872;//g;
    $string =~ s/&#873;//g;
    $string =~ s/&#874;//g;
    $string =~ s/&#875;//g;
    $string =~ s/&#876;//g;
    $string =~ s/&#877;//g;
    $string =~ s/&#878;//g;
    $string =~ s/&#879;//g;
   */

  private def readContents(implicit xml: Iterator[EvEvent]): String = {
    var break = false
    val content = new StringBuilder()
    while (xml.hasNext && !break) xml.next match {
      case EvElemStart(_,_,_) =>
      case EvText(text) => content.append(text)
      case er: EvEntityRef => XhtmlEntities.entMap.get(er.entity) match {
        case Some(chr) => content.append(chr)
        case _ => content.append(er.entity)
      }
      case EvComment(_) =>
      case EvElemEnd(_,"V") => break = true
      case EvElemEnd(_,_) =>
    }
    content.append('\n')
    content.toString.replaceFirst("^[0-9]* ","").replaceAll("#[0-9]*","").replaceAllLiterally("[","").replaceAllLiterally("]","")
  }

  def main(args: Array[String]): Unit = {
    val opts = new ScallopConf(args) {
      val directories = trailArg[List[String]](required = true)
      val dest = opt[String](required = true)
      verify()
    }
    new File(opts.dest()).mkdirs()
    val metadataCSV = CSVWriter.open(opts.dest()+"/"+"metadata.csv")
    opts.directories().toStream.flatMap(f => getFileTree(new File(f))
    ).filter(_.getName.endsWith(".xml")).foreach(f => {
      val s = new FileInputStream(f)
      implicit val xml = getXMLEventReader(s,"UTF-8")
      while (xml.hasNext) xml.next match {
        case EvElemStart(_, "ITEM", iattrs) =>
          var break = false
          val id = iattrs("nro")
          val year = iattrs("y")
          val place = iattrs("p")
          val collector = iattrs("k")
          metadataCSV.writeRow(Seq(id,year,place,collector))
          val tsw: PrintWriter = new PrintWriter(opts.dest()+"/"+id+".txt")
          val msw: PrintWriter = new PrintWriter(opts.dest()+"/"+id+"_metadata.xml")
          while (xml.hasNext && !break) xml.next match {
            case EvElemStart(_, "V", _) => tsw.append(readContents)
            case EvElemStart(_, label, attrs) =>
              msw.append("<" + label + attrsToString(attrs) + ">\n")
            case EvText(text) =>
              if (text.length==1 && Utility.Escapes.escMap.contains(text(0))) msw.append(Utility.Escapes.escMap(text(0)))
              else msw.append(text)
            case er: EvEntityRef =>
              msw.append('&'); msw.append(er.entity); msw.append(';')
            case EvElemEnd(_, "ITEM") => break = true
            case EvElemEnd(_, label) =>
              msw.append("</"+label+">")
            case EvComment(_) =>
          }
          tsw.close()
          msw.close()
        case _ =>
      }
    })
    metadataCSV.close()
  }
}
