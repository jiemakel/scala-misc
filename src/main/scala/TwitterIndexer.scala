import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Locale
import java.util.zip.GZIPInputStream

import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.json4s._
import org.json4s.native.JsonParser._
import org.rogach.scallop._

import scala.language.{postfixOps, reflectiveCalls}


object TwitterIndexer extends OctavoIndexer {

  class Reuse {
    val td = new Document()
    val idFields = new StringSDVFieldPair("id", td)
    val timeFields = new LongPointSDVDateTimeFieldPair("created_at", ISODateTimeFormat.dateTimeNoMillis(), td)
    val textField = new Field("text", "", contentFieldType)
    td.add(textField)
    val truncatedFields = new StringSDVFieldPair("truncated", td)
    val languageFields = new StringSDVFieldPair("language",td)
    val sourceFields = new StringSDVFieldPair("source",td)
    val inReplyToUserIdFields = new StringSDVFieldPair("inReplyToUserId",td)
    val inReplyToScreenNameFields = new StringSDVFieldPair("inReplyToScreenName",td)
    val userIdFields = new StringSDVFieldPair("userId", td)
    val userNameFields = new StringSDVFieldPair("userName",td)
    val userScreenNameFields = new StringSDVFieldPair("userScreenName",td)
    val userLocationFields = new StringSDVFieldPair("userLocation",td)
    val userDescriptionFields = new StringSDVFieldPair("userDescription",td)
    val userFollowersCountFields = new IntPointNDVFieldPair("userFollowersCount",td)
    val userFriendsCountFields = new IntPointNDVFieldPair("userFriendsCount",td)
    val userListedCountFields = new IntPointNDVFieldPair("userListedCount", td)
    val userFavouritesCountFields = new IntPointNDVFieldPair("userFavouritesCount", td)
    val userVerifiedFields = new StringSDVFieldPair("userVerified",td)
    val userLanguageFields = new StringSDVFieldPair("userLanguage",td)
    val retweetIdFields = new StringSDVFieldPair("retweetId",td)
    val retweetUserIdFields = new StringSDVFieldPair("retweetUserId",td)
    val retweetScreenNameFields = new StringSDVFieldPair("retweetScreenName",td)
    val quotedIdFields = new StringSDVFieldPair("quotedId",td)
    val quotedUserIdFields = new StringSDVFieldPair("quotedUserId",td)
    val quotedScreenNameFields = new StringSDVFieldPair("quotedScreenName",td)
    val retweetCountFields = new IntPointNDVFieldPair("retweetCount",td)
    val favoriteCountFields = new IntPointNDVFieldPair("favoriteCount", td)
    val possiblySensitiveFields = new StringSDVFieldPair("possiblySensitive", td)
    val placeId = new StringSDVFieldPair("placeId",td)
    val placeFullName = new StringSDVFieldPair("placeFullName",td)
    val placeType = new StringSDVFieldPair("placeType",td)
    val country = new StringSDVFieldPair("country",td)
    val placeCoordinates = new LatLonFieldPair("placeCoordinates", td)
    def clean() {
      td.removeFields("hashtags")
      td.removeFields("userMentionsScreenName")
      td.removeFields("urls")
    }
  }
  
  val tld = new java.lang.ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  implicit val formats = DefaultFormats
  
  private def index(file: File): Unit = {
    parse(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))), (p: Parser) => {
      var obj = ObjParser.parseObject(p)
      while (obj != JNothing) {
        index(obj.asInstanceOf[JObject])
        obj = ObjParser.parseObject(p)
      }
      logger.info("File "+file+" processed.")
    })
  }

  private val twitterDateTimeFormat = DateTimeFormat.forPattern( "EE MMM dd HH:mm:ss Z yyyy" ).withLocale(Locale.US)
  private val isoDateTimeFormat = ISODateTimeFormat.dateTimeNoMillis()
 
  private def index(t: JObject): Unit = {
    val d = tld.get
    d.clean()
    d.idFields.setValue((t \ "id_str").asInstanceOf[JString].values)

    d.timeFields.setValue(isoDateTimeFormat.print(twitterDateTimeFormat.parseMillis((t \ "created_at").asInstanceOf[JString].values)))
    d.textField.setStringValue((t \ "text").asInstanceOf[JString].values)
    d.truncatedFields.setValue((t \ "truncated").asInstanceOf[JBool].values.toString)
    for (v <- (t \ "entities" \ "hashtags" \ "text").asInstanceOf[JArray].arr) {
      new StringSSDVFieldPair("hashtags", d.td).setValue(v.asInstanceOf[JString].values)
    }
    for (v <- (t \ "entities" \ "user_mentions").asInstanceOf[JArray].arr) {
      new StringSSDVFieldPair("userMentionsUserId", d.td).setValue((v \ "id_str").asInstanceOf[JString].values)
      new StringSSDVFieldPair("userMentionsScreenName", d.td).setValue((v \ "screen_name").asInstanceOf[JString].values)
    }
    for (v <- (t \ "entities" \ "urls" \ "expanded_url").asInstanceOf[JArray].arr) {
      new StringSSDVFieldPair("urls", d.td).setValue(v.asInstanceOf[JString].values)
    }

    d.languageFields.setValue((t \ "lang").asInstanceOf[JString].values)
    d.sourceFields.setValue((t \ "source").asInstanceOf[JString].values)
    t \ "in_reply_to_user_id_str" match {
      case JNull => d.inReplyToUserIdFields.setValue("")
      case JString(str) => d.inReplyToUserIdFields.setValue(str)
    }
    t \ "in_reply_to_screen_name" match {
      case JNull => d.inReplyToScreenNameFields.setValue("")
      case JString(str) => d.inReplyToScreenNameFields.setValue(str)
    }
    val user = (t \ "user").asInstanceOf[JObject]
    d.userIdFields.setValue((user \ "id_str").asInstanceOf[JString].values)
    d.userNameFields.setValue((user \ "name").asInstanceOf[JString].values)
    d.userScreenNameFields.setValue((user \ "screen_name").asInstanceOf[JString].values)
    d.userLocationFields.setValue((user \ "location").asInstanceOf[JString].values)
    d.userDescriptionFields.setValue((user \ "description").asInstanceOf[JString].values)
    d.userFollowersCountFields.setValue((user \ "followers_count").asInstanceOf[JInt].num.intValue)
    d.userFriendsCountFields.setValue((user \ "friends_count").asInstanceOf[JInt].num.intValue)
    d.userListedCountFields.setValue((user \ "listed_count").asInstanceOf[JInt].num.intValue)
    d.userFavouritesCountFields.setValue((user \ "favourites_count").asInstanceOf[JInt].num.intValue)
    d.userVerifiedFields.setValue((user \ "verified").asInstanceOf[JBool].values.toString)
    d.userLanguageFields.setValue((user \ "lang").asInstanceOf[JString].values)
    t \ "retweeted_status" match {
      case JNull | JNothing =>
        d.retweetIdFields.setValue("")
        d.retweetUserIdFields.setValue("")
        d.retweetScreenNameFields.setValue("")
      case rt =>
        d.retweetIdFields.setValue((rt \ "id_str").asInstanceOf[JString].values)
        val rtuser = (rt \ "user").asInstanceOf[JObject]
        d.retweetUserIdFields.setValue((rtuser \ "id_str").asInstanceOf[JString].values)
        d.retweetScreenNameFields.setValue((rtuser \ "screen_name").asInstanceOf[JString].values)
    }
    t \ "quoted_status" match {
      case JNull | JNothing =>
        d.quotedIdFields.setValue("")
        d.quotedUserIdFields.setValue("")
        d.quotedScreenNameFields.setValue("")
      case rt =>
        d.quotedIdFields.setValue((rt \ "id_str").asInstanceOf[JString].values)
        val rtuser = (rt \ "user").asInstanceOf[JObject]
        d.quotedUserIdFields.setValue((rtuser \ "id_str").asInstanceOf[JString].values)
        d.quotedScreenNameFields.setValue((rtuser \ "screen_name").asInstanceOf[JString].values)
    }
    t \ "in_reply_to_screen_name" match {
      case JNull => d.inReplyToScreenNameFields.setValue("")
      case JString(str) => d.inReplyToScreenNameFields.setValue(str)
    }
    d.retweetCountFields.setValue((t \ "retweet_count").asInstanceOf[JInt].num.intValue)
    d.favoriteCountFields.setValue((t \ "favorite_count").asInstanceOf[JInt].num.intValue)
    t \"possibly_sensitive" match {
      case JNothing => d.possiblySensitiveFields.setValue("unknown")
      case v => d.possiblySensitiveFields.setValue(v.asInstanceOf[JBool].value.toString)
    }
    t \ "place" match {
      case JNull =>
        d.placeId.setValue("")
        d.placeFullName.setValue("")
        d.placeType.setValue("")
        d.country.setValue("")
        d.placeCoordinates.setValue(0,0)
      case p =>
        d.placeId.setValue((p \ "id").asInstanceOf[JString].values)
        d.placeFullName.setValue((p \ "full_name").asInstanceOf[JString].values)
        d.placeType.setValue((p \ "place_type").asInstanceOf[JString].values)
        d.country.setValue((p \ "country").asInstanceOf[JString].values)
        val coords = (p \ "bounding_box" \ "coordinates")(0).asInstanceOf[JArray].arr.map{ case JArray(a) => (a.head.asInstanceOf[JDouble].num.doubleValue,a(1).asInstanceOf[JDouble].num.doubleValue) }
        d.placeCoordinates.setValue((coords.head._2+coords(1)._2)/2,(coords.head._1+coords(1)._1)/2)
    }
    tiw.addDocument(d.td)
  }
  
  var tiw: IndexWriter = null.asInstanceOf[IndexWriter]

  val ts = new Sort(new SortField("id",SortField.Type.STRING))

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val tpostings = opt[String](default = Some("fst"))
      verify()
    }
    if (!opts.onlyMerge()) {
      tiw = iw(opts.index()+"/tindex",ts,opts.indexMemoryMb() / 1)
      feedAndProcessFedTasksInParallel(() =>
        opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".jsonl.gz")).foreach(file => addTask(file.getName, () => index(file)))
      )
    }
    val termVectorFields = Seq("text")
    waitForTasks(
      runSequenceInOtherThread(
        () => close(tiw),
        () => merge(opts.index()+"/aindex", ts,opts.indexMemoryMb() / 1, toCodec(opts.tpostings(), termVectorFields))
      ),
    )
  }
}
