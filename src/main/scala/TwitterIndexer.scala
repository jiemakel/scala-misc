import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Locale
import java.util.zip.GZIPInputStream

import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.{Sort, SortField}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.json4s._
import org.json4s.native.JsonParser._
import org.rogach.scallop._

import scala.language.{postfixOps, reflectiveCalls}


object TwitterIndexer extends OctavoIndexer {

  class Reuse {
    val td = new FluidDocument
    val idFields = new StringSDVFieldPair("id").r(td)
    val timeFields = new LongPointSDVDateTimeFieldPair("created_at", ISODateTimeFormat.dateTimeNoMillis()).r(td)
    val textField = new ContentField("text").r(td)
    val truncatedFields = new StringSDVFieldPair("truncated").r(td)
    val languageFields = new StringSDVFieldPair("language").r(td)
    val sourceFields = new StringSDVFieldPair("source").r(td)
    val inReplyToUserIdFields = new StringSDVFieldPair("inReplyToUserId").o(td)
    val inReplyToScreenNameFields = new StringSDVFieldPair("inReplyToScreenName").o(td)
    val tweetType = new StringSDVFieldPair("type").r(td)
    val userIdFields = new StringSDVFieldPair("userId").r(td)
    val userNameFields = new TextSDVFieldPair("userName").r(td)
    val userScreenNameFields = new StringSDVFieldPair("userScreenName").o(td)
    val userLocationFields = new TextSDVFieldPair("userLocation").r(td)
    val userDescriptionFields = new TextSDVFieldPair("userDescription").r(td)
    val userFollowersCountFields = new IntPointNDVFieldPair("userFollowersCount").r(td)
    val userFriendsCountFields = new IntPointNDVFieldPair("userFriendsCount").r(td)
    val userListedCountFields = new IntPointNDVFieldPair("userListedCount").r(td)
    val userFavouritesCountFields = new IntPointNDVFieldPair("userFavouritesCount").r(td)
    val userVerifiedFields = new StringSDVFieldPair("userVerified").r(td)
    val userLanguageFields = new StringSDVFieldPair("userLanguage").r(td)
    val retweetIdFields = new StringSDVFieldPair("retweetId").o(td)
    val retweetUserIdFields = new StringSDVFieldPair("retweetUserId").o(td)
    val retweetScreenNameFields = new StringSDVFieldPair("retweetScreenName").o(td)
    val quotedIdFields = new StringSDVFieldPair("quotedId").o(td)
    val quotedUserIdFields = new StringSDVFieldPair("quotedUserId").o(td)
    val quotedScreenNameFields = new StringSDVFieldPair("quotedScreenName").o(td)
    val retweetCountFields = new IntPointNDVFieldPair("retweetCount").r(td)
    val favoriteCountFields = new IntPointNDVFieldPair("favoriteCount").r(td)
    val possiblySensitiveFields = new StringSDVFieldPair("possiblySensitive").r(td)
    val contentLengthFields = new IntPointNDVFieldPair("contentLength").r(td)
    val contentTokensFields = new IntPointNDVFieldPair("contentTokens").r(td)
    val placeId = new StringSDVFieldPair("placeId").o(td)
    val placeFullName = new TextSDVFieldPair("placeFullName").o(td)
    val placeType = new StringSDVFieldPair("placeType").o(td)
    val country = new StringSDVFieldPair("country").o(td)
    val placeCoordinates = new LatLonFieldPair("placeCoordinates").o(td)

    def clean() {
      td.clearOptional()
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
    val text = (t \ "text").asInstanceOf[JString].values
    d.textField.setValue(text)
    getNumberOfTokens(d.textField.tokenStream)
    getNumberOfTokens(d.textField.tokenStream)
    getNumberOfTokens(d.textField.tokenStream)
    d.contentLengthFields.setValue(text.length)
    d.contentTokensFields.setValue(d.textField.numberOfTokens)
    d.truncatedFields.setValue((t \ "truncated").asInstanceOf[JBool].values.toString)
    for (v <- (t \ "entities" \ "hashtags" \ "text").asInstanceOf[JArray].arr) {
      new StringSSDVFieldPair("hashtags").o(d.td).setValue(v.asInstanceOf[JString].values)
    }
    for (v <- (t \ "entities" \ "user_mentions").asInstanceOf[JArray].arr) {
      new StringSSDVFieldPair("userMentionsUserId").o(d.td).setValue((v \ "id_str").asInstanceOf[JString].values)
      new StringSSDVFieldPair("userMentionsScreenName").o(d.td).setValue((v \ "screen_name").asInstanceOf[JString].values)
    }
    for (v <- (t \ "entities" \ "urls" \ "expanded_url").asInstanceOf[JArray].arr) {
      new StringSSDVFieldPair("urls").o(d.td).setValue(v.asInstanceOf[JString].values)
    }

    d.languageFields.setValue((t \ "lang").asInstanceOf[JString].values)
    d.sourceFields.setValue((t \ "source").asInstanceOf[JString].values)
    t \ "in_reply_to_user_id_str" match {
      case JNull =>
      case JString(str) => d.inReplyToUserIdFields.o(d.td).setValue(str)
    }
    t \ "in_reply_to_screen_name" match {
      case JNull =>
      case JString(str) =>
        d.tweetType.setValue("reply")
        d.inReplyToScreenNameFields.o(d.td).setValue(str)
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
    d.tweetType.setValue("tweet")
    t \ "retweeted_status" match {
      case JNull | JNothing =>
      case rt =>
        d.tweetType.setValue("retweet")
        d.retweetIdFields.o(d.td).setValue((rt \ "id_str").asInstanceOf[JString].values)
        val rtuser = (rt \ "user").asInstanceOf[JObject]
        d.retweetUserIdFields.o(d.td).setValue((rtuser \ "id_str").asInstanceOf[JString].values)
        d.retweetScreenNameFields.o(d.td).setValue((rtuser \ "screen_name").asInstanceOf[JString].values)
    }
    t \ "quoted_status" match {
      case JNull | JNothing =>
      case rt =>
        d.tweetType.setValue("quoted")
        d.quotedIdFields.o(d.td).setValue((rt \ "id_str").asInstanceOf[JString].values)
        val rtuser = (rt \ "user").asInstanceOf[JObject]
        d.quotedUserIdFields.o(d.td).setValue((rtuser \ "id_str").asInstanceOf[JString].values)
        d.quotedScreenNameFields.o(d.td).setValue((rtuser \ "screen_name").asInstanceOf[JString].values)
    }
    d.retweetCountFields.setValue((t \ "retweet_count").asInstanceOf[JInt].num.intValue)
    d.favoriteCountFields.setValue((t \ "favorite_count").asInstanceOf[JInt].num.intValue)
    t \"possibly_sensitive" match {
      case JNothing => d.possiblySensitiveFields.setValue("unknown")
      case v => d.possiblySensitiveFields.setValue(v.asInstanceOf[JBool].value.toString)
    }
    t \ "place" match {
      case JNull =>
      case p =>
        d.placeId.o(d.td).setValue((p \ "id").asInstanceOf[JString].values)
        d.placeFullName.o(d.td).setValue((p \ "full_name").asInstanceOf[JString].values)
        d.placeType.o(d.td).setValue((p \ "place_type").asInstanceOf[JString].values)
        d.country.o(d.td).setValue((p \ "country").asInstanceOf[JString].values)
        (p \ "bounding_box" \ "coordinates")(0) match {
          case JArray(a) =>
            val coords = a.map{ case JArray(a) => (a.head.asInstanceOf[JDouble].num.doubleValue,a(1).asInstanceOf[JDouble].num.doubleValue) }
            d.placeCoordinates.o(d.td).setValue((coords.head._2+coords(1)._2)/2,(coords.head._1+coords(1)._1)/2)
          case JNothing =>
        }
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
        () => merge(opts.index()+"/tindex", ts,opts.indexMemoryMb() / 1, toCodec(opts.tpostings(), termVectorFields))
      ),
    )
  }
}
