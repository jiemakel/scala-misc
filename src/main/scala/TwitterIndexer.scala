import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Locale

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

  }
  
  val tld = new java.lang.ThreadLocal[Reuse] {
    override def initialValue() = new Reuse()
  }
  
  private def index(file: File): Unit = {
    parse(new InputStreamReader(new FileInputStream(file)), (p: Parser) => {
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

  private var indexRetweets: Boolean = true
  private var indexQuoted: Boolean = true
  private var indexReplies: Boolean = true
 
  private def index(t: JObject): Unit = {
    val ttype = t \ "retweeted_status" match {
      case JNull | JNothing => t \ "quoted_status" match {
        case JNull | JNothing =>
          t \ "in_reply_to_screen_name" match {
            case JNull | JNothing => "tweet"
            case _ =>
              "reply"
          }
        case _ => "quoted"
      }
      case _ => "retweet"
    }
    val index = ttype match {
      case "reply" => indexReplies
      case "retweet" => indexRetweets
      case "quoted" => indexQuoted
      case "tweet" => true
    }
    if (index) {
      val t2 = if (ttype == "retweet") t \ "retweeted_status" else t
      val d = tld.get
      d.td.clearOptional()
      d.idFields.setValue((t \ "id_str").asInstanceOf[JString].values)

      d.tweetType.setValue(ttype)
      d.timeFields.setValue(isoDateTimeFormat.print(twitterDateTimeFormat.parseMillis((t \ "created_at").asInstanceOf[JString].values)))
      val text = t \ "retweeted_status" match {
        case JNull | JNothing => t \ "text" match {
          case JString(s) => s
          case JNothing => (t \ "full_text").asInstanceOf[JString].values
        }
        case rt => "RT @" + (rt \ "user" \ "screen_name").asInstanceOf[JString].values + ": " + (rt \ "text" match {
          case JString(s) => s
          case JNothing => (rt \ "full_text").asInstanceOf[JString].values
        })
      }
      d.textField.setValue(text)
      d.contentLengthFields.setValue(text.length)
      d.contentTokensFields.setValue(d.textField.numberOfTokens)
      d.truncatedFields.setValue((t \ "truncated").asInstanceOf[JBool].values.toString)
      for (v <- (t2 \ "entities" \ "hashtags" \ "text").asInstanceOf[JArray].arr)
        new StringSSDVFieldPair("hashtags").o(d.td).setValue(v.asInstanceOf[JString].values.toLowerCase,v.asInstanceOf[JString].values)
      for (v <- (t2 \ "entities" \ "user_mentions").asInstanceOf[JArray].arr) {
        new StringSSDVFieldPair("userMentionsUserId").o(d.td).setValue((v \ "id_str").asInstanceOf[JString].values)
        new StringSSDVFieldPair("userMentionsScreenName").o(d.td).setValue((v \ "screen_name").asInstanceOf[JString].values)
      }
      for (v <- (t2 \ "entities" \ "urls" \ "expanded_url").asInstanceOf[JArray].arr)
        new StringSSDVFieldPair("urls").o(d.td).setValue(v.asInstanceOf[JString].values)
      d.languageFields.setValue((t \ "lang").asInstanceOf[JString].values)
      d.sourceFields.setValue((t \ "source").asInstanceOf[JString].values)
      t \ "extended_entities" \ "media"  \ "media_url" match {
        case JNull | JNothing =>
        case JArray(v) => for (JString(str) <-v)
          new StringSSDVFieldPair("mediaURL").o(d.td).setValue(str)
      }
      t \ "extended_entities" \ "media" \ "type" match {
        case JNull | JNothing =>
        case JArray(v) => for (JString(str) <-v)
          new StringSSDVFieldPair("mediaType").o(d.td).setValue(str)
      }
      t \ "in_reply_to_user_id_str" match {
        case JNull =>
        case JString(str) => d.inReplyToUserIdFields.setValue(str)
      }
      t \ "in_reply_to_screen_name" match {
        case JNull | JNothing =>
        case JString(str) =>
          d.inReplyToScreenNameFields.setValue(str)
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
        case rt =>
          d.retweetIdFields.setValue((rt \ "id_str").asInstanceOf[JString].values)
          val rtuser = (rt \ "user").asInstanceOf[JObject]
          d.retweetUserIdFields.setValue((rtuser \ "id_str").asInstanceOf[JString].values)
          d.retweetScreenNameFields.setValue((rtuser \ "screen_name").asInstanceOf[JString].values)
      }
      t \ "quoted_status" match {
        case JNull | JNothing =>
        case rt =>
          d.quotedIdFields.setValue((rt \ "id_str").asInstanceOf[JString].values)
          val rtuser = (rt \ "user").asInstanceOf[JObject]
          d.quotedUserIdFields.setValue((rtuser \ "id_str").asInstanceOf[JString].values)
          d.quotedScreenNameFields.setValue((rtuser \ "screen_name").asInstanceOf[JString].values)
      }
      d.retweetCountFields.setValue((t \ "retweet_count").asInstanceOf[JInt].num.intValue)
      d.favoriteCountFields.setValue((t \ "favorite_count").asInstanceOf[JInt].num.intValue)
      t \ "possibly_sensitive" match {
        case JNothing => d.possiblySensitiveFields.setValue("unknown")
        case v => d.possiblySensitiveFields.setValue(v.asInstanceOf[JBool].value.toString)
      }
      t \ "place" match {
        case JNull =>
        case p =>
          d.placeId.setValue((p \ "id").asInstanceOf[JString].values)
          d.placeFullName.setValue((p \ "full_name").asInstanceOf[JString].values)
          d.placeType.setValue((p \ "place_type").asInstanceOf[JString].values)
          d.country.setValue((p \ "country").asInstanceOf[JString].values)
          (p \ "bounding_box" \ "coordinates") (0) match {
            case JArray(a) =>
              val coords = a.map { case JArray(a) => (a.head.asInstanceOf[JDouble].num.doubleValue, a(1).asInstanceOf[JDouble].num.doubleValue) }
              d.placeCoordinates.setValue((coords.head._2 + coords(1)._2) / 2, (coords.head._1 + coords(1)._1) / 2)
            case JNothing =>
          }
      }
      tiw.addDocument(d.td)
    }
  }
  
  var tiw: IndexWriter = null.asInstanceOf[IndexWriter]

  val ts = new Sort(new SortField("id",SortField.Type.STRING))

  def main(args: Array[String]): Unit = {
    val opts = new AOctavoOpts(args) {
      val tpostings = opt[String](default = Some("fst"))
      val noRetweets= opt[Boolean]()
      val noReplies = opt[Boolean]()
      val noQuoted = opt[Boolean]()
      verify()
    }
    this.indexQuoted = !opts.noQuoted()
    this.indexReplies = !opts.noReplies()
    this.indexRetweets = !opts.noRetweets()
    if (!opts.onlyMerge()) {
      tiw = iw(opts.index()+"/tindex",ts,opts.indexMemoryMb() / 1)
      feedAndProcessFedTasksInParallel(() =>
        opts.directories().toStream.flatMap(n => getFileTree(new File(n))).filter(_.getName.endsWith(".jsonl")).foreach(file => addTask(file.getName, () => index(file)))
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
