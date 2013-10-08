import com.mongodb.casbah.Imports._

object Common {
  def buildMongoDbObject(tweet: Tweet): MongoDBObject = {
    val locBuilder = MongoDBObject.newBuilder
    locBuilder += "type" -> "Point"
    locBuilder += "location" -> tweet.Location
    val builder = MongoDBObject.newBuilder
    builder += "Text" -> tweet.Text
    builder += "Location" -> locBuilder.result
    builder += "TweetTime" -> tweet.Date.toString()
    builder.result
  }
}