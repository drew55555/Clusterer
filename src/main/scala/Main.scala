import com.github.nscala_time.time.Imports._
import com.mongodb.casbah.Imports._
import java.util.Date;

object Main {

  def main(args: Array[String]): Unit = {

    val collection = MongoFactory.collection
    val tweets = collection.find
    while (tweets.hasNext) {
      val tweet = buildTweet(tweets.next)
      println(tweet.Location(0))
    }
  }

  def buildTweet(obj: MongoDBObject): Tweet = {
    val text = obj.getAs[String]("Text").get;
    val location = obj.getAs[BasicDBObject]("Location").get
    val coordinates = location.getAs[MongoDBList]("coordinates").get
    val date = obj.getAs[Date]("TweetTime").get
    new Tweet(text, coordinates, date)
  }

}