import com.github.nscala_time.time.Imports._
import com.mongodb.casbah.Imports._
import com.mongodb.MongoCredential
import java.util.Date;
import scala.collection.immutable._

object Main {

 
  def main(args: Array[String]): Unit = {
    val collection = MongoFactory.collection
    createIDF(collection).foreach(println)
  }

  
  def returnIDF(collection: MongoCollection): Map[String, Double] = {
    val query = MongoDBObject("CountryCode" -> "US")
    val words = scala.collection.mutable.Set[String]()
    val tweets = collection.find(query)
    tweets.foreach(x => words ++= buildTweet(x).termFreq.keys)
    createIDF(words.toSet)
  }
  
  def createIDF(wordSet: Set[String]): Map[String, Double] = {
    
  }
  
  
  def buildTweet(obj: MongoDBObject): Tweet = {
    val text = obj.getAs[String]("Text").get;
    val location = obj.getAs[BasicDBObject]("Location").get
    val coordinates = location.getAs[MongoDBList]("coordinates").get
    val date = obj.getAs[Date]("TweetTime").get
    val hashtags = obj.getAs[MongoDBList]("HashTags").get.toList
    new Tweet(text, coordinates, date, hashtags)
  }

}