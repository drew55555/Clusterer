import com.mongodb.casbah.Imports._
import java.util.Date;
import scala.collection.immutable._
import scala.math._

object Main {

 
  def main(args: Array[String]): Unit = {
    val factory = new MongoFactory(args(0), args(1))
    returnIDF(factory.collection).foreach(println)
  }

  
  def returnIDF(collection: MongoCollection): Map[String, Double] = {
    val query = MongoDBObject("CountryCode" -> "US")
    val words = scala.collection.mutable.ArrayBuffer[String]()
    val tweets = collection.find(query)
    val docCount = tweets.count
    tweets.foreach(x => words ++= buildTweet(x).termFreq.keys)
    idfMap(words.groupBy(x => x), docCount)//.mapValues(x => idf(x.size, docCount))
  }
  
  def idfMap(elements: Map[String, scala.collection.mutable.ArrayBuffer[String]], docCount: Double): Map[String, Double] = {
    elements.flatMap(x => if (x._2.size > 1) Some(x._1 -> idf(x._2.size.toDouble, docCount)) else None)
  }
  
  def idf(numTerms: Double, docCount: Double): Double = {
    log(docCount / numTerms)
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