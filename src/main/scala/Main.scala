import com.mongodb.casbah.Imports._
import java.util.Date
import scala.collection.immutable._
import scala.math._
import scala.annotation.tailrec
import sun.org.mozilla.javascript.ast.Yield

object Main {

  def main(args: Array[String]): Unit = {
    val query = MongoDBObject("CountryCode" -> "US")
    val tweetNum = args(2).toInt
    val factory = new MongoFactory(args(0), args(1))
    val idf = createIDF(factory.collection, query)
    val tweets = getTweets(factory.collection, query)
    val first = tweets(tweetNum)
    val dists = for (x <- tweets.slice(tweetNum + 1, tweets.length))
      yield Tuple2(x, tweetDist(first, x, idf))
    val minDist = dists.sortBy(x => x._2)
    println(first.Text)
    println
    minDist.take(25).foreach(x => println(x._1.Text + "\t" + x._2))
  }
  def getTweets(mongo: MongoCollection, query: MongoDBObject): List[Tweet] = {
    val res = for(x <- mongo.find(query)) yield buildTweet(x)
    res.toList
  }

  def createIDF(collection: MongoCollection, query: MongoDBObject): Map[String, Double] = {
    val words = scala.collection.mutable.ArrayBuffer[String]()
    val tweets = collection.find(query)
    val docCount = tweets.count
    tweets.foreach(x => words ++= buildTweet(x).termFreq.keys)
    idfMap(words.groupBy(x => x), docCount) //.mapValues(x => idf(x.size, docCount))
  }

  def idfMap(elements: Map[String, scala.collection.mutable.ArrayBuffer[String]], docCount: Double): Map[String, Double] = {
    elements.flatMap(x => if (x._2.size > 1) Some(x._1 -> idf(x._2.size.toDouble, docCount)) else None)
  }

  def idf(numTerms: Double, docCount: Double): Double = {
    log(docCount / numTerms)
  }

  
  def tweetDist(tweet1: Tweet, tweet2: Tweet, idfResult: Map[String, Double]): Double = {
    val wordSet = (tweet1.termFreq.keys ++ tweet2.termFreq.keys).toList
    @tailrec
    def dist(words: List[String], accu: Double): Double = {
      words match {
        case word :: tail => dist(tail, accu + diffSqares(tweet1.termFreq.getOrElse[Int](word, 0) * idfResult.getOrElse[Double](word, 0), tweet2.termFreq.getOrElse[Int](word, 0) * idfResult.getOrElse[Double](word, 0)))
        case _ => accu
      }
    }
    dist(wordSet, 0)
  }

  def diffSqares(x: Double, y: Double): Double = {
    pow(x - y, 2)
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