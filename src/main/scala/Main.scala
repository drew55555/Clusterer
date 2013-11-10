import com.mongodb.casbah.Imports._
import java.util.Date
import scala.collection.immutable._
import scala.math._
import scala.annotation.tailrec
import sun.org.mozilla.javascript.ast.Yield
import org.bson._
import javax.xml.soap.Text
import sun.font.TrueTypeFont

object Main {

  def main(args: Array[String]): Unit = {
    //val query = MongoDBObject("CountryCode" -> "US")
    val nelat = 41
    val nelon = -73
    val swlat = 40
    val swlon = -74
    val nwlat = nelat
    val nwlon = swlon
    val selat = swlat
    val selon = nelon
    val geo = MongoDBObject("$geometry" ->
      MongoDBObject("type" -> "Polygon",
        "coordinates" -> List(((GeoCoords(nelon, nelat),
          GeoCoords(selon, selat),
          GeoCoords(swlon, swlat),
          GeoCoords(nwlon, nwlat),
          GeoCoords(nelon, nelat))))))
    val query = "Location" $geoWithin (geo)
    val factory = new MongoFactory(args(0), args(1))
    val tweets = getTweets(factory.collection, query)
    val count = tweets.count(x => true)
    println(count + " Tweets Read")
    val idf = createIDF(tweets.values, count)
    println("IDF Created")
    findNeighboors(tweets.values, idf)
    println("Neighboors found")
    countReverseNeighboors(tweets)
    println("Reverse Neighboors Counted")

    try {
      val popular = tweets.maxBy(x => x._2.revNearCount)._2
      println(popular.Text)
      println
      println
      for (ID <- popular.nearestNeighbors) {
        println(tweets.get(ID._1).get.Text + "\t" + ID._2)

      }
    } catch {
      case e: Exception =>
        println("Exception")
      // TODO: handle exception
    }

    //    val first = tweets(tweetNum)
    //    val dists = for (x <- tweets.slice(tweetNum + 1, tweets.length))
    //      yield Tuple2(x, tweetDotProd(first, x, idf))
    //    val minDist = dists.sortBy(x => x._2)
    //    println(first.Text)
    //    println
    //    minDist.takeRight(25).foreach(x => println(x._1.Text + "\t" + x._2))
  }

  def countReverseNeighboors(tweets: Map[String, Tweet]): Unit = {
    for (
      tweet <- tweets.values;
      neighboor <- tweet.nearestNeighbors
    ) {
      tweets.get(neighboor._1).get.revNearCount += 1
    }
  }

  def findNeighboors(tweets: scala.collection.Iterable[Tweet], idfResult: Map[String, Double]): Unit = {
    for (tweet <- tweets) {
      var dists = (for (
        tempTweet <- tweets if (tweet.ID != tempTweet.ID)
      ) yield new Tuple2(tempTweet.ID, tweetDotProd(tweet, tempTweet, idfResult))).toList
      dists = dists.sortBy(x => x._2)
      tweet.nearestNeighbors ++= dists.takeRight(10)
    }
  }

  def getTweets(mongo: MongoCollection, query: MongoDBObject): Map[String, Tweet] = {
    val res = for (x <- mongo.find(query)) yield buildTweet(x)
    res.toMap
  }

  def createIDF(tweets: scala.collection.Iterable[Tweet], count: Int): Map[String, Double] = {
    val words = scala.collection.mutable.ArrayBuffer[String]()
    tweets.foreach(x => words ++= x.termFreq.keys)
    idfMap(words.groupBy(x => x), count) //.mapValues(x => idf(x.size, docCount))
  }

  def idfMap(elements: Map[String, scala.collection.mutable.ArrayBuffer[String]], docCount: Double): Map[String, Double] = {
    elements.flatMap(x => if (x._2.size > 1) Some(x._1 -> idf(x._2.size.toDouble, docCount)) else None)
  }

  def idf(numTerms: Double, docCount: Double): Double = {
    log(docCount / numTerms)
  }

  def tweetDotProd(tweet1: Tweet, tweet2: Tweet, idfResult: Map[String, Double]): Double = {
    val wordSet = (tweet1.termFreq.keys ++ tweet2.termFreq.keys).toList
    @tailrec
    def dotProd(words: List[String], accu: Double): Double = {
      words match {
        case word :: tail => dotProd(tail, accu + ((tweet1.termFreq.getOrElse[Int](word, 0) * idfResult.getOrElse[Double](word, 0)) * (tweet2.termFreq.getOrElse[Int](word, 0) * idfResult.getOrElse[Double](word, 0))))
        case _ => accu
      }
    }
    dotProd(wordSet, 0)
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

  def buildTweet(obj: MongoDBObject): (String, Tweet) = {
    val id = obj.getAs[types.ObjectId]("_id").get
    val text = obj.getAs[String]("Text").get;
    val location = obj.getAs[BasicDBObject]("Location").get
    val coordinates = location.getAs[MongoDBList]("coordinates").get
    val date = obj.getAs[Date]("TweetTime").get
    val hashtags = obj.getAs[MongoDBList]("HashTags").get.toList
    new Tuple2(id.toString(), new Tweet(id.toString(), text, coordinates, date, hashtags))
  }

}