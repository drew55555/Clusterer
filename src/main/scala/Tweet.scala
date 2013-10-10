import com.github.nscala_time.time.Imports._
import com.mongodb.casbah.Imports._
import java.util.Date;
import scala.reflect.internal.util.Collections
import scala.collection.immutable.Map
import scala.collection.mutable._

class Tweet(text: String, location: MongoDBList, date: Date, hashtags: List[Any]) {

  val Text: String = text
  val Location: MongoDBList = location
  val Date: Date = date

  val termFreq: scala.collection.immutable.Map[String, Int] = {
    var words = Text.split("[^A-Za-z]+").toList
    for(x <- hashtags)
      words = x.toString.toUpperCase() :: words
    words.filter(x => x.toString.length() > 2).groupBy(x => x.toUpperCase()).mapValues(_.size)
  }
}