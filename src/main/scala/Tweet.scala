import com.github.nscala_time.time.Imports._
import com.mongodb.casbah.Imports._
import java.util.Date;

class Tweet(text: String, location: MongoDBList, date: Date) {
  
  val Text: String = text
  val Location: MongoDBList = location
  val Date: Date = date
}