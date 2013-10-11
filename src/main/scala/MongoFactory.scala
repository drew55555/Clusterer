import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.MongoConnection
import com.mongodb._

object MongoFactory {
  private val SERVER = "localhost"
  private val PORT = 27017
  private val DATABASE = "twitterDB"
  private val COLLECTION = "TweetData"

  val connection = MongoConnection(SERVER)
  connection(DATABASE).authenticate("******", "*********")
  val collection = connection(DATABASE)(COLLECTION)
}