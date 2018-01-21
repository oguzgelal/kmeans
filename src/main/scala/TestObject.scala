import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object TestObject {
  type Tag = String
  type Likes = Int
  case class Tweet(id: BigInt, text: String, hashTags: Array[Tag], likes: Likes)

  // Settings
  val CLUSTER_COUNT = 5
  val MAX_ITERATION = 20
  val MEANS_DELTA_THRESHOLD = 1

  val sc: SparkContext = new SparkContext(
    new SparkConf()
    .setAppName("Twitter Example")
    .setMaster("local")
  )


  // Parse tweets
  def parseTweet(tweet: String): Tweet = {
    var id = ""; var text = ""; var likes = 0
    var hashtags: Array[String] = new Array(0)

    try {
      implicit val formats = DefaultFormats
      val t = parse(tweet)

      // Extract the id
      try { id = (t \ "id_str").extract[String]; }
      catch { case e: Exception => (/* do nothing */) }
      if (id == ""){ return null; }

      // Extract tweet text
      try { text = (t \ "text").extract[String]; }
      catch { case e: Exception => (/* do nothing */) }
      if (text == ""){ return null; }

      // Extract hashtags
      try { hashtags = (t \ "entities" \ "hashtags" \ "text").extract[Array[String]]; }
      catch { case e: Exception => (/* do nothing */) }

      // Extract popularity
      try { likes = (t \ "retweeted_status" \ "favorite_count").extract[String].toInt; }
      catch {
        case e: Exception =>
          try {
            // post is not a retweet - estimate by user avg
            val userTotalFavs = (t \ "user" \ "favourites_count").extract[String].toInt
            val userTotalPosts = (t \ "user" \ "statuses_count").extract[String].toInt
            likes = Math.round(userTotalFavs / userTotalPosts);
          } catch {
            case e: Exception => (/* do nothing */)
          }
      }

      // Return tweet object
      Tweet(BigInt(id), text, hashtags, likes);
    } catch {
      case e: Exception =>  null
    }
  }


  // Pair hashtags and likes
  def toPairRdd(tweets: RDD[Tweet]): RDD[(Tag, Likes)] = {
    tweets
      .filter(_ != null)
      .filter(_.hashTags != null)
      .filter(_.hashTags.length > 0)
      .flatMap(t => {
        t.hashTags.map((_, t.likes))
      })
  }


  // Sum up likes of the same hashtags
  def toScores(pairRDD: RDD[(Tag, Likes)]): RDD[(Tag, Int)] = {
    pairRDD.reduceByKey(_ + _)
  }


  // Find top 20 most popular hashtags
  def mostTrending(scores: RDD[(Tag, Int)]): Array[Tag] = {
    scores
      .sortBy(_._2, false)
      .keys
      .take(20)
  }


  // Cluster tags by the likes
  def trendingSets(pairRDD: RDD[(Tag, Likes)]): Map[Tag, RDD[Likes]] = {
    // TODO: sort by trending
    pairRDD
      .groupByKey()
      .mapValues(v => { this.sc.parallelize(v.toList) })
      .take(20)
      .toMap
  }


  // Compute initial values of means
  def sampleVector(likes: RDD[Likes], k: Int): Array[Int] = {
    null
  }


  // K-means algorithm
  def kmeans(means: Array[Int], vector: RDD[Likes]): Array[(Int, Int)] = {
    this.kmeansAcc(means, null, vector, 0)
  }
  def kmeansAcc(means: Array[Int], oldMeans: Array[Int], vector: RDD[Likes], iteration: Int): Array[(Int, Int)] = {
    // Base case - stop condition holds - end recursion and return clusters
    if (false){
      null
    }
    // Compute clusters
    else {
      null
    }
  }


  // Pretty print
  def printResults(tag: Tag, meansAndCount: Array[(Int, Int)]) = {
    null
  }


  def main(args: Array[String]) {

    // Get source file
    val source = this.sc.textFile("/data/twitter/tweets")

    // Parse tweets into Tweet array
    val tweets: RDD[Tweet] = source.map(this.parseTweet)

    // Pair hashtags and like counts
    // This value is cached, because we don't want to
    // recompute it for Task 1 and Task 2 separately
    val pairRDD = this.toPairRdd(tweets).cache()

    // --- Task 1 --- //

    // Sum up likes for each distinct keyword
    val scores = this.toScores(pairRDD)

    // 20 op hashtags
    val trending = mostTrending(scores)

    // --- Task 2 --- //

    // Map each hashtag to the likes they got, take 20
    val sets = trendingSets(pairRDD)

    // Run kmeans algorithm
    for((tag, rdd) <- sets) {
      val meansAndCount = this.kmeans(this.sampleVector(rdd, this.CLUSTER_COUNT), rdd)
      this.printResults(tag, meansAndCount)
    }

    this.sc.stop()
  }


}
