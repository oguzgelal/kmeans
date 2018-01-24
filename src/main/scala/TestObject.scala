import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.Random

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
  def trendingSets(trends: Array[Tag], pairRDD: RDD[(Tag, Likes)]): Array[(Tag, RDD[Likes])] = {
    trends.map(h => (h, this.sc.parallelize(pairRDD.lookup(h))))
  }


  // Take sample from likes to compute initial values of means
  def sampleVector(likes: RDD[Likes], size: Int): Array[Int] = {
    likes.distinct.takeSample(false, size)
  }


  // K-means algorithm
  def kmeans(means: Array[Int], vector: RDD[Likes]): Array[(Int, Int)] = {
    this.kmeansAcc(means, null, null, vector, 0)
  }

  // accumulator for the recursive k-means algorithm
  def kmeansAcc(means: Array[Int], oldMeans: Array[Int], clusters: Array[(Int, Int)], vector: RDD[Likes], iteration: Int): Array[(Int, Int)] = {

    // TODO: calculate delta x for means

    // Base case: stop condition holds - end recursion and return clusters
    if (iteration > this.MAX_ITERATION){ clusters }

    // Calculate new means
    else {

      // Calculate clusters (<cluster>, <value>)
      // Find the closest mean for each vector, and pair them up
      val newClusters = vector.map(v => {
        var closestMean = -1
        var closestDistance = -1
        means.foreach(mean => {
          var distance = Math.abs(mean - v)
          if (distance < closestDistance || closestDistance == -1){
            closestDistance = distance
            closestMean = mean
          }
        })
        (closestMean, v)
      })

      // Calculate new means based on the new clusters
      val newMeans = newClusters
        // Pair values with 1's to keep count
        .mapValues { case value => (value, 1) }
        // Sum up the values and the 1's
        .reduceByKey { case ((vAcc, cAcc), (v, c)) => (vAcc + v, cAcc + c)}
        // Divide the sum by the counts to find average
        .mapValues { case (sum , count) => sum / count }
        // Extract the values into an array to get the new means
        .values

      kmeansAcc(newMeans.collect(), means, newClusters.collect(), vector, iteration + 1)
    }
  }


  // Pretty print
  def printResults(tag: Tag, meansAndCount: Array[(Int, Int)]) = {
    null
  }


  // **** MAIN ****
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
    val trending = this.mostTrending(scores)

    // --- Task 2 --- //

    // Map each hashtag to the likes they got, take 20
    val sets = this.trendingSets(trending, pairRDD)

    // Run kmeans algorithm
    for((tag, likes) <- sets) {

      // TODO: remove this and use the likes above
      val likesTmp = this.sc.parallelize(Array(Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100)))

      val sample = this.sampleVector(likesTmp, this.CLUSTER_COUNT)

      println("Samples")
      println(sample.mkString(", "))

      val meansAndCount = this.kmeans(sample, likesTmp)

      println("MeansAndCount")
      println(meansAndCount.mkString("\n"))

      //this.printResults(tag, meansAndCount)
    }

    this.sc.stop()
  }


}
