import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.rdd.RDD
import scala.util.Random

object TestObject {
  type Tag = String
  type Likes = Int
  case class Tweet(id: BigInt, text: String, hashTags: Array[Tag], likes: Likes)

  // Settings
  val ISABELLE = false

  var FILE_LOCATION = "/data/twitter/tweets"
  if (ISABELLE){ FILE_LOCATION = "/data/twitter/tweetsraw" }
  var SET_MASTER = true
  if (ISABELLE){ SET_MASTER = false }

  val CLUSTER_COUNT = 5
  val MAX_ITERATION = 20
  val MEANS_DELTA_THRESHOLD = 1

  val conf = new SparkConf()

  conf.setAppName("TwitterProject")
  if (SET_MASTER) { conf.setMaster("local") }
  val sc: SparkContext = new SparkContext(conf)

  sc.setLogLevel("ERROR")

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
      .take(20)
      .map(_._1)

  }


  // Cluster tags by the likes
  def trendingSets(trends: Array[Tag], pairRDD: RDD[(Tag, Likes)]): Array[(Tag, RDD[Likes])] = {
    trends.map(h => (h, sc.parallelize(pairRDD.lookup(h))))
  }


  // Take sample from likes to compute initial values of means
  def sampleVector(likes: RDD[Likes], size: Int): Array[Int] = {
    likes.distinct.takeSample(false, size)
  }


  // K-means algorithm
  def kmeans(means: Array[Int], vector: RDD[Likes]): Array[(Int, Int)] = {
    kmeansAcc(means, null, null, vector, 0)
  }

  // accumulator for the recursive k-means algorithm
  def kmeansAcc(means: Array[Int], oldMeans: Array[Int], clusters: Array[(Int, Int)], vector: RDD[Likes], iteration: Int): Array[(Int, Int)] = {

    // Calculate delta x of the means Trade off:
    // 1) should I halt if just one means delta x falls under the treshold ? If by chance, one of the sample means
    // falls into the right location, the algorithm will halt without properly computing other means
    // 2) should I half if all deta x'es fall under the treshold ? This could potentially cause performance issues, but
    // there is already max iteration parameter, and also this will generate more accurate results. I'll go with this
    var meansDeltaTresholdReached = true
    if (oldMeans != null){
      (means zip oldMeans).foreach{
        case (newMean, oldMean) =>
          if (Math.abs(newMean - oldMean) > MEANS_DELTA_THRESHOLD){
            meansDeltaTresholdReached = false
          }
      }
    } else {
      meansDeltaTresholdReached = false
    }

    // Base case: stop condition holds - end recursion and return clusters
    if (meansDeltaTresholdReached || iteration > MAX_ITERATION){

      // Count number of data points in each cluster (<cluster>, <count>)
      val pairsRdd = sc.parallelize(clusters)
      pairsRdd
        .mapValues{ case value => (value, 1) }
        .reduceByKey { case ((vAcc, cAcc), (v, c)) => (vAcc + v, cAcc + c)}
        .mapValues { case (sum , count) => count }
        .collect()
    }

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
    println("_____________________________")
    println("Tag: " + tag)
    meansAndCount.foreach{
      case (mean, count) => {
        println("Cluster (" + mean + ") -> " + count + " items")
      }
    }
  }


  // **** MAIN ****
  def main(args: Array[String]) {

    println("> reading file...")

    // Get source file
    val source = sc.textFile(FILE_LOCATION)

    println("> reading file completed")
    println("> parsing tweets...")

    // Parse tweets into Tweet array
    val tweets: RDD[Tweet] = source.map(parseTweet)

    println("> parsing tweets completed")
    println("> generating pairrdd...")

    // Pair hashtags and like counts
    // This value is cached, because we don't want to
    // recompute it for Task 1 and Task 2 separately
    val pairRDD = toPairRdd(tweets).cache()

    println("> generating pairrdd completed")

    // --- Task 1 --- //

    println("> pairing scores...")

    // Sum up likes for each distinct keyword
    val scores = toScores(pairRDD)

    println("> pairing scores completed")
    println("> finding top tweets...")

    // 20 op hashtags
    val trending = mostTrending(scores)

    println("> finding top tweets completed")

    // --- Task 2 --- //

    println("> finding trending sets...")

    // Map each hashtag to the likes they got, take 20
    val sets = trendingSets(trending, pairRDD)

    println("> finding trending sets completed")
    println("> clustering...")

    // Run kmeans algorithm
    for((tag, likes) <- sets) {
      // I will use this instead of the real likes
      val likesTmp = sc.parallelize(Array(Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100), Random.nextInt(100)))
      val sample = sampleVector(likesTmp, CLUSTER_COUNT)
      val meansAndCount = kmeans(sample, likesTmp)
      printResults(tag, meansAndCount)
    }

    println("> clustering completed")

    sc.stop()
  }


}
