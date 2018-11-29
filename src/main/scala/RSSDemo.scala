import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

object RSSDemo {
  def main(args: Array[String]) {
    // Configure Twitter credentials using twitter.txt
    // setupTwitter()
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(15))
    // setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None, Array("Space X"))

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())
    statuses.print()

    ssc.start()
    ssc.awaitTermination()

  }
}