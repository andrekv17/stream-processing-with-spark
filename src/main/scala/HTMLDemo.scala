import java.net.URL

import com.github.catalystcode.fortis.spark.streaming.html.HTMLInputDStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object HTMLDemo {
    val durationSeconds = 60
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")

    val urls = Array("https://queryfeed.net/tw?token=5bfec0d2-4657-4d2a-98d0-69f3584dc3b3&q=%23weather") //urlCSV.split(",")
    val stream = new HTMLInputDStream(
      urls,
      ssc,
      requestHeaders = Map[String, String](
        "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
      )
    )
    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      rdd.take(15).foreach(println)
      //import spark.sqlContext.implicits._
      //rdd.toDS().show(100)
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }