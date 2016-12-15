package com.tikalk.neo

import org.apache.spark.SparkConf
import org.apache.spark._
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.DeserializationFeature
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Milliseconds
import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds

object Main {
  
  def main(args: Array[String]) {
    
    val dir = args(0)
    
    val conf = new SparkConf().setAppName("Word Count")
        .setMaster("local[2]") //TODO: remove this line and add to VM options: -Dspark.master=local[2]
        .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    
    val pages = sc.wholeTextFiles(s"$dir/*.json", 1).map { file =>
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      val page = mapper.readValue(file._2, classOf[Page])
      page.near_earth_objects
    }.collect()
    
    sc.stop()
    
    println(s"pages: ${pages.length}")
    
    val ssc = new StreamingContext(conf, Seconds(5))
    val rddQueue = new Queue[RDD[NearEarthObject]]
    val inputStream = ssc.queueStream(rddQueue)
    
    val hazardousStream = inputStream.filter(_.is_potentially_hazardous_asteroid)
    val countStream = hazardousStream.count
    
    print("count stream: ")
    countStream.print
    
    ssc.start()
    
    pages.foreach { page => rddQueue += ssc.sparkContext.makeRDD(page.toSeq, 1) }
    
    ssc.awaitTermination()
    ssc.stop()
    
    println("Done")
  }
}