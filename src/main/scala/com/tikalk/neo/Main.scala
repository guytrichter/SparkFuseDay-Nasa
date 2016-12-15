package com.tikalk.neo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.DeserializationFeature

object Main {
  
  def main(args: Array[String]) {
    
    val dir = args(0)
    
    val conf = new SparkConf().setAppName("Word Count")
        .setMaster("local[2]") //TODO: remove this line and add to VM options: -Dspark.master=local[2]
    val sc = new SparkContext(conf)
    val objects = sc.wholeTextFiles(s"$dir/*.json", 1).flatMap { file =>
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      val page = mapper.readValue(file._2, classOf[Page])
      page.near_earth_objects
    }
    
    val hazardous = objects.filter(_.is_potentially_hazardous_asteroid)
    
    println(hazardous.count())
    
    println("Done")
  }
}