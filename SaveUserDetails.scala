package com.xuvantum.streaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.log4j.Level
import Utilities._

object SaveUserDetails {
  def main(args:Array[String]):Unit = {
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]","SaveUserDetails",Seconds(1))
    
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc,None)
    
    val statuses = tweets.map(stat => stat.getUser())
    
    var userCount:Long = 0
    
    statuses.foreachRDD((rdd,time)=>{
      if(rdd.count() > 0){
        val repartitionRDD = rdd.repartition(1).cache()
        repartitionRDD.saveAsTextFile("UserDetails_"+time.milliseconds.toString)
//        rdd.saveAsTextFile("userDetails")
        userCount += repartitionRDD.count()
        println("User count"+userCount)
        if(userCount>5000){
          System.exit(0)
        }
      }
    })
    
    ssc.checkpoint("C:/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }  
}