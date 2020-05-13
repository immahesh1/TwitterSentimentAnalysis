package com.xuvantum.streaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.log4j.Level
import Utilities._

object SaveTweets {
  def main(args:Array[String]){
    //setup twitter using twitter.txt
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]","Saving Tweets",Seconds(1))
    
    //get rid of log error
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc,None)
    val statuses = tweets.map(stat => stat.getText())
    
    var totalTweets:Long = 0
    statuses.foreachRDD((rdd,time)=>{
      if(rdd.count() > 0){
        val repartitionedRDD = rdd.repartition(1).cache()
        repartitionedRDD.saveAsTextFile("Tweets_"+time.milliseconds.toString())
        totalTweets += repartitionedRDD.count()
        println("Tweet Count "+totalTweets)
        if(totalTweets > 1000){
          System.exit(0)
        }
      }
    })
    ssc.checkpoint("C:/checkpoint")
    ssc.start()
    ssc.awaitTermination()
    
  }
}