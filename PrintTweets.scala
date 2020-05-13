package com.xuvantum.streaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.log4j.Level
import Utilities._


object PrintTweets {
  def main(args:Array[String]){
    //setting up twitter Auth using twitter.txt creds
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]","Printing Tweets",Seconds(1))
    
    //get rid of logger
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc,None)
    val user = tweets.map(stat => (stat.getText(),stat.getUser()))
    
    user.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}