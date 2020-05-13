package com.xuvantum.streaming


import org.apache.log4j.Logger

object Utilities {
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
  }
  
  def setupTwitter() = {
    import scala.io.Source
    
    val lines = Source.fromFile("../twitter.txt").getLines()
    for(line <- lines){
      val fields = line.split(" ")
      if(fields.length == 2){
        System.setProperty("twitter4j.oauth." +fields(0), fields(1))
      }
    }
  }
}