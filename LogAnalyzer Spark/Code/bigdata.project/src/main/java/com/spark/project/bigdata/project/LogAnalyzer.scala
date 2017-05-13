package com.spark.project.bigdata.project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

object LogAnalyzer {
  
  def main (args: Array[String]){
    val conf = new SparkConf()
    .setAppName("LogAnalyzer")
    .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val accessLogs: RDD[ApacheAccessLog] = sc.textFile("SparkInput.txt")
    .map(ApacheAccessLog.parseLogLine).cache()
    
   
    
    // Any top 5 IPAddress that has accessed the server more than 10 times.
    val Top5IPResult = accessLogs.map(_.ipAddress).map(ip => (ip,1)).reduceByKey(_+_).filter(_._2 > 10).sortBy(-_._2).take(5)
    
    sc.parallelize(Top5IPResult, 1).saveAsTextFile("file:///home/cloudera/workspace/bigdata.project/SparkResults/Top5IPResult")
    
    // get Number of Logs by date 
    
     accessLogs.map(_.dateTime).map(date => (date.substring(0,11),1)).reduceByKey(_+_).sortBy(-_._2)
    .saveAsTextFile("file:///home/cloudera/workspace/bigdata.project/SparkResults/NumberOfLogsByDate")
    
    
     //number of status Codes responses 
    
     accessLogs.map(_.responseCode).map(rc => (rc,1)).reduceByKey(_+_).sortBy(-_._2)
    .saveAsTextFile("file:///home/cloudera/workspace/bigdata.project/SparkResults/NumberofResponses")
     
  
}
    
  
}