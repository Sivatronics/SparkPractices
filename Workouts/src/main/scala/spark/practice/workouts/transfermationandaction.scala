package spark.practice.workouts

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.io


object transfermationandaction {
  def main(args:Array[String])={
    flatvsmap()
  }
  def flatvsmap()={
    val conf= new SparkConf().setAppName("transfermationandaction").setMaster("local")
    val sc= new SparkContext(conf)
//    val rdd= sc.textFile("panda.txt")
    val rdd= sc.parallelize(List("coffee panda","happy panda","happiest panda party"))
    rdd.flatMap(x=>x.split(" ")).saveAsTextFile("flatmapresults")
    rdd.map(y=>(y,1)).saveAsTextFile("mapresults")
    
    }
 }