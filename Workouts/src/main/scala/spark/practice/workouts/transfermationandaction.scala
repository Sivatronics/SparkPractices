package spark.practice.workouts

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.io

object transfermationandaction {
  def main(args: Array[String]) = {
    //flatvsmap()
    workingwithKeyValuePairs()

  }
  def flatvsmap() = {
    val conf = new SparkConf().setAppName("transfermationandaction").setMaster("local")
    val sc = new SparkContext(conf)
    //    val rdd= sc.textFile("panda.txt")
    val rdd = sc.parallelize(List("coffee panda", "happy panda", "happiest panda party"))
    rdd.flatMap(x => x.split(" ")).saveAsTextFile("flatmapresults")
    rdd.map(y => (y, 1)).saveAsTextFile("mapresults")

  }
  def workingwithKeyValuePairs() = {
    val conf = new SparkConf().setAppName("transfermationandaction").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("map.txt")
    //lines.saveAsTextFile("map.rdd.txt")
    
    //Converting string input into Int. 
    //x(0).toString().toInt - To understand why do I convert input string values to a string then to a Int? 
    // http://stackoverflow.com/questions/42016117/why-spark-map-produces-different-result-as-rdd-when-input-element-converted-to-i 
    val pairRDD = lines.map(x => (x(0).toString().toInt, x(1).toString().toInt))
    pairRDD.saveAsTextFile("map.pairrdd.txt")
    
    //reduceByKey Function
    val reducedByKeyRDD = pairRDD.reduceByKey((x, y) => x + y)
    reducedByKeyRDD.saveAsTextFile("map.reduceByKey.txt")

    //groupByKey Function
    val groupByKey = pairRDD.groupByKey()
    groupByKey.saveAsTextFile("map.groupByKey.txt")
    
    //mapValue Function
    val mapValue=pairRDD.mapValues(x=>x+1)
    mapValue.saveAsTextFile("map.mapValue.txt")
    
    //flatMapValue Function
    val flatMapValue=pairRDD.flatMapValues(x=>(2 to 5))
    flatMapValue.saveAsTextFile("map.flatMapValue.txt")
    
    //keys Function
    val keys=pairRDD.keys
    keys.saveAsTextFile("map.keys.txt")
    
    //values Function
    val values=pairRDD.values
    values.saveAsTextFile("map.values.txt")
    
    //sortByKey Function
    val sortByValue=pairRDD.sortByKey()
    sortByValue.saveAsTextFile("map.soryByvalues.txt")
    

  }
}