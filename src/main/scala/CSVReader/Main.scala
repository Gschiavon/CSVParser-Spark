package CSVReader

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Main extends App {

  val sc = new SparkContext(new SparkConf().setAppName("reader"))
  val sqlCtx = SQLContext.getOrCreate(sc)

  val dataFrame = sc.textFile("hdfs://hdfs-master:9000//user/csv/output")

  println("*******************************")
  dataFrame.take(39).foreach(println)

}
