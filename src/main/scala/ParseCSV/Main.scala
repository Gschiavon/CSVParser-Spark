package ParseCSV

import Utils.CommonUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import collection.JavaConversions._

object Main extends App {

  implicit val conf = ConfigFactory.load

  val sc = new SparkContext(new SparkConf().setAppName(conf.getString("spark.appName")))
  val sqlCtx = SQLContext.getOrCreate(sc)

  val files = conf.getStringList("filePaths.files")

  for (file <- files){

    println("************************************************")
    println("************************************************")
    println("************************************************")
    println("**************DATAFRAME******************")
    val dataFrame = ProcessFile(sqlCtx).readFile(file)
    val utils = new CommonUtils().saveDF(dataFrame)
    dataFrame.show(20)
  }

}
