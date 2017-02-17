package CJFiles

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object CSVCJPublisher extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CJPublisher.avsc"))
  val sqlCtx = SQLContext.getOrCreate(sc)
  import sqlCtx.implicits._

  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/CJ_Commission_API_20170123.txt")

  val header = rawData.first
  val data = rawData
    .filter(line => line != header)
    .map(_.split(","))
    .map(values => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16)))
    .toDF("cid","country","currency","network_rating","publisher_name","seven_day_epc","three_month_epc","program_name","date_accepted","date_expired","join_status","website_name","website_url","website_category","website_pid","promotional_method","insert_dt")

  data.take((20)).foreach(println)
  data.show(20)
  data.printSchema()

}
