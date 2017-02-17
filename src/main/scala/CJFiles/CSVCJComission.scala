package CJFiles

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object CSVCJComission extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CJComission"))
  val sqlCtx = SQLContext.getOrCreate(sc)
  import sqlCtx.implicits._

  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/CJ_Commission_API_20170123.txt")

  val header = rawData.first
  val data = rawData
    .filter(line => line != header)
    .map(_.split(","))
    .map(values => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18), values(19)))
    .toDF("cid","action_type","aid","commission_id","country","event_date","locking_date","order_id","original","original_action_id","posting_date","website_id","action_tracker_id","action_tracker_name","action_status","publisher_name","commission_amount","order_discount","sale_amount","insert_dt")

  data.take((20)).foreach(println)
  data.show(20)
  data.printSchema()


}
