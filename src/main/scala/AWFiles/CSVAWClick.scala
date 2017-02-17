package AWFiles

import Utils.CommonUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object CSVAWClick extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("click"))
  val sqlCtx = SQLContext.getOrCreate(sc)
  val utils = new CommonUtils

  import sqlCtx.implicits._
  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/AW_Click_20161107.csv")
  val title = rawData.first
  val header = "Campaign ID,Ad group ID,Day,Most specific location target (Location of interest),Campaign location target,Most specific location target (Physical location),Ad ID,Keyword ID,Device,Top vs. Other"

  val data = rawData
    .filter(line => line != title && line != header)
    .map(_.split(","))
    .map(value => (value(0), value(1), value(2), value(3), value(4), value(5), value(6), value(7), value(8), value(9)))
    .toDF("campaign_id","ad_group_id","day","target_location_of_interest","campaign_location_target","target_physical_location","ad_id","keyword_id","device","top_vs_other")

  utils.saveDF(data)

}
