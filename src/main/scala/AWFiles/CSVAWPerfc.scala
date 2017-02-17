package AWFiles

import Utils.CommonUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object CSVAWPerfc extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("perfc"))
  val sqlCtx = SQLContext.getOrCreate(sc)

  val utils = new CommonUtils

  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/AW_Perfc_20161130.csv")
  val title = rawData.first
  val header = "Campaign ID,Campaign,Ad group ID,Ad group,Ad ID,Ad type,Day,Clicks,Impressions,Cost,Description line 1,Description line 2,Device,Display URL,Ad,Image Ad URL,Image ad name,Keyword ID,Top vs. Other"

  import sqlCtx.implicits._
  val data = rawData
    .filter(line => line != title && line != header)
    .map(_.split(","))
    .map(values => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18)))
    .toDF("campaign_id","campaign","ad_group_id","ad_group","ad_id","ad_type","day","clicks","impressions","cost","description_line_1","description_line_2","device","display_url","ad","image_ad_url","image_ad_name","keyword_id","top_vs_other")

  utils.saveDF(data)
}

