package AWFiles

import Utils.CommonUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object CSVAWGeo extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("geo"))
  val sqlCtx = SQLContext.getOrCreate(sc)

  val utils = new CommonUtils

  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/AW_Geo_20161107.csv")
  val title = rawData.first
  val header = "Campaign ID,Campaign,Ad group ID,Ad group,Day,Location type,Metro area,Country/Territory,Region,Is Targetable,Impressions"

  import sqlCtx.implicits._

  val data = rawData
    .filter(line => line != title && line != header)
    .map(_.split(","))
    .map(values => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10)))
    .toDF("campaign_id", "campaign", "ad_group_id", "ad_group", "day", "location_type", "metro_area", "country_territory", "region", "is_targetable", "impressions")

  utils.saveDF(data)

}
