package AWFiles

import Utils.CommonUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object CSVAWCriteria extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("criteria"))
  val sqlCtx = SQLContext.getOrCreate(sc)

  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/AW_Criteria_20161107.csv")
  val title = rawData.first
  val header = "Campaign ID,Campaign,Ad group ID,Ad group,Day,Criteria Type,Keyword / Placement,Quality score,Impressions"
  val utils = new CommonUtils

  import sqlCtx.implicits._

  val data = rawData
    .filter(line => line != title && line != header)
    .map(_.split(","))
    .map(values => (values(0), values(1).trim, values(2), values(3), values(4), values(5), values(6), values(7), values(8)))
    .toDF("campaign_id".trim, "campaign", "ad_group_id", "ad_group", "day", "criteria_type", "keyword_placement", "quality_score", "impressions")

  utils.saveDF(data)

}
