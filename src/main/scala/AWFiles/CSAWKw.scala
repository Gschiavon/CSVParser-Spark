package AWFiles

import Utils.CommonUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object CSAWKw extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("kw"))
  val sqlCtx = SQLContext.getOrCreate(sc)

  val utils = new CommonUtils

  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/AW_Kw_20161107.csv")
  val title = rawData.first
  val header = "Keyword ID,Keyword,Day,Match type,Ad group ID,Max. CPC,Impressions"

  import sqlCtx.implicits._
  val data = rawData
    .filter(line => line != title && line != header)
    .map(_.split(","))
    .map(values => (values(0), values(1), values(2), values(3), values(4), values(5), values(6)))
    .toDF("keyword_id", "keyword", "day", "match_type", "ad_group_id", "max_cpc", "impressions")

  utils.saveDF(data)

}
