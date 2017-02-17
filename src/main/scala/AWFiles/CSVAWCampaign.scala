package AWFiles

import Utils.CommonUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object CSVAWCampaign extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("campaign"))
  val sqlCtx = SQLContext.getOrCreate(sc)

  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/AW_Cmpgn_20161107.csv")
  val header = rawData.first
  val header2 = "Campaign ID,Campaign,Day,Search Impr. share,Search Lost IS (rank),Search Lost IS (budget),Search Exact match IS,Labels,Clicks"
  val utils = new CommonUtils
  import sqlCtx.implicits._
  val data = rawData
    .filter(line => line != header2 && line != header)
    .map(_.split(","))
    .map(value => (value(0), value(1), value(2), value(3), value(4), value(5), value(6), value(7), value(8)))
    .toDF("campaign_id","campaign","day","search_impr_share","search_lost_is_rank","search_lost_is_budget","search_exact_match_is","labels","clicks")

  utils.saveDF(data)
}


