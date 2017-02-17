package BingFiles

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object CSVBingAds extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("bing-ads"))
  val sqlCtx = SQLContext.getOrCreate(sc)
  import sqlCtx.implicits._

  val rawData = sc.textFile("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/BingAds012517.csv")
  val removeData1 = """﻿"Report Name: Ad 1/18/2017 Compass Bank""""
  val removeData2 = """"Report Time: 1/24/2017""""
  val removeData3 = """"Time Zone: Various""""
  val removeData4 = """"Last Completed Available Day: 1/25/2017 12:50:00 PM (GMT)""""
  val removeData5 = """"Last Completed Available Hour: 1/25/2017 12:50:00 PM (GMT)""""
  val removeData6 = """"Report Aggregation: Hour""""
  val removeData7 = """"Report Filter: """"
  val removeData8 = """"Rows: 14246""""
  val removeData9 = """"Gregorian date","Hour","Account name","Campaign name","Ad group","Ad title","Ad description","Ad distribution","Impressions","Clicks","CTR (%)","Average CPC","Spend","Avg. position","Conversions","Conversion rate (%)","CPA","Account number","Account status","Ad group ID","Ad group status","Ad ID","Ad status","Ad type","Final App URL","Bid match type","Business category","Business category ID","Business listing","Business listing ID","Campaign status","Currency code","Custom Parameters","Customer","Customer name","Delivered match type","Destination URL","Device OS","Device type","Display URL","Final URL","Language","Final Mobile URL","Network","Path 1","Path 2","Pricing method","Title part 1","Title part 2","Top vs. other","Tracking Template","Avg. CPM","Assists","Extended cost","Revenue","Return on ad spend (%)","Cost per assist","Revenue per conversion","Revenue per assist""""
  val removeData10 = ""


  val data = rawData
    .filter(line =>
         line != removeData1
      && line != removeData2
      && line != removeData3
      && line != removeData4
      && line != removeData5
      && line != removeData6
      && line != removeData7
      && line != removeData8
      && line != removeData9
      && line != removeData10)
    .map(_.split(","))
//    .map(values =>
//      (Chunk1(values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18), values(19)), Chunk2(values(20), values(21), values(22), values(23))))//, values(24), values(25), values(26), values(26), values(27), values(28), values(29), values(30), values(31), values(32), values(33),values(34), values(35)), Chunk3(values(36), values(37), values(38), values(39), values(40), values(41), values(42), values(43),values(44), values(45), values(46), values(47), values(48), values(49), values(50), values(51), values(52), values(53), values(54), values(55)), Chunk4(values(56), values(57))))
//      .map(chunks => (chunks._1.gregorian_date, chunks._1.hour, chunks._1.account_name, chunks._1.campaign_name, chunks._1.ad_group, chunks._1.ad_title, chunks._1.ad_description, chunks._1.ad_distribution, chunks._1.impressions,chunks._1.clicks, chunks._1.ctr, chunks._1.average_cpc, chunks._1.spend, chunks._1.avg_position, chunks._1.conversions, chunks._1.conversion_rate, chunks._1.cpa, chunks._1.account_number, chunks._1.account_status, chunks._1.ad_group_id, chunks._2.ad_group_status,chunks._2.ad_id, chunks._2.ad_status, chunks._2.ad_type))
//    .toDF("gregorian_date","hour","account_name","campaign_name","ad_group","ad_title","ad_description","ad_distribution","impressions","clicks","ctr_(%)","average_cpc","spend","avg._position","conversions","conversion_rate_(%)","cpa","account_number","account_status","ad_group_id","ad_group_status","ad_id","ad_status","ad_type")//,"final_app_url","bid_match_type","business_category","business_category_id","business_listing","business_listing_id","campaign_status","currency_code","custom_parameters","customer","customer_name","delivered_match_type","destination_url","device_os","device_type","display_url","final_url","language","final_mobile_url","network","path_1","path_2","pricing_method","title_part_1","title_part_2","top_vs._other","tracking_template","avg._cpm","assists","extended_cost","revenue","return_on_ad_spend_(%)","cost_per_assist","revenue_per_conversion","revenue_per_assist")






}
case class Message(chunk1: Chunk1)//, chunk2: Chunk2, chunk3: Chunk3, chunk4: Chunk4)
case class Chunk1 (gregorian_date : String, hour : String, account_name : String, campaign_name : String, ad_group : String, ad_title : String, ad_description : String, ad_distribution : String, impressions : String, clicks : String, ctr : String, average_cpc : String, spend : String, avg_position : String, conversions : String, conversion_rate : String, cpa : String, account_number : String, account_status : String, ad_group_id : String)
case class Chunk2 (ad_group_status : String, ad_id : String, ad_status : String, ad_type : String)//, final_app_url : String, bid_match_type : String, business_category : String, business_category_id : String, business_listing : String, business_listing_id : String, campaign_status : String, currency_code : String, custom_parameters : String, customer : String, customer_name : String, delivered_match_type : String, destination_url : String)
case class Chunk3 (device_os : String, device_type : String, display_url : String, final_url : String, language : String, final_mobile_url : String, network : String, path_1 : String, path_2 : String, pricing_method : String, title_part_1 : String, title_part_2 : String, top_vs_other : String, tracking_template : String, avg_cpm : String, assists : String, extended_cost : String, revenue : String, return_on_ad_spend : String, cost_per_assist : String)
case class Chunk4 (revenue_per_conversion : String, revenue_per_assist: String)
