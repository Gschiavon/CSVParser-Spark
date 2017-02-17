import org.scalatest.WordSpec

class HealthyString extends WordSpec{

  "test" in {

    val inputStr = """"Gregorian date","Hour","Account name","Campaign name","Ad group","Ad title","Ad description","Ad distribution","Impressions","Clicks","CTR (%)","Average CPC","Spend","Avg. position","Conversions","Conversion rate (%)","CPA","Account number","Account status","Ad group ID","Ad group status","Ad ID","Ad status","Ad type","Final App URL","Bid match type","Business category","Business category ID","Business listing","Business listing ID","Campaign status","Currency code","Custom Parameters","Customer","Customer name","Delivered match type","Destination URL","Device OS","Device type","Display URL","Final URL","Language","Final Mobile URL","Network","Path 1","Path 2","Pricing method","Title part 1","Title part 2","Top vs. other","Tracking Template","Avg. CPM","Assists","Extended cost","Revenue","Return on ad spend (%)","Cost per assist","Revenue per conversion","Revenue per assist""""

    val output = inputStr.toLowerCase().replace(" ", "_")

    println(output)

 val input2 = """"gregorian_date","hour","account_name","campaign_name","ad_group","ad_title","ad_description","ad_distribution","impressions","clicks","ctr_(%)","average_cpc","spend","avg._position","conversions","conversion_rate_(%)","cpa","account_number","account_status","ad_group_id","ad_group_status","ad_id","ad_status","ad_type","final_app_url","bid_match_type","business_category","business_category_id","business_listing","business_listing_id","campaign_status","currency_code","custom_parameters","customer","customer_name","delivered_match_type","destination_url","device_os","device_type","display_url","final_url","language","final_mobile_url","network","path_1","path_2","pricing_method","title_part_1","title_part_2","top_vs._other","tracking_template","avg._cpm","assists","extended_cost","revenue","return_on_ad_spend_(%)","cost_per_assist","revenue_per_conversion","revenue_per_assist""""


  val output2 = input2.replace("\","," : String, ")

    println(output2)

    val output3= output2.replace("\"", "")
    println(output3)

  }

}
