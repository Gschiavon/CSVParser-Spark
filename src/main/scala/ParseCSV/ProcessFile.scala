package ParseCSV

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{DataFrame, SQLContext}


case class ProcessFile(sqlCtx: SQLContext)(implicit conf: Config)  {

  def readFile(filePath: String): DataFrame = {
    sqlCtx.read
      .format(conf.getString("formats.csvFormat"))
      .option("header", "true")
      .option("delimiter", ";")
      .load(filePath)
  }
  def saveDf(dataFrame: DataFrame): Unit = {
    dataFrame.write.format(conf.getString("formats.avroFormat"))
      .mode(Append)
      .save(conf.getString("hdfs.path"))
  }

}
