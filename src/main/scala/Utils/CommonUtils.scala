package Utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode.Append


class CommonUtils {

  def saveDF(dataFrame: DataFrame): Unit = {
    dataFrame
      .write
      .mode(Append)
      .format("com.databricks.spark.avro")
      .save("hdfs://hdfs-master:9000/user/csv/output")
  }

}
