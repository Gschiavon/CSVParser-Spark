import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.WordSpec
import com.databricks.spark.avro._
case class Person(name: String, surname: String, birthday: Int)
case class Person2(adress: String, telf: String)
class CSVReaderTest extends WordSpec {

  "test CSV reads" in {


    val sc = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("test"))
    val sqlCtx = SQLContext.getOrCreate(sc)


    val df =  sqlCtx.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/germanschiavonmatteo/Proyectos/Mexico/compass/Documentacion/DatioTestFiles/DIM_OAO_APPL.csv")

    val peopleDF = sqlCtx.createDataFrame(Seq(
      Person("Mario", "Delrio", 123456),
      Person("German", "Delatorre", 234567),
      Person("Sergio", "Garcia", 897654)))

    val people2DF = sqlCtx.createDataFrame(Seq(
      Person2("MarioCalle", "2314651"),
      Person2("GermanCalle", "dfgdfg" ),
      Person2("SergioCalle", "dsfdfdf")))

    val newdf = peopleDF.unionAll(people2DF)
    newdf.printSchema()
    newdf.show

//    peopleDF.printSchema()
//    peopleDF.show()
//    people2DF.printSchema()
//    people2DF.show()

    def saveDf(dataFrame: DataFrame): Unit = {
    dataFrame.write.format("com.databricks.spark.avro").mode(Append).save("hdfs://hdfs-master:9000/user/hadoop/data")
    }


//    df.show(10)
//    df.printSchema()
//    saveDf(df)
  }

}
