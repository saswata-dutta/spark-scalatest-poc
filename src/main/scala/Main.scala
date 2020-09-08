import Process._
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val dataPath = getClass.getResource("/data.csv").getPath

    // separate data ingestion and data processing so they can be easily tested
    val df = readCsv(spark, dataPath)

    // separate each logical processing step so they can be chained and individually tested
    val processed = Process(df)

    processed.show(truncate = false)
  }

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }
}
