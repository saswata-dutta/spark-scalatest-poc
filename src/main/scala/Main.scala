import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val dataPath = getClass.getResource("/data.csv").getPath
    val df = Process.readCsv(spark, dataPath)
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
