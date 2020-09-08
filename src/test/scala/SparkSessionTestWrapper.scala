import SparkUtils.disableSparkLogs
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    disableSparkLogs()

    SparkSession
      .builder()
      .master("local")
      .appName("spark test session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
