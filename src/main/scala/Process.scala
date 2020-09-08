import org.apache.spark.sql.{DataFrame, SparkSession}

object Process {
  def readCsv(spark: SparkSession, dataPath: String): DataFrame =
    spark.read.
      options(Map("header" -> "true", "inferSchema" -> "true")).
      csv(dataPath)

}
