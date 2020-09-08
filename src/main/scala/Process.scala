import org.apache.spark.sql.functions.{col, concat, lit, udf}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Process {
  def readCsv(spark: SparkSession, dataPath: String): DataFrame =
    spark.read.
      options(Map("header" -> "true", "inferSchema" -> "true")).
      csv(dataPath)

  def greeting(col: Column): Column = concat(lit("Hello "), col)

  def addGreeting()(df: DataFrame): DataFrame =
    df.withColumn("greeting", greeting(col("name")))

  def addCustomColumn(colName: String, value: String)(df: DataFrame): DataFrame =
    df.withColumn(colName, lit(value))

  def snakeCase(string: String): String = string.toLowerCase.split("""\s+""").mkString("_")

  val snakeCaseUdf = udf[String, String](snakeCase)

  def snakeCaseName()(df: DataFrame): DataFrame =
    df.withColumn("snake_case_name", snakeCaseUdf(col("name")))

  def apply(df: DataFrame): DataFrame = df.
    transform(snakeCaseName()).
    transform(addGreeting()).
    transform(addCustomColumn("species", "human"))
}
