import org.apache.spark.sql.SparkSession

trait LoanedSparkSession {
  def withSpark(testMethod: SparkSession => Any): Unit = {

    // init a spark session before test method
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("spark test session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    try {
      // run test
      testMethod(spark)
    }
    finally spark.stop()
    // must stop spark as jvm supports only one spark session at a time
  }
}

/**
 * Sample Usage
 * -------------------------------------------------------------------
 * import org.scalatest.funsuite.AnyFunSuite
 * import org.scalatest.matchers.should._
 *
 * class LoanedSparkDataFrameSpec extends AnyFunSuite with Matchers with LoanedSparkSession {
 *
 *
 * test("compare rows and schema for methods that create a dataframe")(withSpark({ spark =>
 * import spark.implicits._
 *
 * // Arrange
 * val expectedRows = Set(
 * (1, "Saswata Dutta"),
 * (2, "John Smith"),
 * (3, "Jane Doe")
 * )
 * val expextedDf =
 * expectedRows.toSeq.toDF("id", "name")
 * val expectedColumns = SparkUtils.columns(expextedDf.schema)
 *
 * // Act
 * val actualDf =
 * Process.readCsv(spark, this.getClass.getResource("/test_data.csv").getPath)
 * val actualColumns = SparkUtils.columns(actualDf.schema)
 * val actualRows =
 * actualDf.collect().map(r => (r.getInt(0), r.getString(1))).toSet
 *
 * // Assert
 *
 * // be careful about ordering, so always sort or convert to set
 * actualColumns shouldEqual expectedColumns
 * actualRows shouldEqual expectedRows
 * }))
 * }
 */


