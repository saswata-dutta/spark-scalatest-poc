import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class DataFrameSpec extends AnyFunSuite with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  test("compare rows and schema for methods that create a dataframe") {

    // Arrange
    val expectedRows = Set(
      (1, "Saswata Dutta"),
      (2, "John Smith"),
      (3, "Jane Doe")
    )
    val expextedDf =
      expectedRows.toSeq.toDF("id", "name")
    val expectedColumns = SparkUtils.columns(expextedDf.schema)

    // Act
    val actualDf =
      Process.readCsv(spark, getClass.getResource("/test_data.csv").getPath)
    val actualColumns = SparkUtils.columns(actualDf.schema)
    val actualRows =
      actualDf.collect().map(r => (r.getInt(0), r.getString(1))).toSet

    // Assert
    actualColumns shouldEqual expectedColumns
    actualRows shouldEqual expectedRows
  }
}
