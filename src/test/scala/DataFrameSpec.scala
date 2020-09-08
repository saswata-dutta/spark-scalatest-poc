import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
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

    // be careful about ordering, so always sort or convert to set
    actualColumns shouldEqual expectedColumns
    actualRows shouldEqual expectedRows
  }

  test("compare columns for methods that create/update a dataframe column") {
    // Arrange
    val expectedColumn = "test-column"
    val expectedValue = "TEST VALUE"

    // Act
    val seedDf =
      Process.readCsv(spark, getClass.getResource("/test_data.csv").getPath)
    val inputRowCount = seedDf.count().toInt
    val inputColCount = seedDf.schema.length

    val actualDf = seedDf.transform(Process.addCustomColumn(expectedColumn, expectedValue))
    val actualColumns = SparkUtils.columns(actualDf.schema)
    val actualValues =
      actualDf.collect().map(r => r.getString(inputColCount))

    // Assert
    actualColumns should contain(expectedColumn -> StringType)
    actualValues shouldEqual Array.fill(inputRowCount)(expectedValue)
  }

  test("compare column functions by evaluating on a literal") {
    // Arrange
    val input = "World !!"
    val expectedValue = s"Hello $input"

    // Act
    val actualColumn = Process.greeting(lit(input))
    val actualValue = actualColumn.expr.eval().toString

    // Assert
    actualValue shouldEqual expectedValue
  }

  test("compare UDF by evaluating on a literal") {
    // Arrange
    val input = "Hello Beautiful World"
    val expectedValue = "hello_beautiful_world"

    // Act
    val actualColumn = Process.snakeCaseUdf(lit(input))
    val actualValue = actualColumn.expr.eval().toString

    // Assert
    actualValue shouldEqual expectedValue
  }

  test("better to compare scala function used to create UDF") {
    // Arrange
    val input = "Hello Beautiful World"
    val expectedValue = "hello_beautiful_world"

    // Act
    val actualValue = Process.snakeCase(input)

    // Assert
    actualValue shouldEqual expectedValue
  }
}
