import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

object SparkUtils {
  def initSpark(): SparkSession = {
    disableSparkLogs()

    SparkSession
      .builder()
      .master("local")
      .appName("spark session test")
      .config("spark.sql.shuffle.partitions", "1") // make tests faster by avoiding shuffles
      .getOrCreate()
  }

  def columns(schema: StructType): Set[(String, DataType)] = schema.map(f => (f.name, f.dataType)).toSet

  def disableSparkLogs(): Unit = {
    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }
}
