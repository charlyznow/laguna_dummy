import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MainApp extends App {
    final val READ_FILE_PATH = "gs://laguna-certification-associate/datalake/raw_data/Warehouse_and_Retail_Sales.csv"
    final val WRITE_FILE_PATH = "gs://laguna-certification-associate/datalake/raw_data/scala"

      val spark = SparkSession
        .builder()
        .appName("scala_test")
        .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        var df = spark.read
        .option("header", true)
        .option("delimirer", ',')
        .csv(READ_FILE_PATH)

        df.show(truncate = false, numRows = 20)
}