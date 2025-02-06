package sparkprocessing

import org.apache.spark.sql.SparkSession

object SparkSessionSingleton {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Guarantees Fetcher")
    .getOrCreate()
}
