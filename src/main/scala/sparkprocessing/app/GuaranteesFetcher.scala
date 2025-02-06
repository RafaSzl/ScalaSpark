package sparkprocessing.app

import com.typesafe.config.ConfigFactory
import sparkprocessing.SparkSessionSingleton
import sparkprocessing.data.DataLoader
import sparkprocessing.processors.GuaranteesProcessor

object GuaranteesFetcher {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load()
    val spark = SparkSessionSingleton.spark

    val pathToExchangeRatesRegistry =
      config.getString("spark.app.pathToExchangeRatesRegistry")
    val pathToReceiversRegistry =
      config.getString("spark.app.pathToReceiversRegistry")
    val pathToTransactions = config.getString("spark.app.pathToTransactions")
    val outputPath = config.getString("spark.app.pathToOutput")

    val processor = new GuaranteesProcessor(spark)
    val dataloader = new DataLoader(spark)

    val processedDf =
      processor
        .getProcessedTransaction(
          dataloader.getTransactions(pathToTransactions),
          dataloader.getLatestExchangeRates(pathToExchangeRatesRegistry),
          dataloader.getLatestReceiversRegistry(pathToReceiversRegistry)
        )
    val aggregatedDf = processor.getAggregatedDF(processedDf)

    aggregatedDf.write
      .mode("overwrite")
      .partitionBy("PARTITION_DATE")
      .csv(outputPath)

    spark.stop()
  }
}
