package sparkprocessing.data

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkprocessing.data.DataLoader.{
  exchangeRatesSchema,
  receiversSchema,
  transactionsSchema
}
import sparkprocessing.utils.Utils.parseDateUDF

object DataLoader {
  val exchangeRatesSchema: StructType = StructType(
    Seq(
      StructField("FROM_CURRENCY", StringType, nullable = false),
      StructField("TO_CURRENCY", StringType, nullable = false),
      StructField("RATE", DecimalType(5, 2), nullable = false),
      StructField("PARTITION_DATE", StringType, nullable = false)
    )
  )

  val receiversSchema: StructType = StructType(
    Seq(
      StructField("RECEIVER_NAME", StringType, nullable = false),
      StructField("COUNTRY", StringType, nullable = false),
      StructField("PARTITION_DATE", StringType, nullable = false)
    )
  )

  val transactionsSchema: StructType = StructType(
    Seq(
      StructField("TRANSACTION_ID", StringType, nullable = false),
      StructField("GUARANTORS", StringType, nullable = false),
      StructField("SENDER_NAME", StringType, nullable = false),
      StructField("RECEIVER_NAME", StringType, nullable = false),
      StructField("TYPE", StringType, nullable = false),
      StructField("COUNTRY", StringType, nullable = false),
      StructField("CURRENCY", StringType, nullable = false),
      StructField("AMOUNT", DecimalType(13, 2), nullable = false),
      StructField("STATUS", StringType, nullable = false),
      StructField("DATE", StringType, nullable = false),
      StructField("PARTITION_DATE", StringType, nullable = false)
    )
  )

  val guarantorSchema: ArrayType = ArrayType(
    StructType(
      Seq(
        StructField("NAME", StringType, nullable = false),
        StructField("PERCENTAGE", DecimalType(4, 2), nullable = false)
      )
    )
  )

}

class DataLoader(spark: SparkSession) {

  import spark.implicits._

  val windowSpec: WindowSpec = Window.orderBy(col("PARTITION_DATE").desc)

  def getLatestPartition(
      df: DataFrame,
      partitionColumn: String,
      windowSpecFunc: WindowSpec
  ): DataFrame =
    df.withColumn(
      "LATEST_PARTITION_DATE",
      first(partitionColumn).over(windowSpecFunc)
    ).filter(col(partitionColumn) === col("LATEST_PARTITION_DATE"))
      .drop("LATEST_PARTITION_DATE")

  def readCsv(filePath: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .csv(filePath)
  }

  def getLatestExchangeRates(filePath: String): DataFrame = {
    getLatestPartition(
      readCsv(filePath, exchangeRatesSchema),
      "PARTITION_DATE",
      windowSpec
    )
      .filter($"TO_CURRENCY" === "EUR")
      .withColumn("PARTITION_DATE", parseDateUDF(col("PARTITION_DATE")))
  }

  def getLatestReceiversRegistry(filePath: String): DataFrame = {
    getLatestPartition(
      readCsv(filePath, receiversSchema),
      "PARTITION_DATE",
      windowSpec
    )
      .withColumn("PARTITION_DATE", parseDateUDF(col("PARTITION_DATE")))
  }

  def getTransactions(filePath: String): DataFrame = {
    readCsv(filePath, transactionsSchema)
      .withColumn("DATE", parseDateUDF(col("DATE")))
      .withColumn("PARTITION_DATE", parseDateUDF(col("PARTITION_DATE")))
  }

}
