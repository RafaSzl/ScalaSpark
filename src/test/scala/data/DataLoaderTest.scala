package data

import data.DataLoaderTest._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import sparkprocessing.data.DataLoader

import java.math.{RoundingMode, BigDecimal => JBigDecimal}
import java.sql.Date

object DataLoaderTest {
  private val date20240120 = Date.valueOf("2024-01-20")
  private val bigD09 = new JBigDecimal(0.9).setScale(2, RoundingMode.HALF_UP)
  private val bigD12 = new JBigDecimal(1.2).setScale(2, RoundingMode.HALF_UP)

  private val seqForPartitionCheck = Seq(
    ("column", "2024-01-20"),
    ("column", "2024-01-19"),
    ("column", "2024-01-18")
  )

  private val exchangeRatesData = Seq(
    ("USD", "EUR", BigDecimal(0.9), "20.01.2024"),
    ("GBP", "EUR", BigDecimal(1.2), "20.01.2024"),
    ("USD", "EUR", BigDecimal(0.95), "01.01.2024"),
    ("GBP", "GBP", BigDecimal(1), "20.01.2024"),
    ("GBP", "EUR", BigDecimal(1.25), "01.01.2024")
  )

  private val receiversData = Seq(
    ("Receiver1", "Country1", "20.01.2024"),
    ("Receiver2", "Country2", "20.01.2024"),
    ("Receiver3", "Country3", "1.01.2024"),
    ("Receiver4", "Country4", "01.01.2024")
  )

}

class DataLoaderTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("DataLoaderTest")
    .getOrCreate()
  val dataLoader = new DataLoader(spark)

  test("getLatestPartition should return rows with the latest partition date") {
    val testDf: DataFrame = spark
      .createDataFrame(seqForPartitionCheck)
      .toDF("COLUMN1", "PARTITION_DATE")
      .withColumn("PARTITION_DATE", to_date(col("PARTITION_DATE")))

    val resultDf = dataLoader.getLatestPartition(
      testDf,
      "PARTITION_DATE",
      Window.orderBy(col("PARTITION_DATE").desc)
    )
    resultDf.printSchema()
    resultDf.show(false)

    assert(
      resultDf.collect() === Array(
        Row("column", date20240120)
      )
    )
  }

  test(
    "getLatestExchangeRates should return rows with the latest partition date"
  ) {
    val testDf: DataFrame = spark
      .createDataFrame(exchangeRatesData)
      .toDF("FROM_CURRENCY", "TO_CURRENCY", "RATE", "PARTITION_DATE")
      .withColumn("RATE", col("RATE").cast(DecimalType(10, 2)))
    val dataLoaderMock = new DataLoader(spark) {
      override def readCsv(filePath: String, schema: StructType): DataFrame =
        testDf
    }
    val resultDf = dataLoaderMock.getLatestExchangeRates("dummyPath")

    resultDf.printSchema()
    resultDf.show(false)

    assert(
      resultDf.collect() === Array(
        Row("USD", "EUR", bigD09, date20240120),
        Row("GBP", "EUR", bigD12, date20240120)
      )
    )
  }

  test(
    "getLatestReceiversRegistry should return rows with the latest partition date"
  ) {
    val testDf: DataFrame = spark
      .createDataFrame(receiversData)
      .toDF("RECEIVER_NAME", "COUNTRY", "PARTITION_DATE")
    val dataLoaderMock = new DataLoader(spark) {
      override def readCsv(filePath: String, schema: StructType): DataFrame =
        testDf
    }
    val resultDf = dataLoaderMock.getLatestReceiversRegistry("dummyPath")

    resultDf.printSchema()
    resultDf.show(false)

    assert(
      resultDf.collect() === Array(
        Row("Receiver1", "Country1", date20240120),
        Row("Receiver2", "Country2", date20240120)
      )
    )
  }

  test("check schema got from getTransactions method") {
    val transactionsDf =
      dataLoader.getTransactions("src/test/scala/resources/transactions.csv")
    transactionsDf.printSchema()
    transactionsDf.show(false)
  }

}
