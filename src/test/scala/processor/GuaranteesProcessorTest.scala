package processor

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import processor.GuaranteesProcessorTest._
import sparkprocessing.processors.GuaranteesProcessor
import sparkprocessing.utils.Utils.double2BD

import java.sql.Date

object GuaranteesProcessorTest {

  private val formattedDateMinus31 =
    Date.valueOf(java.time.LocalDate.now().minusDays(31)).toString
  private val formattedDateMinus7 =
    Date.valueOf(java.time.LocalDate.now().minusDays(7)).toString
  private val inputTransactions = Seq(
    (
      "T_001",
      """[{NAME: G_001, PERCENTAGE: 0.6}]""",
      "COMP_1",
      "COMP_2",
      "CLASS_A",
      "ES",
      "EUR",
      double2BD(1000.00),
      "OPEN",
      formattedDateMinus7,
      formattedDateMinus7
    ),
    (
      "T_002",
      """[{NAME: G_002, PERCENTAGE: 0.5}]""",
      "COMP_2",
      "COMP_3",
      "CLASS_B",
      "ES",
      "EUR",
      double2BD(2000.00),
      "OPEN",
      formattedDateMinus7,
      formattedDateMinus7
    ),
    (
      "T_003",
      """[{NAME: G_001, PERCENTAGE: 0.5}, {NAME: G_003, PERCENTAGE: 0.5}]""",
      "COMP_1",
      "COMP_4",
      "CLASS_C",
      "ES",
      "EUR",
      double2BD(3000),
      "CLOSED",
      formattedDateMinus7,
      formattedDateMinus7
    ),
    (
      "T_004",
      """[{NAME: G_005, PERCENTAGE: 0.5}]""",
      "COMP_5",
      "COMP_6",
      "CLASS_A",
      "US",
      "USD",
      double2BD(4000.00),
      "OPEN",
      formattedDateMinus7,
      formattedDateMinus7
    ),
    (
      "T_005",
      """[{NAME: G_005, PERCENTAGE: 0.6}]""",
      "COMP_5",
      "COMP_7",
      "CLASS_D",
      "US",
      "USD",
      double2BD(5000.00),
      "OPEN",
      formattedDateMinus7,
      formattedDateMinus7
    ),
    (
      "T_006",
      """[{NAME: G_007, PERCENTAGE: 0.5}]""",
      "COMP_5",
      "COMP_8",
      "CLASS_B",
      "US",
      "USD",
      double2BD(6000.00),
      "OPEN",
      formattedDateMinus7,
      formattedDateMinus7
    ),
    (
      "T_007",
      """[{NAME: G_006, PERCENTAGE: 0.2}, {NAME: G_007, PERCENTAGE: 0.2}]""",
      "COMP_6",
      "COMP_7",
      "CLASS_A",
      "US",
      "USD",
      double2BD(7000.00),
      "OPEN",
      formattedDateMinus7,
      formattedDateMinus7
    ),
    (
      "T_008",
      """[{NAME: G_001, PERCENTAGE: 0.5}, {NAME: G_004, PERCENTAGE: 0.5}]""",
      "COMP_3",
      "COMP_2",
      "CLASS_B",
      "ES",
      "EUR",
      double2BD(8000.00),
      "OPEN",
      formattedDateMinus7,
      formattedDateMinus7
    ),
    (
      "T_009",
      """[{NAME: G_002, PERCENTAGE: 0.5}]""",
      "COMP_4",
      "COMP_3",
      "CLASS_B",
      "ES",
      "EUR",
      double2BD(9000.00),
      "OPEN",
      formattedDateMinus31,
      formattedDateMinus31
    ),
    (
      "T_001",
      """[{NAME: G_001, PERCENTAGE: 0.6}]""",
      "COMP_1",
      "COMP_2",
      "CLASS_A",
      "ES",
      "EUR",
      double2BD(1000.00),
      "OPEN",
      formattedDateMinus7,
      formattedDateMinus7
    )
  )
  private val inputExchangeRates = Seq(
    ("EUR", "EUR", double2BD(1.00), "2024-01-20"),
    ("USD", "EUR", double2BD(0.90), "2024-01-20"),
    ("GBP", "EUR", double2BD(1.20), "2024-01-20")
  )
  private val inputReceiversRegistry = Seq(
    ("COMP_1", "ES", "2024-01-15"),
    ("COMP_2", "ES", "2024-01-15"),
    ("COMP_3", "ES", "2024-01-15"),
    ("COMP_4", "ES", "2024-01-15"),
    ("COMP_5", "US", "2024-01-15"),
    ("COMP_6", "US", "2024-01-15"),
    ("COMP_7", "US", "2024-01-15")
  )
  private val expectedGetProcessedTransactionSeq = Seq(
    ("COMP_2", "G_001", 60.00, "CLASS_A", "ES", "EUR", 1000.0, "2025-01-30", 600.0, "EUR", 1.0, 1000.0, 600.0),
    ("COMP_3", "G_002", 50.00, "CLASS_B", "ES", "EUR", 2000.0, "2025-01-30", 1000.0, "EUR", 1.0, 2000.0, 1000.0),
    ("COMP_6", "G_005", 50.00, "CLASS_A", "US", "USD", 4000.0, "2025-01-30", 2000.0, "USD", 0.9, 3600.0, 1800.0),
    ("COMP_7", "G_005", 60.00, "CLASS_D", "US", "USD", 5000.0, "2025-01-30", 3000.0, "USD", 0.9, 4500.0, 2700.0),
    ("COMP_7", "G_006", 20.00, "CLASS_A", "US", "USD", 7000.0, "2025-01-30", 1400.0, "USD", 0.9, 6300.0, 1260.0),
    ("COMP_7", "G_007", 20.00, "CLASS_A", "US", "USD", 7000.0, "2025-01-30", 1400.0, "USD", 0.9, 6300.0, 1260.0),
    ("COMP_2", "G_001", 50.00, "CLASS_B", "ES", "EUR", 8000.0, "2025-01-30", 4000.0, "EUR", 1.0, 8000.0, 4000.0),
    ("COMP_2", "G_004", 50.00, "CLASS_B", "ES", "EUR", 8000.0, "2025-01-30", 4000.0, "EUR", 1.0, 8000.0, 4000.0)
  )
  private val expectedGetAggregatedDFSeq = Seq(
    ("G_001", 600, 4000, 0, 0, 200, 3000, 0, 0, "ES", formattedDateMinus7),
    ("G_002", 0, 1000, 0, 0, 200, 3000, 0, 0, "ES", formattedDateMinus7),
    ("G_004", 0, 4000, 0, 0, 200, 3000, 0, 0, "ES", formattedDateMinus7),
    ("G_005", 2000, 0, 0, 3000, 1440, 0, 0, 900, "US", formattedDateMinus7),
    ("G_006", 1400, 0, 0, 0, 1440, 0, 0, 900, "US", formattedDateMinus7),
    ("G_007", 1400, 0, 0, 0, 1440, 0, 0, 900, "US", formattedDateMinus7)
  )
}

class GuaranteesProcessorTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("GuaranteesProcessorTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("getProcessedTransaction should correctly transform data") {

    val processor = new GuaranteesProcessor(spark)

    val inputTransactionsDf: DataFrame = spark
      .createDataFrame(inputTransactions)
      .toDF(
        "TRANSACTION_ID",
        "GUARANTORS",
        "SENDER_NAME",
        "RECEIVER_NAME",
        "TYPE",
        "COUNTRY",
        "CURRENCY",
        "AMOUNT",
        "STATUS",
        "DATE",
        "PARTITION_DATE"
      )
    inputTransactionsDf.show(false)
    val inputExchangeRatesDf = spark
      .createDataFrame(inputExchangeRates)
      .toDF("FROM_CURRENCY", "TO_CURRENCY", "RATE", "PARTITION_DATE")
    inputExchangeRatesDf.show(false)
    val inputReceiversRegistryDf = spark
      .createDataFrame(inputReceiversRegistry)
      .toDF("RECEIVER_NAME", "COUNTRY", "PARTITION_DATE")
    inputReceiversRegistryDf.show(false)

    val resultDf = processor.getProcessedTransaction(
      inputTransactionsDf,
      inputExchangeRatesDf,
      inputReceiversRegistryDf
    )

    resultDf.printSchema()
    resultDf.show(false)

    val expectedDf = expectedGetProcessedTransactionSeq
      .toDF(
        "RECEIVER_NAME",
        "GUARANTOR_NAME",
        "GUARANTOR_PERCENTAGE",
        "TYPE",
        "COUNTRY",
        "CURRENCY",
        "AMOUNT",
        "PARTITION_DATE",
        "GUARANTEED_AMOUNT",
        "FROM_CURRENCY",
        "RATE",
        "AMOUNT_EUR",
        "GUARANTEED_AMOUNT_EUR"
      )
      .withColumn(
        "GUARANTOR_PERCENTAGE",
        col("GUARANTOR_PERCENTAGE").cast("decimal(10,2)")
      )
      .withColumn("AMOUNT", col("AMOUNT").cast("decimal(10,2)"))
      .withColumn(
        "GUARANTEED_AMOUNT",
        col("GUARANTEED_AMOUNT").cast("decimal(10,2)")
      )
      .withColumn("RATE", col("RATE").cast("decimal(10,2)"))
      .withColumn("AMOUNT_EUR", col("AMOUNT_EUR").cast("decimal(10,2)"))
      .withColumn(
        "GUARANTEED_AMOUNT_EUR",
        col("GUARANTEED_AMOUNT_EUR").cast("decimal(10,2)")
      )

    expectedDf.printSchema()

    assert(resultDf.collect() === expectedDf.collect())
  }

  test("getAggregatedDF should compute sums and averages correctly") {
    val processedDf = expectedGetProcessedTransactionSeq.toDF(
      "RECEIVER_NAME",
      "GUARANTOR_NAME",
      "GUARANTOR_PERCENTAGE",
      "TYPE",
      "COUNTRY",
      "CURRENCY",
      "AMOUNT",
      "PARTITION_DATE",
      "GUARANTEED_AMOUNT",
      "FROM_CURRENCY",
      "RATE",
      "AMOUNT_EUR",
      "GUARANTEED_AMOUNT_EUR"
    )

    val processor = new GuaranteesProcessor(spark)
    val resultDf =
      processor.getAggregatedDF(processedDf).orderBy("GUARANTOR_NAME")
    resultDf.printSchema()
    val expectedDf = expectedGetAggregatedDFSeq
      .toDF(
        "GUARANTOR_NAME",
        "CLASS_A",
        "CLASS_B",
        "CLASS_C",
        "CLASS_D",
        "AVG_CLASS_A",
        "AVG_CLASS_B",
        "AVG_CLASS_C",
        "AVG_CLASS_D",
        "COUNTRY",
        "PARTITION_DATE"
      )
      .withColumn("CLASS_A", col("CLASS_A").cast("decimal(10,2)"))
      .withColumn("CLASS_B", col("CLASS_B").cast("decimal(10,2)"))
      .withColumn("CLASS_C", col("CLASS_C").cast("decimal(10,2)"))
      .withColumn("CLASS_D", col("CLASS_D").cast("decimal(10,2)"))
      .withColumn("AVG_CLASS_A", col("AVG_CLASS_A").cast("decimal(10,2)"))
      .withColumn("AVG_CLASS_B", col("AVG_CLASS_B").cast("decimal(10,2)"))
      .withColumn("AVG_CLASS_C", col("AVG_CLASS_C").cast("decimal(10,2)"))
      .withColumn("AVG_CLASS_D", col("AVG_CLASS_D").cast("decimal(10,2)"))
      .withColumn(
        "PARTITION_DATE",
        to_date(col("PARTITION_DATE"), "yyyy-MM-dd")
      )

    expectedDf.printSchema()
    resultDf.show(false)
    expectedDf.show(false)
    assert(resultDf.collect() === expectedDf.collect())
  }
}
