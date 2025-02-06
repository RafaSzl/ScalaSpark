package sparkprocessing.processors

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import sparkprocessing.data.DataLoader.guarantorSchema
import sparkprocessing.processors.GuaranteesProcessor.fixJsonFormat

object GuaranteesProcessor {
  def fixJsonFormat(columnName: String): Column = {
    regexp_replace(
      col(columnName),
      "(\\w+):\\s*([^\\[\\{,}\\]]+)",
      "\"$1\": \"$2\""
    )
  }
}

class GuaranteesProcessor(spark: SparkSession) {

  import spark.implicits._

  def getProcessedTransaction(
      transactionDf: DataFrame,
      exchangeRatesLatestDf: DataFrame,
      receiversRegistryLatestDf: DataFrame
  ): DataFrame = {
    transactionDf
      .filter($"STATUS" === "OPEN")
      .filter(col("DATE") >= lit(date_sub(current_date(), 30)))
      .distinct()
      .withColumn("GUARANTORS", fixJsonFormat("GUARANTORS"))
      .withColumn("GUARANTORS", from_json(col("GUARANTORS"), guarantorSchema))
      .withColumn("GUARANTOR", explode(col("GUARANTORS")))
      .select(
        col("GUARANTOR.NAME").alias("GUARANTOR_NAME"),
        col("GUARANTOR.PERCENTAGE").alias("GUARANTOR_PERCENTAGE"),
        col("RECEIVER_NAME"),
        col("TYPE"),
        col("COUNTRY"),
        col("CURRENCY"),
        col("AMOUNT"),
        col("PARTITION_DATE")
      )
      .withColumn("AMOUNT", col("AMOUNT").cast(DecimalType(10, 2)))
      .withColumn(
        "GUARANTOR_PERCENTAGE",
        ($"GUARANTOR_PERCENTAGE" * 100).cast(DecimalType(10, 2))
      )
      .withColumn(
        "GUARANTEED_AMOUNT",
        ($"AMOUNT" * $"GUARANTOR_PERCENTAGE" / 100).cast(DecimalType(10, 2))
      )
      .join(
        receiversRegistryLatestDf.select($"RECEIVER_NAME"),
        Seq("RECEIVER_NAME"),
        "inner"
      )
      .join(
        broadcast(exchangeRatesLatestDf.select($"FROM_CURRENCY", $"RATE")),
        transactionDf("CURRENCY") === exchangeRatesLatestDf("FROM_CURRENCY"),
        "left"
      )
      .withColumn("RATE", col("RATE").cast(DecimalType(10, 2)))
      .withColumn(
        "AMOUNT_EUR",
        ($"AMOUNT" * coalesce($"RATE", lit(1))).cast(DecimalType(10, 2))
      )
      .withColumn(
        "GUARANTEED_AMOUNT_EUR",
        ($"GUARANTEED_AMOUNT" * coalesce($"RATE", lit(1)))
          .cast(DecimalType(10, 2))
      )
  }

  def getAggregatedDF(df: DataFrame): DataFrame = {
    val decimalType = DecimalType(10, 2)

    val pivotedDF = df
      .groupBy("GUARANTOR_NAME", "COUNTRY", "PARTITION_DATE")
      .pivot("TYPE", Seq("CLASS_A", "CLASS_B", "CLASS_C", "CLASS_D"))
      .sum("GUARANTEED_AMOUNT")
      .na
      .fill(0)
      .select(
        col("GUARANTOR_NAME"),
        col("COUNTRY"),
        col("PARTITION_DATE"),
        col("CLASS_A").cast(decimalType),
        col("CLASS_B").cast(decimalType),
        col("CLASS_C").cast(decimalType),
        col("CLASS_D").cast(decimalType)
      )

    val uniqueGuarantors = df
      .select("COUNTRY", "GUARANTOR_NAME")
      .distinct()
      .groupBy("COUNTRY")
      .agg(count("GUARANTOR_NAME").alias("UNIQUE_GUARANTORS"))

    val classSums = df
      .groupBy("COUNTRY")
      .agg(
        sum(when(col("TYPE") === "CLASS_A", col("GUARANTEED_AMOUNT_EUR")))
          .cast(decimalType)
          .alias("SUM_CLASS_A"),
        sum(when(col("TYPE") === "CLASS_B", col("GUARANTEED_AMOUNT_EUR")))
          .cast(decimalType)
          .alias("SUM_CLASS_B"),
        sum(when(col("TYPE") === "CLASS_C", col("GUARANTEED_AMOUNT_EUR")))
          .cast(decimalType)
          .alias("SUM_CLASS_C"),
        sum(when(col("TYPE") === "CLASS_D", col("GUARANTEED_AMOUNT_EUR")))
          .cast(decimalType)
          .alias("SUM_CLASS_D")
      )

    val avgClasses = classSums
      .join(uniqueGuarantors, "COUNTRY")
      .withColumn(
        "AVG_CLASS_A",
        (col("SUM_CLASS_A") / col("UNIQUE_GUARANTORS")).cast(decimalType)
      )
      .withColumn(
        "AVG_CLASS_B",
        (col("SUM_CLASS_B") / col("UNIQUE_GUARANTORS")).cast(decimalType)
      )
      .withColumn(
        "AVG_CLASS_C",
        (col("SUM_CLASS_C") / col("UNIQUE_GUARANTORS")).cast(decimalType)
      )
      .withColumn(
        "AVG_CLASS_D",
        (col("SUM_CLASS_D") / col("UNIQUE_GUARANTORS")).cast(decimalType)
      )
      .drop(
        "SUM_CLASS_A",
        "SUM_CLASS_B",
        "SUM_CLASS_C",
        "SUM_CLASS_D",
        "UNIQUE_GUARANTORS"
      )

    val resultDF = pivotedDF
      .join(avgClasses, "COUNTRY", "left")
      .na
      .fill(0.00)
      .withColumn(
        "PARTITION_DATE",
        to_date(col("PARTITION_DATE"), "yyyy-MM-dd")
      )

    val finalColumns = Seq(
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

    resultDF.select(finalColumns.map(col): _*)
  }

}
