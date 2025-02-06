package sparkprocessing.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import java.math.{RoundingMode, BigDecimal => JBigDecimal}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

object Utils {

  val parseDateUDF: UserDefinedFunction = udf((dateStr: String) => {
    val formats = Seq("dd.MM.yyyy", "d.MM.yyyy")
    formats.view
      .flatMap { format =>
        Try(
          LocalDate.parse(dateStr, DateTimeFormatter.ofPattern(format))
        ).toOption
      }
      .headOption
      .orNull
  })

  def double2BD(value: Double, scale: Int = 2): BigDecimal = {
    BigDecimal(new JBigDecimal(value).setScale(scale, RoundingMode.HALF_UP))
  }

}
