package com.dng.ebengine.utils

import org.apache.spark.sql.functions.udf

import scala.math.BigDecimal.RoundingMode

object FormatterUtils {

  val formatFloat = udf((value: Double) => {
    val bd = BigDecimal(value)
    val decimal = 2
    val formatted = bd.setScale(decimal, RoundingMode.HALF_EVEN)
    formatted.toFloat
  })
}
