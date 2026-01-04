package io.github.yumuuu.main

import io.github.yumuuu.core.sparkle.SparkleInstance.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}


private val sparkSession = SparkSession
  .builder()
  .appName("Spark-Exercise")
  .config("spark.master", "local")
  .getOrCreate()
private val path = "src/main/resources/quote_normalized.csv"

private val csvOptions = Map("sep" -> ",", "nullValue" -> "*****", "dateFormat" -> "yyyy/M/d")

private val colNames: Seq[String] =
  Seq("date",
    "USD", "GBP", "EUR", "CAD", "CHF", "SEK", "DKK", "NOK", "AUD", "NZD", "ZAR", "BHD",
    "IDR_100", "CNY", "HKD", "INR", "MYR", "PHP", "SGD", "KRW_100", "THB", "KWD", "SAR", "AED",
    "MXN", "PGK", "HUF", "CZK", "PLN", "TRY",
    "DUMMY", //トルコリラと台湾ドルの間に謎の無名の通貨が存在する
    "TWD", "CNY_REF", "KRW_100_REF", "IDR_100_REF", "MYR_REF", "XPF", "BRL", "VND_100", "EGP", "RUB"
  )

private val colTypes = Map(
  "date" -> to_date(col("date"), "yyyy/M/d")
)

@main def main(): Unit =
  val f = for {
    csvRow <- ReadCsv(path, csvOptions)
    namedColumn <- WithColumns(csvRow, colNames*)
    typedColumn <- WithColumnsType(namedColumn, colTypes)
    selected  <- Select(typedColumn)
    filtered <- Where(selected)
    _ <- WriteParquet(filtered, "target")
    _ <- CalculateArimaRegression(selected) // AICが出力できなかったり、ARIMAを自前実装しないといけなかったり諸々大変なのでsparkで動かすのは一旦諦める。。。
  } yield filtered
  val result = f.run(sparkSession)
//  result.show()