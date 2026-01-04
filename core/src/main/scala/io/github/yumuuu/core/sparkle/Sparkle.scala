package io.github.yumuuu.core.sparkle

import cats.data.Reader
import io.github.yumuuu.core.spark.regression.ArimaRegression
import org.apache.spark.sql.functions.{col, expr, lit, to_date}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

// idea: https://web.archive.org/web/20150911191943/https://spark-summit.org/2015-east/wp-content/uploads/2015/03/SSE15-29-Lance-Co-Ting-Keh.pdf
trait Sparkle[A] {
  def run(sc: SparkSession): A
  def flatMap[B](f: A => Sparkle[B]): Sparkle[B] =
    (sc: SparkSession) => f(this.run(sc)).run(sc)
  def map[B](f: A => B):Sparkle[B] =
    (sc: SparkSession) => f(this.run(sc))
}

trait Sparkle2[A] {
  def run(sc: SparkSession): A

  def flatMap[B](f: A => Sparkle[B]): Sparkle[B] =
    (sc: SparkSession) => f(this.run(sc)).run(sc)

  def map[B](f: A => B): Sparkle[B] =
    (sc: SparkSession) => f(this.run(sc))
}


object Sparkle {
  def pure[A](a: A):Sparkle[A] = _ => a
  def ask: Sparkle[SparkSession] = sc => sc

  given [A]: Conversion[Sparkle[A], Reader[SparkSession, A]] with
    def apply(sa: Sparkle[A]): Reader[SparkSession, A] = Reader(sa.run)
}

object SparkleInstance {
  final case class ReadCsv(path: String, options: Map[String, String] = Map.empty) extends Sparkle[DataFrame] {
    override def run(sc: SparkSession): DataFrame =
      sc.read.options(options).csv(path)
  }

  final case class WithColumns(df: DataFrame, colNames: String*) extends Sparkle[DataFrame] {
    override def run(sc: SparkSession): DataFrame =
      df.toDF(colNames *).withColumns(Map("date" -> to_date(col("date"), "yyyy/M/d")))
  }

  final case class WithColumnsType(df: DataFrame,  withColumns: Map[String, Column] = Map.empty) extends Sparkle[DataFrame] {
    override def run(sc: SparkSession): DataFrame =
      df.withColumns(withColumns)
  }

  final case class Select(df: DataFrame) extends Sparkle[DataFrame] {
    override def run(sc: SparkSession): DataFrame =
      df.select(expr("date"), expr("USD"))
  }

  final case class Where(df: DataFrame) extends Sparkle[DataFrame] {
    override def run(sc: SparkSession): DataFrame =
      df.filter(col("date").between(lit("2008-1-1"), lit("2011-12-31")))
  }

  final case class WriteParquet(df: DataFrame, path: String) extends Sparkle[Unit] {
    override def run(sc: SparkSession): Unit =
      df.write.mode(SaveMode.Overwrite).parquet(path)
  }

  final case class CalculateArimaRegression(df: DataFrame) extends Sparkle[Unit] {
    override def run(sc: SparkSession): Unit = {
      // https://github.com/apache/spark/blob/5fabccec44a02f6dd7fc2dd73353c3d73770687f/examples/src/main/scala/org/apache/spark/examples/ml/ArimaRegressionExample.scala

      val tsData =  df.select(expr("USD").cast("double").alias("y"))
      val arima = new ArimaRegression().setP(1).setD(0).setQ(0) //日付ごとの差は等差とするのでd=0で良い。qは0しか対応していないので0にする
      val model = arima.fit(tsData)
      val result = model.transform(tsData)
      result.show()


      /**
       * +------+------------------+
       * |     y|        prediction|
       * +------+------------------+
       * |133.15|0.1481759542783809|
       * | 133.2|  133.122059596793|
       * | 133.2| 133.1719934622239|
       * | 133.1| 133.1719934622239|
       * | 132.3|133.07212573136206|
       * |131.55|132.27318388446753|
       * |131.55|131.52417590300388|
       * | 130.7|131.52417590300388|
       * | 131.1|130.67530019067837|
       * |131.55|131.07477111412567|
       * | 132.2|131.52417590300388|
       * |131.85|132.17331615360567|
       * |130.95|131.82377909558932|
       * | 130.6|130.92496951783292|
       * | 130.1|130.57543245981654|
       * | 130.3|130.07609380550744|
       * | 129.9| 130.2758292672311|
       * |130.25| 129.8763583437838|
       * |129.45| 130.2258954018002|
       * |128.75| 129.4269535549056|
       * +------+------------------+ 
       */
    }
  }
}
