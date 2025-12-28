package io.github.yumuuu.core.sparkle

import org.apache.spark.sql.{DataFrame, SparkSession}

// idea: https://web.archive.org/web/20150911191943/https://spark-summit.org/2015-east/wp-content/uploads/2015/03/SSE15-29-Lance-Co-Ting-Keh.pdf
trait Sparkle[A] {
  //reader monad
  def run(env: SparkSession): A
  def flatMap[B](f: A => Sparkle[B]): Sparkle[B] =
    (env: SparkSession) => f(this.run(env)).run(env)
  def map[B](f: A => B):Sparkle[B] =
    (env: SparkSession) => f(this.run(env))
}

object Sparkle {
  def pure[A](a: A):Sparkle[A] = _ => a
  def ask: Sparkle[SparkSession] = env => env
}

object SparkleInstance {
  case class readCsvSparkle(path: String) extends Sparkle[DataFrame] {
    override def run(sc: SparkSession): DataFrame =
      sc.read.format("csv").csv(path)
  }


}

private object Example {
  import io.github.yumuuu.core.sparkle.SparkleInstance.readCsvSparkle
  val env = SparkSession
    .builder()
    .appName("Spark-Exercise")
    .config("spark.master", "local")
    .getOrCreate()
  val path: String = "cur.csv"

  val f = for {
    csv <- readCsvSparkle(path)
  } yield ()
  f.run(env)
}
