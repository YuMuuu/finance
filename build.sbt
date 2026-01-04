import sbt.Keys.libraryDependencies

val scala3Version = "3.7.4"

lazy val root = project
  .in(file("."))
  .settings(
    name := "finance",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
  ).dependsOn(core)

lazy val core = project
  .in(file("core"))
  .settings(
    scalaVersion := scala3Version,
    libraryDependencies ++=
      Seq(
        "org.apache.spark" % "spark-core_2.13" % "4.1.0",
        "org.apache.spark" % "spark-sql_2.13" % "4.1.0",
        "org.apache.spark" % "spark-mllib_2.13" % "4.1.0",
        "org.scalanlp" % "breeze_2.13" % "2.1.0",
        "org.typelevel" % "cats-core_2.13" % "2.13.0"
      )
  )