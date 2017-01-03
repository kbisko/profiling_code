name := "File Stats"

version := "1.0"

scalaVersion := "2.10.5"

// https://mvnrepository.com/artifact/org.scala-lang/scala-library
// https://mvnrepository.com/artifact/com.github.scala-incubator.io/scala-io-file_2.10.0-M6
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.3.0",
	"org.apache.spark" %% "spark-sql" % "1.3.0",
	"org.scala-lang" % "scala-library" % "2.10.5",
	"com.databricks" % "spark-csv_2.10" % "1.4.0",
	"com.github.scala-incubator.io" % "scala-io-file_2.10.0-M6" % "0.4.0",
        "javax.transaction" % "jta" % "1.1"
)



