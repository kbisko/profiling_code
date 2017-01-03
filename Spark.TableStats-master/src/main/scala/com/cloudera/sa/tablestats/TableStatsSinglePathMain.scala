package com.cloudera.sa.tablestats

import com.cloudera.sa.tablestats.model.{FirstPassStatsModel}
import org.apache.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.databricks.spark.csv._

import scala.collection.mutable

//spark-submit --class com.cloudera.sa.tablestats.TableStatsSinglePathMain --master yarn --deploy-mode client --executor-memory 6G --num-executors 1000 file-stats_2.10-1.0.jar /refined/analytics/Amerigroup/Denorms/Claims_Denorm1_Par/OP

/**
 * Created by ted.malaska on 6/27/15.
 */
object TableStatsSinglePathMain {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("TableStatsSinglePathMain <inputPath>")
      return
    }

    val inputPath = args(0)
	val formatPath = args(1)
	val tmpOutput = args(2)
	val tmpOutput2 = args(3)
	val evalPath = args(4)
	val distPath = args(5)
    val runLocal = (args.length == 2 && args(1).equals("L"))
    var sc:SparkContext = null

    if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      sc = new SparkContext("local", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain").set("spark.driver.maxResultSize", "3g")
      sc = new SparkContext(sparkConfig)
    }
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	import sqlContext.implicits._
	
	val hadoopConf = new org.apache.hadoop.conf.Configuration()
	val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://norwalk-hadoop"), hadoopConf)
	
    //Part A
    //val df = sqlContext.parquetFile(inputPath)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(tmpOutput), true) } catch { case _ : Throwable => { } }
	val file = sc.textFile(inputPath).map(x => x.replace(","," ")).map(fields => fields.replace("\u0001", ",")).coalesce(999, shuffle=true).saveAsTextFile(tmpOutput)
	val df = sqlContext.load("com.databricks.spark.csv",Map("path" -> tmpOutput, "inferSchema" -> "true"))
	//Part B
    val firstPassStats = getFirstPassStat( df)
    //Part E 
    //println(firstPassStats)
	
	//Part 1 (KDB) -- format the file & save 
	val output = Array(firstPassStats.toString())
	val raw_eval = sc.parallelize(output,1).coalesce(1, shuffle=true)
	val replace_newline = raw_eval.map(x => x.replace(")), ","))\n")).map(x => x.replace(" -> ",";")).map(x => x.replaceAll("(-9223372036854775808|9223372036854775807)","NA")).map(x => x.replace("""Map(""","")).map(x => x.replace("ArrayBuffer","")).map(x => x.replaceAll("""\)\)\)""","""\)""")).map(x => x.replaceAll("""\)\)""","""\)""")).map(x => x.replaceAll("""\(\(""","""\(""")).map(x => x.replaceAll("""\(""","")).map(x => x.replaceAll("""\)""",""))			
	try { hdfs.delete(new org.apache.hadoop.fs.Path(tmpOutput2), true) } catch { case _ : Throwable => { } }
	replace_newline.saveAsTextFile(tmpOutput2)
	
	// Part 2 (KDB) -- load & create DFs
	val eval = sc.textFile(tmpOutput2).map(fields => fields.split(";"))
	// Part 2.1 (KDB) -- create a DF with just the eval values
	val refined_eval = eval.map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8)))
	val evalSchemaString = "colNum nulls empties totalCount uniqueValues maxLong minLong sumLong avgLong"
	val eval_schema = StructType(evalSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
	val eval_df = sqlContext.createDataFrame(refined_eval, eval_schema)
	// Part 2.2 (KDB) -- flatten the distributions 
	val dist = eval.map(p=>(p(0).asInstanceOf[String],p(9).split(""", """).asInstanceOf[Array[String]]))
	val raw_dist = dist.flatMap {case (key, value) => for ((v) <- value) yield (key, v)}
	val dist_df = raw_dist.toDF()
	
	//Part 3 (KDB) -- combine eval columns with layout names
	var file_layout = sc.textFile(formatPath)
	val file_output = file_layout.map(_.split('|')).map(p => Row(p(0).trim, p(1).trim))
	val fileSchemaString = "fileColNum colName"
	val file_schema = StructType(fileSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
	val file_df = sqlContext.createDataFrame(file_output, file_schema)
	
	val combined_eval = eval_df.join(file_df, eval_df("colNum") === file_df("fileColNum")).select("colNum", "colName", "nulls", "empties", "totalCount", "uniqueValues", "maxLong", "minLong", "sumLong", "avgLong")
	val combined_dist = dist_df.join(file_df, dist_df("_1") === file_df("fileColNum")).select("_1", "colName", "_2").withColumnRenamed("_1","colNum").withColumnRenamed("_2","Entry_Count")
	//val rdd_dist = combined_dist.rdd
	//combined_eval.show()
	//combined_dist.show()
	//Part F - save & stop
	try { hdfs.delete(new org.apache.hadoop.fs.Path(evalPath), true) } catch { case _ : Throwable => { } }
    combined_eval.repartition(1).save(evalPath)  
	try { hdfs.delete(new org.apache.hadoop.fs.Path(distPath), true) } catch { case _ : Throwable => { } }
	combined_dist.repartition(1).save(distPath)  
	sc.stop()
  }

  def getFirstPassStat(df: DataFrame): FirstPassStatsModel = {
    val schema = df.schema

    //Part B.1
    val columnValueCounts = df.flatMap(r =>
      (0 until schema.length).map { idx =>
        //((columnIdx, cellValue), count)
        ((idx, r.get(idx)), 1l)
      }
    ).reduceByKey(_ + _) //This is like word count

    //Part C
    val firstPassStats = columnValueCounts.mapPartitions[FirstPassStatsModel]{it =>
      val firstPassStatsModel = new FirstPassStatsModel()
      it.foreach{ case ((columnIdx, columnVal), count) =>
        firstPassStatsModel += (columnIdx, columnVal, count)
      }
      Iterator(firstPassStatsModel)
    }.reduce { (a, b) => //Part D
      a += (b)
      a
    }

    firstPassStats
  }
  
}
