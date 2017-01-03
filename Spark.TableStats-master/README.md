# Spark.TableStatsExample
Simple Spark example of generating table stats for use of data quality checks

##SimpleDataGeneratorMain
This will generate small test data set

SimpleDataGeneratorMain {outputPath}

##ConfigurableDataGeneratorMain
This will generate large test data set

ConfigurableDataGeneratorMain {outputPath} {numberOfColumns} {numberOfRecords} {numberOfPartitions}

##TableStatsSinglePathMain
This will output the following information on a given column in the table

* null count
* empty count
* total count
* unique value count
* max 
* min
* sum
* top N values with there cardinality

TableStatsSinglePathMain {inputPath}

##Examples of Execution
###Small data set
spark-submit --class com.cloudera.sa.examples.tablestats.ConfigurableDataGeneratorMain --master yarn --deploy-mode client --executor-memory 512M --num-executors 4 file-stats_2.10-1.0.jar ./gen/output 10 10000 4

spark-submit --class com.cloudera.sa.examples.tablestats.TableStatsSinglePathMain --master yarn --deploy-mode client --executor-memory 512M --num-executors 4 file-stats_2.10-1.0.jar ./gen/output

###Larger data set
spark-submit --class com.cloudera.sa.examples.tablestats.ConfigurableDataGeneratorMain --master yarn --deploy-mode client --executor-memory 512M --num-executors 8 file-stats_2.10-1.0.jar /user/appuser/work/spark/dataset2 10 10000000 8

spark-submit --class com.cloudera.sa.examples.tablestats.TableStatsSinglePathMain --master yarn --deploy-mode client --executor-memory 1024M --num-executors 8 file-stats_2.10-1.0.jar /user/appuser/work/spark/dataset2
spark-submit --class com.cloudera.sa.examples.tablestats.TableStatsSinglePathMain --master yarn --deploy-mode client --executor-memory 6G --num-executors 1000 file-stats_2.10-1.0.jar /refined/analytics/Amerigroup/Denorms/Claims_Denorm1_Par/OP