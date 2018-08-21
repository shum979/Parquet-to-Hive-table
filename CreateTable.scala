

def getDdl(dataPath : String,tableName : String, numOfPatrs :String ) = {
val numOfPartitions = numOfPatrs.toInt
val d1 = spark.read.parquet(dataPath)
d1.createOrReplaceTempView(tableName)
spark.sql(s"create table temp_table as select * from $tableName limit 1")
val d2 = spark.sql("show create table temp_table").collect()
val tableDef = d2(0).getString(0)
spark.sql(s"drop table temp_table")
val l1 = tableDef.indexOf("ROW FORMAT")
val tbl = tableDef.substring(0,l1)
val arr = tbl.split(",")
val length = tbl.split(",").size
val remainder = for (i <- 0 until length - numOfPartitions ) yield arr(i)
val tblP1 = remainder.mkString(",") 
val parts = if (numOfPartitions > 0) " ) " + "PARTITIONED BY" +" ( " + arr.slice(length - numOfPartitions, length).mkString(",") else ""
val fullTableDef = s"$tblP1 $parts STORED AS PARQUET LOCATION '$dataPath'"
val finalTableDef = fullTableDef.replace("temp_table",tableName+" ").replace("CREATE TABLE","CREATE EXTERNAL TABLE IF NOT EXISTS")
finalTableDef.replace("`","")
}

//getting arguments from shell script
val args = sc.getConf.get("spark.driver.args").split("\\s+")
println("Arguments Passed :")
println(s"Table Name : $args(0)")
println(s"File Path : $args(1)")
println(s"Number of Partitions : $args(2)")
val tableName = args(0)
val filePath = args(1)
val numOfPatrs = args(2)

val ddl = getDdl(filePath,tableName,numOfPatrs)
//println(ddl)
spark.sql(ddl)
spark.sql(s"MSCK REPAIR TABLE $tableName")

println(s"---------------------------> Table $tableName created <------------------------------------")
System.exit(0)
