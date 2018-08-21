#!/bin/bash
FilePath=./table_properties.conf

#--Define functions
find_depth_local(){
  baseLength=`echo "$1" | awk -F"/" '{print NF-1}'`
  fullLenght=`find $1 -type d | awk -F"/" 'NF > max {max = NF} END {print max}'`
  export depth=$(( $fullLenght - $baseLength -1 ))
}

find_depth_hdfs(){
  baseLength=`hadoop fs -ls $1/.. | grep -i '^d' | awk -F"/" 'NF > max {max = NF} END {print max-1}'`
  fullLenght=`hadoop fs -ls -R $1 | grep -i '^d' | awk -F"/" 'NF > max {max = NF} END {print max-1}'`
  export depth=`expr $fullLenght - $baseLength`
}


#Check if file path exists
while read line; do
  case "$line" in \#*) continue ;; esac
  givenPath=`echo $line | cut -d',' -f2`
  tableName=`echo $line | cut -d',' -f1`
  if [ ! -d "$givenPath" ]; then
  hadoop fs -ls $givenPath >tm 2>&1
  if [ $? -ne 0 ];then
   echo "ERROR>>  '$givenPath' does not exist on Hdfs or Local"
   echo "Please correct givenPath and try again"
   exit 5
  else
  find_depth_hdfs $givenPath
  echo $tableName $givenPath $depth
  spark-shell -i CreateTable.scala --conf spark.driver.args="$tableName $givenPath $depth"
  fi
  else
    find_depth_local $givenPath
	echo $tableName $givenPath $depth
    spark-shell -i CreateTable.scala --conf spark.driver.args="$tableName $givenPath $depth" 	
  fi
done < $FilePath



