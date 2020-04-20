#!/bin/bash
spark_home=/home/czp/workspace/spark-2.4.4-bin-hadoop2.7/
rm ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar
sbt assembly
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar -h -p ./csv -o ./log -f ST_GeomFromGeojson -t 5 -s 

