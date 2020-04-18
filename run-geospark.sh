#!/bin/bash
rm ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar
sbt assembly
#/home/czp/workspace/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --class GeoSparkDemo ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar 
/home/czp/workspace/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar -h -p 1 -o 2 -f ST_Point -t 5 

