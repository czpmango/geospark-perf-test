#!/bin/bash
export TERM=xterm-color
# spark_home=/home/czp/workspace/spark-2.4.4-bin-hadoop2.7/
rm ./target/scala-2.11/GeoSparkTest-assembly-0.1.jar
sbt assembly
#${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkTest-assembly-0.1.jar -h -p ./csv -o ./log -f all -t 1 -s 
spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkTest-assembly-0.1.jar -h -p ./csv -o ./log -f all -t 1 -s 
