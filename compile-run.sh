#!/bin/bash
export TERM=xterm-color
# spark_home=/home/czp/workspace/spark-2.4.4-bin-hadoop2.7/
rm ./target/scala-2.11/GeoSparkTest-assembly-0.1.jar
sbt assembly
spark-submit --class GeoSparkTest file:///home/zilliz/czp/geospark-perf-test/target/scala-2.11/GeoSparkTest-assembly-0.1.jar -h -p file:///home/zilliz/czp/geospark-perf-test/csv/ -o log/ -f all -t 1 -s
