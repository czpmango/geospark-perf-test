#!/bin/bash
spark_home=/home/czp/workspace/spark-2.4.4-bin-hadoop2.7/
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Point -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Intersection -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_IsValid -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Equals -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Touches -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Overlaps -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Crosses -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_IsSimple -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_GeometryType -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_SimplifyPreserveTopology -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_PolygonFromEnvelope -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Contains -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Intersects -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Within -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Distance -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Area -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Centroid -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Length -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_ConvexHull -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_NPoints -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Envelope -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Buffer -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Union_Aggr -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Envelope_Aggr -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_Transform -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_GeomFromWkt -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_GeomFromGeojson -t 1 -s 
${spark_home}/bin/spark-submit --class GeoSparkTest ./target/scala-2.11/GeoSparkSQLScalaTemplate-assembly-0.1.0.jar  -p ./csv -o ./log -f ST_AsText -t 1 -s 

