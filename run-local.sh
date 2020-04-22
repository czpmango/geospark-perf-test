#!/bin/bash
spark-submit --class GeoSparkTest file:///home/zilliz/czp/jar/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv -o hdfs://geospark-master:9000/log -f all -t 6
