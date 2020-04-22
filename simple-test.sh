#!/bin/bash -e
spark-submit  --master yarn --deploy-mode client --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv -o hdfs://geospark-master:9000/log -f all -t 1 -s
