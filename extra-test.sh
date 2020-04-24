spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv100k -o hdfs://geospark-master:9000/log100k -f ST_SimplifyPreserveTopology -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1m -o hdfs://geospark-master:9000/log1m -f ST_SimplifyPreserveTopology -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv10m -o hdfs://geospark-master:9000/log10m -f ST_SimplifyPreserveTopology -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv100m -o hdfs://geospark-master:9000/log100m -f ST_SimplifyPreserveTopology -t 6