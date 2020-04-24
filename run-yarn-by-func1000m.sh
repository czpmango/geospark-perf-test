#!/bin/bash
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Point -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Intersection -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_IsValid -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Equals -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Touches -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Overlaps -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Crosses -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_IsSimple -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_GeometryType -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_SimplifyPreserveTopology -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_PolygonFromEnvelope -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Contains -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Intersects -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Within -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Distance -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Area -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Centroid -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Length -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_ConvexHull -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_NPoints -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Envelope -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_Transform -t 6
spark-submit  --master yarn --deploy-mode cluster --class GeoSparkTest file:///home/zilliz/GeoSparkTest-assembly-0.1.jar   -p hdfs://geospark-master:9000/csv-geospark/csv1000m -o hdfs://geospark-master:9000/log1000m -f ST_AsText -t 6
