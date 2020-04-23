#!/bin/bash -e
./run-yarn-by-func100k.sh
echo "100k test ok ......"
./run-yarn-by-func1m.sh
echo "1m test ok ......"
./run-yarn-by-func10m.sh
echo "10m test ok ......"
./run-yarn-by-func100m.sh
echo "100m test ok ......"
./spatial-func-test.sh
echo "spatial func test ok ......"
echo "All tests ok ......"
