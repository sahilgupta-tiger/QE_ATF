echo "Tiger-DATF Execution Started"
#/opt/spark/bin/spark-submit --driver-memory 6g --driver-class-path $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') --jars $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') /app/src/s2ttester.py $1 $2
/opt/spark/bin/spark-submit --driver-memory 6g --driver-class-path $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') /app/src/s2ttester.py $1 $2
echo "Tiger-DATF Execution Completed"