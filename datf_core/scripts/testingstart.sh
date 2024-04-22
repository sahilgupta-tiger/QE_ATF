echo "Tiger-DATF Execution Started"
spark-submit --driver-class-path $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') --jars $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') /app/src/s2ttester.py $1 $2
echo "Tiger-DATF Execution Completed"