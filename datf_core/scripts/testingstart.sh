echo "Tiger-DATF Execution Started"
/opt/spark/bin/spark-submit --driver-memory 6g --driver-class-path $(echo datf_core/test/jdbcdriver/*.jar | tr ' ' ',') --jars $(echo datf_core/test/jdbcdriver/*.jar | tr ' ' ',') datf_core/src/s2ttester.py $1 $2
echo "Tiger-DATF Execution Completed"