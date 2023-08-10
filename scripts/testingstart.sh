echo "Tiger-ATF Execution Started"
#/opt/spark/bin/spark-submit --driver-class-path /app/test/jdbcdriver/mysql-connector-java-5.1.46.jar --jars /app/test/jdbcdriver/mysql-connector-java-5.1.46.jar  /app/src/s2ttester.py $1 $2 $3 > /app/test/log/executionlog.txt
/opt/spark/bin/spark-submit --driver-memory 6g --driver-class-path $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') --jars $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') /app/src/s2ttester.py $1 $2 $3
#spark-submit --driver-memory 6g --driver-class-path $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') --jars $(echo /app/test/jdbcdriver/*.jar | tr ' ' ',') /app/src/s2ttester.py $1 $2 $3
echo "Tiger-ATF Execution Completed"