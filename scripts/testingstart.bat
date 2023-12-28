echo "Tiger-DATF Execution Started"
%SPARK_HOME%\bin\spark-submit --driver-memory 6g src\s2ttester.py %1 %2
echo "Tiger-DATF Execution Completed"