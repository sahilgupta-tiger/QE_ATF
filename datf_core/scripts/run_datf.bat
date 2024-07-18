set datfpath = D:/My_Workspaces/GitHub/DATF_Other/Pyspark/QE_ATF/datf_core
@echo "Build Docker Container"
docker run -dt -v %datfpath%:/app --name datf_pyspark apache/spark-py bash
@echo "Install Python Plugins"
docker exec -u root datf_pyspark bash -c "python3 -m pip install --upgrade pip"
docker exec -u root datf_pyspark bash -c "cd / && sh app/scripts/install.sh"
@echo "Execute within Docker Container"
docker exec -u root datf_pyspark bash -c "cd / && cd app/scripts && sh testingstart.sh %1 %2"
@echo "Stop and Remove Docker Container"
docker stop datf_pyspark
docker rm datf_pyspark