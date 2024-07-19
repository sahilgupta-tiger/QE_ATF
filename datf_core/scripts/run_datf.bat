set datfpath=D:/My_Workspaces/GitHub/DATF_Other/Pyspark/QE_ATF/datf_core
set container=datf_pyspark
@echo "Build Docker Container"
docker run -dt -v %datfpath%:/app --name %container% apache/spark-py bash
@echo "Install Python Plugins"
docker exec -u root %container% bash -c "python3 -m pip install --upgrade pip"
docker exec -u root %container% bash -c "cd / && sh app/scripts/install.sh"
@echo "Execute within Docker Container"
docker exec -u root %container% bash -c "cd / && cd app/scripts && sh testingstart.sh %*"
@echo "Stop and Remove Docker Container"
docker stop %container%
docker rm %container%