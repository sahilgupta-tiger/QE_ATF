@echo OFF
set container=DATF
@echo "Start Docker Container"
docker start %container%
@echo "Execute within Docker Container"
docker exec -u root %container% bash -c "sh datf_core/scripts/testingstart.sh %*"
@echo "Stop Docker Container"
docker stop %container%