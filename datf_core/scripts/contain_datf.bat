set container=DATF
@echo "Start Docker Container"
docker start %container%
@echo "Execute within Docker Container"
docker exec -u root %container% bash -c "cd / && cd app/scripts && sh testingstart.sh %*"
@echo "Stop Docker Container"
docker stop %container%