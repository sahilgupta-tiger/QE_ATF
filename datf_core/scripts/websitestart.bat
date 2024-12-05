@echo off
set datfpath=D:/My_Workspaces/GitHub/DATF_Other/Pyspark/QE_ATF
set container=datf_with_app

docker ps -a --filter "name=%container%" --format "{{.Names}}" > temp_exists.txt
set /p CONTAINER_EXISTS=<temp_exists.txt
del temp_exists.txt

if "%CONTAINER_EXISTS%"=="" (
    echo Container does not exist. Creating a new container...
    docker run -dt -p 8501-8510:8501-8510 -v %datfpath%:/app --name %container% apache/spark-py bash
) else (
    echo Container exists. Checking if it is running...

    docker ps --filter "name=%container%" --filter "status=running" --format "{{.Names}}" > temp.txt
    set /p CONTAINER_CHECK=<temp.txt
    del temp.txt

    if "%CONTAINER_CHECK%"=="" (
        echo Container is not running. Starting the container...
        docker start %container%
    ) else (
        echo Container is already running...
    )
)
echo Installing Plugins
docker exec -u root %container% bash -c "pip config set global.trusted-host 'pypi.org files.pythonhosted.org pypi.python.org' --trusted-host=pypi.python.org --trusted-host=pypi.org --trusted-host=files.pythonhosted.org"
docker exec -u root %container% bash -c "python3 -m pip install --upgrade pip"
docker exec -u root %container% bash -c "cd / && sh app/datf_core/scripts/install.sh"
echo Starting Website
docker exec -u root %container% bash -c "cd / && cd app/datf_core/scripts && sh websitestart.sh"
