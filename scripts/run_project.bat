@echo off

echo ====================================
echo Starting infrastructure services...
echo ====================================

docker compose up -d ^
zookeeper ^
kafka1 ^
kafka2 ^
kafka3 ^
elasticsearch ^
postgres ^
minio ^
kibana

echo.
echo ====================================
echo Running initialization jobs...
echo ====================================

docker compose run --rm kafka-init
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

docker compose run --rm elasticsearch-init
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

docker compose run --rm kibana-init
IF %ERRORLEVEL% NEQ 0 EXIT /B 1

docker compose run --rm minio-init
IF %ERRORLEVEL% NEQ 0 EXIT /B 1


echo.
echo ====================================
Starting Kafka UI...
echo ====================================

docker compose up -d kafka-ui

echo.
echo ====================================
echo Starting Spark and Jupyter...
echo ====================================

docker compose up -d ^
spark-master ^
spark-worker ^
jupyter

echo.
echo Waiting for Jupyter...
timeout /t 15

echo.
echo ====================================
echo Starting Spark Streaming Job...
echo ====================================

start cmd /k "docker exec -it jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0,org.apache.hadoop:hadoop-aws:3.3.4 --jars /home/jovyan/jars/postgresql-42.6.2.jar /home/jovyan/work/streaming_job.py"

echo.
echo ====================================
echo Starting IoT Producer...
echo ====================================

start cmd /k "python producer/ahmed_producer.py"