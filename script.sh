#!/bin/bash

docker exec -it spark-master spark-submit \
--master spark://spark-master:7077 \
--deploy-mode client \
--jars /opt/spark/jars/postgresql-42.6.0.jar \
--driver-class-path /opt/spark/jars/postgresql-42.6.0.jar \
/opt/spark-apps/spark_postgres.py

docker exec -it spark-master spark-submit \
--master spark://spark-master:7077 --deploy-mode \
client --jars "/opt/spark/jars/postgresql-42.6.0.jar,\
/opt/spark/jars/clickhouse-jdbc-0.4.6.jar" \
--driver-class-path "/opt/spark/jars/postgresql-42.6.0.jar:/opt/spark/jars/clickhouse-jdbc-0.4.6.jar" \
/opt/spark-apps/spark_clickhouse.py
