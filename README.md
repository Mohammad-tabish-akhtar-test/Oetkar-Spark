# Oetkar-Spark

Kafka version: 2.3.1
Spark Structured Streaming Version: pyspark - 2.4.4

Commands:
Start Zookeeper: bin/zookeeper-server-start.sh config/zoeeper.properties
Start Kafka: bin/kafka-server-start.sh config/server.properties
Send message from Producer: python sync_P.py
Consume message from Spark Structured Streaming Consumer: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark_C.py
