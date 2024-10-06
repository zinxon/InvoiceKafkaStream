%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices

kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic invoices
docker exec -it kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic invoices
