%KAFKA_HOME%\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic invoices

kafka-console-producer --bootstrap-server kafka:9092 --topic invoices
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic invoices