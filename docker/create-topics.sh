#!/bin/bash

echo "Esperando a que Kafka arranque..."
for i in $(seq 1 30); do
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >/dev/null 2>&1 && break
  sleep 2
done

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic envios.estados
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --config cleanup.policy=compact --topic envios.promesas
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic envios.reprogramaciones
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic alertas.sla
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic notificaciones.enviadas
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --topic envios.estados.dlq

echo "Tópicos creados exitosamente."