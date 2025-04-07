#!/bin/bash

# Create the Kafka topic if it doesn't exist
echo "Creating Kafka topic 'users_created' if it doesn't exist..."
docker exec broker kafka-topics --create --topic users_created --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1 --if-not-exists

echo "Kafka topic creation completed." 