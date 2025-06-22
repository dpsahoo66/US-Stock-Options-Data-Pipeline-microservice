#!/bin/bash
set -e
until nc -z kafka 9092; do
echo "Waiting for Kafka to be ready..."
sleep 2
done
echo "Kafka is up!"
exec "$@"