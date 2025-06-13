#!/bin/bash 
set -x 
# Enable debug output 
# Migrate Airflow database 
airflow db migrate || { echo "Airflow database migration failed"; exit 1; } 
# Create default connections 
echo "Running airflow connections create-default-connections" 
airflow connections create-default-connections || { echo "Failed to create default connections"; exit 1; } 
# Start Airflow scheduler 
exec airflow scheduler