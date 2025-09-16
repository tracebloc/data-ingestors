#!/bin/bash

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
while ! nc -z $MYSQL_HOST 3306; do
  sleep 1
done
echo "MySQL is ready!"

# Run the CSV ingestor
python ingestor.py "$@" # train/test switch
