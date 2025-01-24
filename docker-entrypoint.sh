#!/bin/bash

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
while ! nc -z $MYSQL_HOST $MYSQL_PORT; do
  sleep 1
done
echo "MySQL is ready!"

# Run the CSV ingestor
python src/examples/csv_ingestor.py "$@" 