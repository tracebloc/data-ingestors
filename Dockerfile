# Use Python 3.11 as base image
FROM --platform=linux/amd64 python:3.11


# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y netcat-traditional

# Install data ingestor package
RUN pip install git+https://<user-token>:x-oauth-basic@github.com/tracebloc/data-ingestors.git@develop#egg=tracebloc_ingestor> # for dev
# <for prod: RUN pip install tracebloc_ingestor>

# Create necessary directories
RUN mkdir -p /app/data # Creates a data storage folder within the cluster where all the ingested data will be stored

# Copy the source code and requirements
COPY /data/X_train.csv /app/data/X_train.csv # Copying the data from local to the cluster
COPY /examples/csv_ingestor.py /app/csv_ingestor.py # Copying the ingestor script from local to the cluster

# Set environment variables
ENV PYTHONPATH=/app

# Create an entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"] 
