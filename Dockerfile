# Use Python 3.11 as base image
FROM --platform=linux/amd64 python:3.11

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y netcat-traditional

# Install data ingestor package
RUN pip install git+https://<secret>:x-oauth-basic@github.com/tracebloc/data-ingestors.git@develop#egg=tracebloc_ingestor
# <for prod: RUN pip install tracebloc_ingestor>

# Create necessary directories
RUN mkdir -p /data/shared

# Copy the source code and requirements # train/test switch
COPY templates/data/tabular_classification_sample_in_csv_format.csv /app/data/shared/tabular_classification_sample_in_csv_format.csv
COPY templates/csv_ingestor.py /app/ingestor.py


# Set environment variables
ENV PYTHONPATH=/app
# Disable GPU/CUDA usage
ENV CUDA_VISIBLE_DEVICES=-1
ENV TF_CPP_MIN_LOG_LEVEL=2

# Create an entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"] 
